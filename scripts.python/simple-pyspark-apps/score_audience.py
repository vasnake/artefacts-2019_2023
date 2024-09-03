# -*- coding: utf-8 -*-

"""
spark-submit experimental (test) job, part of migration to scala-apply from python-apply (ML models).

Original code: scoring/score_auditory/hive_udf_predict.py

curl -T /tmp/p_hmc_660.npz http://hadoop.mail.ru/uu/vlk/
hdfs dfs -mv ./upload/p_hmc_660.npz ./dm-6154-tests/

export extraJavaOptions="-XX:+UseCompressedOops -XX:hashCode=0" && \
spark-submit --master yarn --deploy-mode cluster --queue dev.priority \
    --name DM-6154-test \
    --num-executors 26 --executor-cores 4 --executor-memory 4G --driver-memory 2G \
    --conf spark.yarn.executor.memoryOverhead=2G \
    --conf spark.sql.shuffle.partitions=104 \
    --conf spark.hadoop.dfs.replication=2 \
    --conf spark.speculation=false \
    --conf spark.dynamicAllocation.enabled=false \
    --conf spark.dynamicAllocation.maxExecutors=24 \
    --conf spark.dynamicAllocation.minExecutors=12 \
    --conf spark.sql.parquet.compression.codec=gzip \
    --conf spark.hadoop.zlib.compress.level=BEST_COMPRESSION \
    --conf spark.eventLog.enabled=true \
    --conf spark.driver.maxResultSize=2g \
    --conf spark.broadcast.blockSize=32m \
    --conf "spark.executor.extraJavaOptions=${extraJavaOptions}" \
    --conf spark.driver.extraJavaOptions=-XX:hashCode=0 \
    --conf spark.checkpoint.dir=hdfs:///tmp \
    --conf spark.sql.sources.partitionColumnTypeInference.enabled=false \
    --conf spark.yarn.maxAppAttempts=1 \
    --files dm-6154-tests/p_hmc_660.npz \
    --jars /etc/hive/conf/hive-site.xml \
    score_audience.py \
        table=scoring.topics_m \
        dt=2019-07-09 \
        uid_type=OKID

"""

from __future__ import print_function

from random import random
from functools import partial
import numpy as np

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.sql.types import Row
from pyspark.files import SparkFiles

FS_ROOT = "/user/vlk"
DEFAULT_SCORE_TYPE = 'SIGMOID'
DEFAULT_TOPIC_THRESHOLD = 0.1


def load_predictor(fname):
    """
    Load fname.npz file from driver work dir.
    It should work if you pass a parameter like '--files dm-6154/p_hmc_660.npz' to spark-submit command.

    :param fname: file name as SparkFiles.get argument
    :return: tuple (W, b, d_idf, t_idf, g_idf)
    """
    path = SparkFiles.get(fname)
    fi = np.load(path)
    t_idf = fi['t_idf'] if (('t_idf' in fi.keys()) and (fi['t_idf'].ndim != 0)) else None
    g_idf = fi['g_idf'] if (('g_idf' in fi.keys()) and (fi['g_idf'].ndim != 0)) else None
    # (W, b, d_idf, t_idf, g_idf)
    return fi['W'], fi['b'].ravel()[0], fi['d_idf'], t_idf, g_idf


def get_score(predictor, features, score_type=DEFAULT_SCORE_TYPE):
    """
    Apply model.
    Code copied from scoring/score_auditory/hive_udf_predict.py

    :param predictor: tuple (W, b, d_idf, t_idf, g_idf)
    :param features: tuple ((age, sex, joined, joined), [ (idx_x, topic_x), ..., (idx_y, topic_y)], None, None)
    :param score_type: 'SIGMOID'
    :return float: score value
    """
    (W, b, d_idf, t_idf, g_idf), fv = predictor, list(features[0])
    len_f, fi = len(fv), range(len(fv))
    dc = np.float64(sorted(features[1]))
    if len(dc):
        di, dv = dc[:, 0].astype(int), dc[:, 1]
        tf = (1 + np.log(dv)) * d_idf[di]
        tf /= np.sqrt(np.sum(tf**2))
        fi.extend((di + len_f).tolist())
        fv.extend(tf.tolist())

    # skip while t_idf, g_idf is None in our test case
    if (t_idf is not None) and (features[2] is not None):
        tc = np.float64(sorted(features[2]))
        if len(tc):
            ti, tv = tc[:, 0].astype(int), tc[:, 1]
            tf = (1 + np.log(tv)) * t_idf[ti]
            tf /= np.sqrt(np.sum(tf**2))
            fi.extend((ti + len_f + d_idf.shape[0]).tolist())
            fv.extend(tf.tolist())
        if (g_idf is not None) and (features[3] is not None):
            gc = np.float64(sorted(features[3]))
            if len(gc):
                gi, gv = gc[:, 0].astype(int), gc[:, 1]
                tf = (1 + np.log(gv)) * g_idf[gi]
                tf /= np.sqrt(np.sum(tf**2))
                fi.extend((gi + len_f + d_idf.shape[0] + t_idf.shape[0]).tolist())
                fv.extend(tf.tolist())

    Z = np.dot(W[fi], np.float64(fv)) + b

    if score_type == "LINEAR":
        return Z
    elif score_type == "NEG_LINEAR":
        return -Z
    elif score_type == "SIGMOID":
        return 1./(1 + np.exp(Z))


def try_float(x, default='\\N'):
    """
    Try to convert x to float and return converted number.
    If can't convert, return given default.

    :param x: value from spark Row column
    :param default: value to return in case of invalid `x`
    :return: float or default value
    """
    try:
        return float(x)
    except:
        return default


def compute_score(row, predictor):
    """
    Refactored code from scoring/score_auditory/hive_udf_predict.py

    :param row: DataFrame row
    :param predictor: tuple (W, b, d_idf, t_idf, g_idf)
    :return: score value for given row
    """
    fields = row.asDict()

    f_age = try_float(fields['age'], default=0.5)
    f_sex = try_float(fields['sex'], default=0.5)
    f_profage = try_float(fields['joined'], default=0.5)

    # main features
    tup = (f_age, f_sex, f_profage, f_profage)

    # topic features
    topic_names = ['topic_{}'.format(x) for x in range(1, 201)]
    topics200 = [try_float(fields[n]) for n in topic_names]
    topics = [
        (it[0], float(it[1])) for it in enumerate(topics200)
        if (it[1] != '\\N' and float(it[1]) > DEFAULT_TOPIC_THRESHOLD)
    ]

    score = get_score(predictor, (tup, topics, None, None), DEFAULT_SCORE_TYPE)
    return score.item() # https://docs.scipy.org/doc/numpy/reference/generated/numpy.ndarray.item.html


def score_partition(rows, predictor):
    """
    Transform DataFrame partition to scored rows,
    compute score for each row

    :param rows: DataFrame rows iterator, row(uid, sex, age, joined, topic_1, ..., topic_200)
    :param predictor: tuple (W, b, d_idf, t_idf, g_idf)
    :return: DataFrame rows iterator, row(uid, score)
    """
    for row in rows:
        yield Row(**{'uid': row['uid'], 'score': compute_score(row, predictor)})


def main():
    spark = SparkSession.builder.appName("DM-6154-test").getOrCreate()

    print("try to add predictor file...")
    spark.sparkContext.addFile('hdfs://hacluster/user/vlk/dm-6154-test/p_hmc_660.npz')
    print("try to load predictor...")
    weights = load_predictor('p_hmc_660.npz')
    print("predictor loaded.")

    src_df = spark.sql("""
select * from scoring.topics_m_all_profiles
where dt = '2019-07-09' and uid_type = 'OKID'
""").cache()
    print("{} rows of input data loaded".format(src_df.count()))
    src_df.show(3, truncate=False)

    rdd = src_df.rdd.mapPartitions(
        partial(score_partition, predictor=weights),
        preservesPartitioning=True
    )

    schema = StructType([
        StructField('uid', StringType()),
        StructField('score', DoubleType())
    ])

    res_df = spark.createDataFrame(rdd, schema).repartition(4).cache()
    print("{} rows of input data transformed".format(res_df.count()))
    res_df.show(3, truncate=False)

    output_path = FS_ROOT + '/dm-6154/simple-apply-python.result'
    res_df.write.mode('overwrite').parquet(output_path)
    print("output written to {}".format(output_path))

    spark.stop()


if __name__ == "__main__":
    main()
