# Spark java UDF test

import os
import sys
import six

from pyspark.sql import SparkSession, functions as sqlfn
from pyspark.sql.types import StructType, StructField, StringType, MapType, FloatType, ArrayType, DoubleType


def show(df):
    df.show(33, truncate=False)
    df.printSchema()
    df.explain()
    return df


conf = {
    "spark.hadoop.hive.exec.dynamic.partition": "true",
    "spark.hadoop.hive.exec.dynamic.partition.mode": "nonstrict",
    "spark.ui.enabled": "false",
    "spark.driver.host": "localhost",
    "spark.default.parallelism": 3,
    "spark.sql.shuffle.partitions": 3,
}

spark = SparkSession.builder.master("local[3]")
spark = spark.enableHiveSupport()
for k, v in six.iteritems(conf):
    spark = spark.config(k, v)
spark_session = spark.getOrCreate()
spark = spark_session

# @formatter:off

df = spark.createDataFrame(
    data=[(
        str(i),
        "an{}".format(i),
        "positive",
        "ut{}".format(i % 3),
        i,
        None if i % 7 == 0 else (
            float("inf") if i % 11 == 0 else (
                float("-inf") if i % 13 == 0 else (float("nan") if i % 5 == 0 else float(i))
            )
        ),
        [float(i) / float(x+1) for x in range(7)],
        {str(k): float(i) / float(k+1) for k in range(7)},
    ) for i in range(1, 123001)],
    schema=(
        "uid:string,audience_name:string,category:string,uid_type:string,"
        "action_dt:long,score:float,score_list:array<float>,score_map:map<string,float>"
    ),
)
# df.printSchema()
# df.show(200, truncate=False)

hdfs_home_dir = "file:/tmp/local_dev/spark_udf-test/"
hdfs_dir = os.path.join(hdfs_home_dir, "test_dataset")
df.repartition(3, "uid").write.option("mapreduce.fileoutputcommitter.algorithm.version", "2").parquet(
    hdfs_dir, mode="overwrite"
)
df = show(spark.read.parquet(hdfs_dir))
# |-- uid: string (nullable = true)
# |-- audience_name: string (nullable = true)
# |-- category: string (nullable = true)
# |-- uid_type: string (nullable = true)
# |-- action_dt: long (nullable = true)
# |-- score: float (nullable = true)
# |-- score_list: array (nullable = true)
# |    |-- element: float (containsNull = true)
# |-- score_map: map (nullable = true)
# |    |-- key: string
# |    |-- value: float (valueContainsNull = true)

df.createOrReplaceTempView("features")


class UDFLibrary(object):

    def warn(self, msg):
        print("WARN: " + msg)

    def info(self, msg):
        print("INFO: " + msg)

    def __init__(self, spark, jar="hdfs:/lib/transformers-SNAPSHOT.jar"):
        self.spark = spark

        if jar is None:
            self.warn("Jar is not given here, so make sure that it is registered upstream")
        elif not isinstance(jar, six.string_types) or not jar:
            raise ValueError("Jar must be a non-empty string, got `{}`".format(jar))
        else:
            spark.sql("ADD JAR {}".format(jar))

    def register_all(self):
        self.spark._jvm.vasnake.udf.catalog.registerAll(
            self.spark._jsparkSession,
            True  # rewrite
        )
        return self


UDFLibrary(spark, jar=os.environ["SPARK_JARS"]).register_all()

_df = (
    df
    .where("uid_type in ('ut2', 'ut1')")
    .selectExpr(
        "map_values_ordered(score_map, array('1', '3'))",
        "hash_to_uint32(audience_name)",
        "murmurhash3_32_positive(audience_name)",
        "is_uint32(uid)",
        "map_join(score_map, ';', ',')",
        "uid2user(uid, 'VID')",
        "uid64(md5(audience_name))",
        "html_unescape(audience_name)",
        "stat_map_to_array(score_map, double(9.9), '2023-05-30', 2)",
        "activity_types(cast(score_list as array<double>), array(1,2,3))",
    )
)
show(_df)

spark.stop()
