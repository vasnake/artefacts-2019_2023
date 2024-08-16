import os
import sys
import six
import glob
import pyspark

from pprint import pformat

from pyspark.ml.wrapper import JavaWrapper
from pyspark.sql import SparkSession, functions as sqlfn
from pyspark.sql.types import StructType, StructField, StringType, MapType, FloatType, ArrayType, DoubleType
from pyspark.sql.utils import AnalysisException
from pyspark.storagelevel import StorageLevel

conf = {
    "spark.default.parallelism": 3,
    "spark.sql.shuffle.partitions": 3,
    "spark.hadoop.hive.exec.dynamic.partition": "true",
    "spark.hadoop.hive.exec.dynamic.partition.mode": "nonstrict",
    "spark.ui.enabled": "false",
    "spark.driver.host": "localhost",
}
spark = SparkSession.builder.master("local[3]")
spark = spark.enableHiveSupport()
for k, v in six.iteritems(conf):
    spark = spark.config(k, v)
spark_session = spark.getOrCreate()
spark = spark_session


def listdir(path, only_path=False):
    def with_attribs(path):
        isDir = os.path.isdir(path)
        size = 0
        if not isDir:
            size = os.path.getsize(path)  # bytes
        return "{}\t{:.3f}KB\t`{}`".format("D" if isDir else "F", size / 1000.0, path)

    if os.path.exists(path):
        yield path if only_path else with_attribs(path)
        if os.path.isdir(path):
            for p in os.listdir(path):
                for x in listdir(os.path.join(path, p), only_path=True):
                    yield x if only_path else with_attribs(x)


def sorted_listdir(path):
    def sortkey(line):
        a, _, c = line.split("\t")
        return a, c
    return sorted(listdir(path), key=lambda x: sortkey(x))


def show(df, msg="DF", lines=200, truncate=False):
    df.persist(StorageLevel.MEMORY_ONLY)

    count = df.count()
    num_parts = df.rdd.getNumPartitions()

    print("\n# {}\nrows: {}, partitions: {}".format(msg, count, num_parts))
    df.printSchema()
    try:
        df.orderBy("dt", "uid_type", "feature", "uid").show(n=lines, truncate=truncate)
    except AnalysisException as e:
        df.show(n=lines, truncate=truncate)

    df.unpersist()
    return df


# uid, feature, uid_type, dt, category
# source_df = local_spark.createDataFrame(
#             [["foo", [float(x) for x in range(20)], {"0": 3.14, "2": 0.9, "9": 99.9}, "2020-10-15", "VID"]],
#             schema="uid:string,fg_0:array<double>,fg_1:map<string,double>,dt:string,uid_type:string",
#         )
df = show(
    spark.createDataFrame(
        [
            ["a", 1.1, "HID", "2022-01-19"],
            ["b", 2.2, "HID", "2022-01-19"],
            ["c", 3.3, "HID", "2022-01-19"],
            ["aa", 4.4, "HID", "2022-01-19"],
            ["bb", 5.5, "HID", "2022-01-19"],
            ["cc", 6.6, "HID", "2022-01-19"],
            ["a", 1.1, "FOO", "2021-11-09"],
            ["b", 2.2, "FOO", "2021-11-09"],
            ["c", 3.3, "FOO", "2021-11-09"],
            ["aa", 4.4, "FOO", "2021-11-09"],
            ["bb", 5.5, "FOO", "2021-11-09"],
            ["cc", 6.6, "FOO", "2021-11-09"],
            # A
            ["a", 1.1, "HID", "2021-11-09"],
            ["b", 2.2, "HID", "2021-11-09"],
            ["c", 3.3, "HID", "2021-11-09"],
            ["aa", 4.4, "HID", "2021-11-09"],
            ["bb", 5.5, "HID", "2021-11-09"],
            ["cc", 6.6, "HID", "2021-11-09"],
            # B
            ["aaa", 7.7, "GAID", "2021-11-09"],
            ["bbb", 8.8, "GAID", "2021-11-09"],
            ["ccc", 9.9, "GAID", "2021-11-09"],
            ["aaaa", 10.10, "GAID", "2021-11-09"],
            ["bbbb", 11.11, "GAID", "2021-11-09"],
            ["cccc", 12.12, "GAID", "2021-11-09"],
            # C
            ["aaaa", 13.13, "IDFA", "2021-11-09"],
            ["bbbb", 14.14, "IDFA", "2021-11-09"],
            ["cccc", 15.15, "IDFA", "2021-11-09"],
            ["aaaaa", 16.16, "IDFA", "2021-11-09"],
            ["bbbbb", 17.17, "IDFA", "2021-11-09"],
            ["ccccc", 18.18, "IDFA", "2021-11-09"],
        ],
        schema="uid:string,feature:float,uid_type:string,dt:string"
    ).withColumn("category", sqlfn.lit("positive")).repartition(12, "uid").persist(StorageLevel.MEMORY_ONLY),
    "Source DF"
)
df.createGlobalTempView("test_features")
assert df.count() == 30

spark.sql("CREATE DATABASE IF NOT EXISTS dev_source")
spark.sql("DROP TABLE IF EXISTS dev_source.trg_73352_test")

# there are two ways of creating a table: the right one and convenient one

# the convenient one
# N.B. extra file created: `/tmp/warehouse/dev_source.db/trg_73352_test/_SUCCESS`:
# df.select("uid", "feature", "dt", "uid_type").where("1 = 0").write.format("orc").mode(
#     "overwrite"
# ).partitionBy("dt", "uid_type").saveAsTable("dev_source.trg_73352_test")

# the right one
partitioned_ddl = """
CREATE TABLE IF NOT EXISTS dev_source.trg_73352_test (
  uid           string             comment 'user id',
  feature       float              comment 'feature value'
) comment 'user feature F'
PARTITIONED BY (
  category      string             comment 'segment category',
  dt            string             comment 'date as string',
  uid_type      string             comment 'uid type'
)
STORED AS orc
"""

x_partition_ddl = """
CREATE TABLE IF NOT EXISTS dev_source.trg_73352_test (
  uid           string             comment 'user id',
  feature       float              comment 'feature value'
) comment 'user feature F'
PARTITIONED BY (
  category      string             comment 'segment category',
  uid_type      string             comment 'uid type'  
)
STORED AS orc
"""

flat_ddl = """
CREATE TABLE IF NOT EXISTS dev_source.trg_73352_test (
  uid           string             comment 'user id',
  feature       float              comment 'feature value',
  category      string             comment 'segment category',
  dt            string             comment 'date as string',
  uid_type      string             comment 'uid type'
) comment 'user feature F'
STORED AS orc
"""

spark.sql(partitioned_ddl.strip())
# spark.sql(x_partition_ddl.strip())
# spark.sql(flat_ddl.strip())


def print_db_files(msg):
    print("\n{}".format(msg))
    for p in sorted_listdir("/tmp/warehouse/dev_source.db"):
        print(p)
    print("")


def insert_twelve_partitions(df):
    _insert_n_partitions(df, 12)


def insert_four_partitions(df):
    _insert_n_partitions(df, 4)


def _insert_n_partitions(df, n):
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    columns = spark.catalog.listColumns(dbName="dev_source", tableName="trg_73352_test")
    _df = show(df.repartition(n, "dt", "uid_type", "category").select(*[c.name for c in columns]), "writing {} partitions DF ...".format(n))
    _df.write.option(
        "mapreduce.fileoutputcommitter.algorithm.version", "2"  # no observable effects
    ).insertInto("dev_source.trg_73352_test", overwrite=True)


def insert_using_jvm_lib(df, max_rows_per_bucket=1):
    # from pyspark.ml.wrapper import JavaWrapper

    # Custom class loader fails on executors if `spark.sql(ADD JAR $JAR)` was used.
    # java.lang.ClassCastException: com...DataFrameHivePartitionsInfo cannot be cast to com...DataFrameHivePartitionsInfo
    # Works fine with `spark-submit ... --jars file:${SPARK_JARS}`.

    def _insert_into_hive(
            df,
            database,
            table,
            max_rows_per_bucket,
            overwrite=True,
            raise_on_missing_columns=True,
            check_parameter=None,
            jar="hdfs:/lib/transformers-assembly-SNAPSHOT.jar",
    ):
        df.sql_ctx.sql("ADD JAR {}".format(jar))  # wrong choice, use `spark-submit ... --jars file:${SPARK_JARS}`
        writer = JavaWrapper._create_from_java_class("hive.Writer")  # SQL partitioner by default
        # writer = JavaWrapper._create_from_java_class("hive.Writer", "RDD")  # RDD partitioner

        writer._java_obj.insertIntoHive(
            df._jdf,
            database,
            table,
            max_rows_per_bucket,
            overwrite,
            raise_on_missing_columns,
            check_parameter,
        )

    # check on/off, recover on/off:
    # flat table, empty
    # flat table, empty, no columns
    # flat table, no columns
    # flat table OK
    # null in part cols
    # repeat for part.table
    _insert_into_hive(
        df,
        database="dev_source",
        table="trg_73352_test",
        # max_rows_per_bucket=3 for 12 rows total, and 4 hive partitions: 1 file (aka bucket) for each hive partition
        max_rows_per_bucket=max_rows_per_bucket,
        overwrite=True,
        raise_on_missing_columns=False,
        check_parameter="marker.partition.written.successfully",
        jar=os.environ["TEST_JARS"]  # "file:/tmp/transformers-assembly-SNAPSHOT.jar"
    )
    # N.B. metastore logic: rewriting partition doesn't clear the flag, it should be cleared explicitly.
    # Inserting empty dataframe does nothing.

    df.unpersist()


def smoke_test(df):

    df = (
        df
        .selectExpr(
            "uid_type",
            "dt",
            "category",
            # "cast(null as string) as category",
            "cast(feature as string) as feature",
            # "'foo' as bar",
        ).withColumn(
            # 6*10 rows/hive.part, 3 hive.part
            "uid", sqlfn.explode(sqlfn.expr("array({})".format(",".join([str(x) for x in range(1, 11)]))))
        )
    ).persist(StorageLevel.MEMORY_ONLY)

    insert_twelve_partitions(df)
    # show(spark.sql("select * from dev_source.trg_73352_test"), "DF saved to Hive table, 12 partitions")
    # print_db_files("DB `dev_source` files after 12 partitions written:")

    # insert_four_partitions(df)
    # show(spark.sql("select * from dev_source.trg_73352_test"), "DF saved to Hive table, 4 partitions")
    # print_db_files("DB `dev_source` files after 4 partitions written:")
    print_db_files("DB `dev_source` files before JVM writer:")

    # stable and correct results starts from 5 rows/file
    insert_using_jvm_lib(df.where("dt = '2021-11-09' and uid_type != 'FOO'"), max_rows_per_bucket=6)
    # 6 rows/file, 3 hive.part, 180 rows => 10 file/hive.part
    show(spark.sql("select * from dev_source.trg_73352_test"), "Hive table after JVM writer")
    print_db_files("DB `dev_source` files after JVM writer:")


smoke_test(df)

spark.stop()
