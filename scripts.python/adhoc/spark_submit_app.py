"""
Example/helper for spark-submit app

https://github.com/apache/spark/blob/v2.4.8/examples/src/main/python/pi.py
https://spark.apache.org/docs/latest/submitting-applications.html
"""

from pyspark.sql import SparkSession
from pyspark.ml.wrapper import JavaWrapper


def insert_into_hive(
        df,
        database,
        table,
        max_rows_per_bucket,
        overwrite=True,
        raise_on_missing_columns=True,
        check_parameter=None,
        jar="hdfs:/lib/prj-transformers-assembly-1.5.3.jar",
):
    df.sql_ctx.sql("ADD JAR " + jar)
    writer = JavaWrapper._create_from_java_class("com.prj.hive.Writer")
    writer._java_obj.insertIntoHive(
        df._jdf,
        database,
        table,
        max_rows_per_bucket,
        overwrite,
        raise_on_missing_columns,
        check_parameter,
    )


def main(spark):
    insert_into_hive(
        df=spark.sql("SELECT * FROM snb_ds_segmentation.prj_mobile_dataset").cache(),
        database="snb_ds_segmentation",
        table="prj_mobile_dataset_new",
        max_rows_per_bucket=int(1e6),
        overwrite=True,
        raise_on_missing_columns=True,
        check_parameter="dmprj_success_flag",
    )


if __name__ == "__main__":
    spark = SparkSession.builder.appName("tables-migration").getOrCreate()
    main(spark)
    spark.stop()
