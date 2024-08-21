import os
import pytest

from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def local_spark(request):
    conf = {
        "spark.jars":                   os.environ["SPARK_JARS"],
        "spark.checkpoint.dir":         "/tmp/checkpoints",
        "spark.driver.host":            "localhost",
        "spark.ui.enabled":             "false",
        "spark.sql.shuffle.partitions": 1
    }
    spark = SparkSession.builder.master("local[1]")
    for k, v in conf.items():
        spark = spark.config(k, v)

    spark = spark.getOrCreate()
    request.addfinalizer(lambda: spark.stop())
    return spark
