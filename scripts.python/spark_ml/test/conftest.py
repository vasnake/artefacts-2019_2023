import os

import pytest

from pyspark.sql import SparkSession
from pyspark.conf import SparkConf


@pytest.fixture(scope="session")
def local_spark(request):
    spark = SparkSession.builder.config(
        conf=SparkConf().setAll(
            [
                ("spark.master", "local[2]"),
                ("spark.driver.host", "localhost"),
                ("spark.ui.enabled", "false"),
                ("spark.sql.shuffle.partitions", 2),
                ("spark.jars", os.environ["SPARK_JARS"]),
            ]
        )
    ).getOrCreate()

    request.addfinalizer(lambda: spark.stop())
    return spark
