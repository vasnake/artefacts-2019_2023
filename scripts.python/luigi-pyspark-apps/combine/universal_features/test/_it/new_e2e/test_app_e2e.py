"""Original e2e test with HiveDataIOSpark, saved for later debugging (table creation problem)"""

import os

from contextlib import contextmanager

import pandas as pd

from lockfile import LockFile, LockError
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf

from prj.apps.utils.common.fs import HdfsClient
from prj.apps.utils.testing.tools import ControlAppTest
from prj.apps.combine.universal_features.app import CombineUniversalFeaturesApp

from .data import (
    TEST_DB,
    FILTER_DATA,
    SOURCE_DATA,
    FILTER_TABLE,
    SOURCE_TABLE,
    TARGET_TABLE,
    TMP_HDFS_DIR,
    FILTER_SCHEMA,
    SOURCE_TABLE_SCHEMA,
    TARGET_SCHEMA,
    FILTER_PARTITION_COLUMNS,
    SOURCE_PARTITION_COLUMNS,
    TARGET_PARTITION_COLUMNS,
    generate_test_data,
)
from .tools import HiveDataIOSpark


class TestCombineUniversalFeatures(ControlAppTest):
    app_class = CombineUniversalFeaturesApp
    test_data = generate_test_data()
    hdfs = HdfsClient()

    @classmethod
    def setup_cls(cls):
        cls.hdfs.mkdir(TMP_HDFS_DIR, remove_if_exists=True)

        with create_spark_session() as spark:

            HiveDataIOSpark(database=TEST_DB, table=SOURCE_TABLE).drop_table().set_schema(
                SOURCE_TABLE_SCHEMA, SOURCE_PARTITION_COLUMNS
            ).create_table().insert_data(SOURCE_DATA, os.path.join(TMP_HDFS_DIR, SOURCE_TABLE)).commit(spark)

            HiveDataIOSpark(database=TEST_DB, table=FILTER_TABLE).drop_table().set_schema(
                FILTER_SCHEMA, FILTER_PARTITION_COLUMNS
            ).create_table().insert_data(FILTER_DATA, os.path.join(TMP_HDFS_DIR, FILTER_TABLE)).commit(spark)

            HiveDataIOSpark(database=TEST_DB, table=TARGET_TABLE).drop_table().set_schema(
                TARGET_SCHEMA, TARGET_PARTITION_COLUMNS
            ).create_table().commit(spark)

    @classmethod
    def teardown_cls(cls):
        with create_spark_session() as spark:
            HiveDataIOSpark(database=TEST_DB, table=SOURCE_TABLE).drop_table().commit(spark)
            HiveDataIOSpark(database=TEST_DB, table=FILTER_TABLE).drop_table().commit(spark)
            HiveDataIOSpark(database=TEST_DB, table=TARGET_TABLE).drop_table().commit(spark)

        cls.hdfs.remove(TMP_HDFS_DIR, recursive=True, skip_trash=True, force=True)

    def check_output(self, task_config, spec):
        columns = ["uid", "score", "score_list", "score_map", "cat_list", "feature_name", "dt", "uid_type"]
        expected_data = [  # use null for omitted data
            {name: row.get(name) for name in columns} for row in spec["expected_data"]
        ]
        expected_pdf = pd.DataFrame(expected_data)[columns].sort_values(by=["uid", "uid_type"])

        with create_spark_session() as spark:
            got_pdf = pd.DataFrame(
                HiveDataIOSpark(TEST_DB, TARGET_TABLE)
                .collect_data(
                    partitions=[{"feature_name": task_config["feature_name"]}],
                    sort_by=["uid", "uid_type"],
                )
                .commit(spark)
                .rows
            )[columns]

        print("Got:\n{}".format(got_pdf.to_string(index=False)))
        print("Expected:\n{}".format(expected_pdf.to_string(index=False)))
        pdt.assert_frame_equal(got_pdf, expected_pdf.reset_index(drop=True), check_dtype=False)


@contextmanager
def create_spark_session(hive_support=True, parallelism=1):
    assert 0, (hive_support, parallelism)
    lock = LockFile("local.spark.session")
    try:
        lock.acquire(5 * 60)
    except LockError as e:
        raise RuntimeError("Spark Session Lock error: {}".format(e))

    # fix config shared with executors
    _psp = os.environ["PYSPARK_PYTHON"]
    os.environ["PYSPARK_PYTHON"] = os.path.join(os.environ["WORKSPACE"], _psp)

    conf = {
        "spark.driver.host": "localhost",
        "spark.master": "local[{}]".format(parallelism),
        "spark.default.parallelism": parallelism,
        "spark.sql.shuffle.partitions": parallelism,
        "spark.ui.enabled": "false",
        "hive.exec.dynamic.partition": "true",
        "hive.exec.dynamic.partition.mode": "nonstrict",
    }
    if hive_support:  # spark.enableHiveSupport()
        conf["spark.sql.catalogImplementation"] = "hive"

    spark = SparkSession.builder.config(conf=SparkConf().setAll(conf.items())).getOrCreate()

    yield spark

    spark.stop()
    os.environ["PYSPARK_PYTHON"] = _psp
    lock.release()
