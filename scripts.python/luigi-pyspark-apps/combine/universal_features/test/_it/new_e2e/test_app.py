# flake8: noqa

import os
import re
import abc
import sys
import copy
import json
import shutil
import tempfile
import traceback

from pprint import pformat
from operator import itemgetter

import six
import luigi
import numpy as np
import pandas as pd
import pytest

from prj.logs import LoggingMixin

# from . import HdfsClient, get_hive_metastore_client
from prj.apps.export.test.it.service import HdfsClient, get_hive_metastore_client
from prj.apps.combine.universal_features.app import CombineUniversalFeaturesApp, CombineUniversalFeaturesTask

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
from .hive_io_spark import HiveDataIOSpark


@pytest.fixture(scope="session")
def session_temp_dir(request):
    print("\nSession temp dir ...")
    assert 0, request
    temp_dir = tempfile.mkdtemp()
    print("session_temp_dir fixture, created dir `{}`\n".format(temp_dir))
    yield temp_dir
    shutil.rmtree(temp_dir)


@pytest.fixture(scope="session", autouse=True)
def mocks_session(monkeysession):
    print("\nSession moks ...")
    assert 0, monkeysession
    monkeysession.setattr("prj.apps.utils.control.luigix.task.HdfsClient", HdfsClient)
    monkeysession.setattr("prj.apps.utils.common.fs.hdfs.HdfsClient", HdfsClient)
    monkeysession.setattr("prj.apps.utils.common.fs.HdfsClient", HdfsClient)
    monkeysession.setattr("prj.apps.utils.common.hive.HiveMetastoreClient", get_hive_metastore_client)
    return


catalog = {
    "{}.{}".format(TEST_DB, SOURCE_TABLE): {
        "partition_columns": SOURCE_PARTITION_COLUMNS,
        "schema": SOURCE_TABLE_SCHEMA,
    },
    "{}.{}".format(TEST_DB, FILTER_TABLE): {
        "partition_columns": FILTER_PARTITION_COLUMNS,
        "schema": FILTER_SCHEMA,
    },
    "{}.{}".format(TEST_DB, TARGET_TABLE): {
        "partition_columns": TARGET_PARTITION_COLUMNS,
        "schema": TARGET_SCHEMA,
    },
}


class TestCombineUniversalFeaturesIT(LoggingMixin):
    test_forced_run = False
    app_class = CombineUniversalFeaturesApp
    test_data = generate_test_data()
    hdfs = HdfsClient()

    _session_temp_dir = "/tmp/foo/bar/"
    _spark = None

    @pytest.fixture(scope="class", autouse=True)
    def prepare_class(self, session_temp_dir):
        self.setup_cls(session_temp_dir)
        yield
        self.teardown_cls()

    @classmethod
    def setup_cls(cls, session_temp_dir):
        cls._session_temp_dir = session_temp_dir
        spark = create_spark_session(session_temp_dir, hive_support=True, parallelism=1)
        cls._spark = spark
        get_hive_metastore_client().set_spark(spark).set_catalog(catalog)
        TMP_HDFS_DIR = os.path.join(cls._session_temp_dir, "tmp")

        cls.hdfs.mkdir(TMP_HDFS_DIR, remove_if_exists=True)

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
        TMP_HDFS_DIR = os.path.join(cls._session_temp_dir, "tmp")

        HiveDataIOSpark(database=TEST_DB, table=SOURCE_TABLE).drop_table()
        HiveDataIOSpark(database=TEST_DB, table=FILTER_TABLE).drop_table()
        HiveDataIOSpark(database=TEST_DB, table=TARGET_TABLE).drop_table()
        cls.hdfs.remove(TMP_HDFS_DIR, recursive=True, skip_trash=True, force=True)

    @property
    def session_temp_dir(self):
        return self._session_temp_dir

    @property
    def spark(self):
        if self._spark is not None:
            return self._spark
        self._spark = create_spark_session(self.session_temp_dir, hive_support=True, parallelism=1)
        return self._spark

    def abs_tmp_path(self, *dirs):
        return os.path.join(self.session_temp_dir, *dirs)

    def test_app_correct(self):
        for tc, ed in self.test_data:
            ctid = tc["ctid"]
            tc["input_urls"] = [self.abs_tmp_path("input_{}".format(ctid))]
            tc["output_urls"] = [self.abs_tmp_path("output_{}".format(ctid))]
            tc["status_urls"] = [self.abs_tmp_path("status_{}".format(ctid))]
            self.run_task(tc)
            self.check_output(tc, ed)

    def run_task(self, task_config):
        ctid = task_config["ctid"]
        spark = self.spark
        tc = self.prepare_config(task_config)
        print("\nTask config prepared: {}\n".format(pformat(tc)))

        task = CombineUniversalFeaturesTask(**tc)

        task.tmp_hdfs_dir = self.abs_tmp_path("task-tmp_hdfs_dir_{}".format(ctid))
        # for name, _ in six.iteritems(task.HIVE_UDF):
        #     spark.sql("DROP TEMPORARY FUNCTION IF EXISTS {}".format(name))
        assert task.conf["spark.sql.shuffle.partitions"] == task_config["shuffle_partitions"]

        task.main(spark.sparkContext)

    @staticmethod
    def prepare_config(config):
        from prj.apps.combine.universal_features.app import CombineUniversalFeaturesApp

        class App(CombineUniversalFeaturesApp):
            config = {}

            def __init__(self):
                pass

            def info(self, msg, *args, **kwargs):
                print(msg)

        import json

        print("\nApp config:\n`{}`".format(json.dumps(config, indent=4)))

        app = App()
        return app.prepare_config(config)

    def check_output(self, task_config, spec):
        columns = ["uid", "score", "score_list", "score_map", "cat_list", "feature_name", "dt", "uid_type"]
        expected_data = [
            # {name: row.get(name, np.nan if name == "score" else "") for name in columns}
            {name: row.get(name) for name in columns}
            for row in spec["expected_data"]
        ]
        expected_pdf = pd.DataFrame(expected_data)[columns].sort_values(by=["uid", "uid_type"])

        print("\nExpected pdf:\n")
        show_pandas_df(expected_pdf)

        got_pdf = pd.DataFrame(
            HiveDataIOSpark(TEST_DB, TARGET_TABLE)
            .collect_data(
                partitions=[{"feature_name": task_config["feature_name"]}],
                tmp_hdfs_dir=os.path.join(TMP_HDFS_DIR, task_config["ctid"], "hdio_read"),
                sort_by=["uid", "uid_type"],
            )
            .commit(self.spark)
            .rows
        )[columns]

        print("\nActual pdf:\n")
        show_pandas_df(got_pdf)

        print("Got:\n{}".format(got_pdf.to_string(index=False)))
        print("Expected:\n{}".format(expected_pdf.to_string(index=False)))
        pdt.assert_frame_equal(got_pdf, expected_pdf.reset_index(drop=True), check_dtype=False)


def show_pandas_df(df, debug=False):
    """Print pandas df as a nice table, spark df.show-like.

    Copy-paste from SO.
    """
    # assert isinstance(df, pd.DataFrame)
    if debug:
        print(df.to_string(index=False))
    df_columns = df.columns.tolist()
    max_len_in_lst = lambda lst: len(sorted(lst, reverse=True, key=len)[0])  # noqa: E731
    align_center = (
        lambda st, sz: "{0}{1}{0}".format(" " * (1 + (sz - len(st)) // 2), st)[:sz] if len(st) < sz else st
    )  # noqa: E731
    align_right = lambda st, sz: "{0}{1} ".format(" " * (sz - len(st) - 1), st) if len(st) < sz else st  # noqa: E731
    max_col_len = max_len_in_lst(df_columns)
    max_val_len_for_col = dict(
        [(col, max_len_in_lst(df.iloc[:, idx].astype("str"))) for idx, col in enumerate(df_columns)]
    )
    col_sizes = dict([(col, 2 + max(max_val_len_for_col.get(col, 0), max_col_len)) for col in df_columns])
    build_hline = lambda row: "+".join(["-" * col_sizes[col] for col in row]).join(["+", "+"])  # noqa: E731
    build_data = lambda row, align: "|".join(  # noqa: E731
        [align(str(val), col_sizes[df_columns[idx]]) for idx, val in enumerate(row)]
    ).join(["|", "|"])
    hline = build_hline(df_columns)
    out = [hline, build_data(df_columns, align_center), hline]
    for _, row in df.iterrows():
        out.append(build_data(row.tolist(), align_right))
    out.append(hline)
    print("\n".join(out))


def create_spark_session(session_dir, conf_update=None, hive_support=False, parallelism=2):
    assert 0, session_dir
    from pyspark.sql import SparkSession
    from pyspark.conf import SparkConf

    parallelism = os.environ.get("PYTEST_SPARK_PARALLELISM", parallelism)
    conf = {
        "spark.hadoop.mapred.output.compress": "true",
        "spark.hadoop.mapred.output.compression.codec": "org.apache.hadoop.io.compress.GzipCodec",
        "spark.driver.host": "localhost",
        "spark.master": "local[{}]".format(parallelism),
        "spark.default.parallelism": parallelism,
        "spark.sql.shuffle.partitions": parallelism,
        "spark.ui.enabled": "false",
        "hive.exec.dynamic.partition": "true",
        "hive.exec.dynamic.partition.mode": "nonstrict",
        "spark.sql.warehouse.dir": os.path.join(session_dir, "spark-warehouse"),
        "spark.driver.extraJavaOptions": "-Dderby.system.home={}".format(os.path.join(session_dir, "derby")),
    }
    if "SPARK_JARS" in os.environ:
        conf["spark.jars"] = os.environ["SPARK_JARS"]
    if hive_support:  # spark.enableHiveSupport()
        conf["spark.sql.catalogImplementation"] = "hive"

    if conf_update:
        conf.update(**conf_update)

    spark = SparkSession.builder.config(conf=SparkConf().setAll(conf.items())).getOrCreate()

    if "ADD_JAR" in os.environ:
        for jar in os.environ["ADD_JAR"].split(","):
            spark.sql("ADD JAR {}".format(jar))

    spark.sql("CREATE DATABASE IF NOT EXISTS user_dmdesc")
    spark.sql("DROP FUNCTION IF EXISTS user_dmdesc.map_key_values")
    spark.sql("CREATE FUNCTION user_dmdesc.map_key_values AS 'brickhouse.udf.collect.MapKeyValuesUDF'")
    spark.sql("DROP FUNCTION IF EXISTS user_dmdesc.collect")
    spark.sql("CREATE FUNCTION user_dmdesc.collect AS 'brickhouse.udf.collect.CollectUDAF'")

    return spark
