# flake8: noqa

import os
import json
import math

from pprint import pformat

import six
import pytest

from prj.apps.export.ad_features.app import ExportAdFeaturesApp, ExportAdFeaturesTask
from prj.apps.export.test.it.service import e2e_data_to_it

from .data import (
    TEST_DB,
    SOURCE_DATA,
    SOURCE_TABLE,
    SOURCE_TABLE_SCHEMA,
    SOURCE_PARTITION_COLUMNS,
    generate_test_data,
)


@pytest.fixture(scope="session")
def custom_hadoop_env(hadoop_env):
    print("\nSession custom hadoop env ...")

    spark = hadoop_env.custom_spark(
        conf={
            "spark.hadoop.mapred.output.compress": "false",
            "spark.hadoop.mapred.output.compression.codec": "org.apache.hadoop.io.compress.SnappyCodec",
        },
        hive_support=True,
        parallelism=3,
    )

    user_dmdesc_functions = {
        "map_key_values": "brickhouse.udf.collect.MapKeyValuesUDF",
        "collect": "brickhouse.udf.collect.CollectUDAF",
    }
    spark.sql("CREATE DATABASE IF NOT EXISTS user_dmdesc")
    for n, c in six.iteritems(user_dmdesc_functions):
        spark.sql("DROP FUNCTION IF EXISTS user_dmdesc.{}".format(n))
        spark.sql("CREATE FUNCTION user_dmdesc.{} AS '{}'".format(n, c))

    def configs():
        # fmt: off
        for db, table, schema, parts, rows in [
            (TEST_DB, SOURCE_TABLE, SOURCE_TABLE_SCHEMA, SOURCE_PARTITION_COLUMNS, SOURCE_DATA,),
        ]:
            yield {"db": db, "table": table, "schema": schema, "partition_columns": parts, "data": rows,}
        # fmt: on

    for conf in configs():
        data = e2e_data_to_it(**conf)
        print("\nLoading source data to Hive: {} ...".format(pformat(data).split("\n")[0]))
        hadoop_env.add_data(data, data.keys())

    print("\nSession custom hadoop env done.")
    return hadoop_env


class TestExportAdFeaturesTask(object):
    test_data = generate_test_data("/tmp/foo/bar/")

    @pytest.mark.parametrize("task_conf, test_conf, idx", test_data)
    def test_task_main(self, task_conf, test_conf, idx, session_temp_dir, custom_hadoop_env):
        print("\nBanner Export App Test #{}, temp dir `{}`...".format(idx, session_temp_dir))

        def abs_path(*dirs):
            return os.path.join(session_temp_dir, *dirs)

        # stupid hack: temp dir fixture in test data
        self.test_data = generate_test_data(session_temp_dir)
        task_conf, test_conf, _ = self.test_data[idx - 1]
        hadoop_env = custom_hadoop_env
        self.hdfs = hadoop_env.hdfs

        def run_task():
            # imitation of app.prepare_config
            tc = self._prepare_config(task_conf)
            print("\nTask config prepared: {}\n".format(pformat(tc)))
            print("\nTest config: {}\n".format(pformat(test_conf)))

            # execute task main
            t = ExportAdFeaturesTask(**tc)
            t.tmp_hdfs_dir = abs_path("task-tmp_hdfs_dir-{}".format(idx))
            t.main(hadoop_env.spark.sparkContext)

            return t

        exception = test_conf.get("exception")
        if exception:
            with pytest.raises(exception) as e:
                task = run_task()
            print("\nExpected exception: {}".format(pformat(e)))
            return

        # happy path
        task = run_task()

        if test_conf.get("expect_fatal"):
            print("\nChecking FATAL state in output_urls ...")
            assert task.control_client.get_status(task.output_urls[0])[0]["type"] == "exception"
            return

        # happy path
        self._check_result(task, task_conf, test_conf, hadoop_env)

    @staticmethod
    def _prepare_config(config):
        class App(ExportAdFeaturesApp):
            config = {}

            def __init__(self):
                from prj.apps.utils.common.hive import FindPartitionsEngine
                self.partitions_finder = FindPartitionsEngine(raise_on_invalid_table=False)

            def info(self, msg, *args, **kwargs):
                print(msg)

        print("\nApp config:\n`{}`".format(json.dumps(config, indent=4)))
        return App().prepare_config(config)

    def _check_result(self, task, task_config, expected_data, hadoop_env):
        # set of CSV files in given target dir, with given content
        max_rows_per_file = task_config["output_max_rows_per_file"]

        expected_header = expected_data["expected_data"][0]
        expected_rows = expected_data["expected_data"][1:]
        expected_num_files = int(math.ceil(float(len(expected_rows)) / float(max_rows_per_file)))

        expected_target_dir = os.path.join(task_config["target_hdfs_basedir"], task_config["features_subdir"], task_config["target_dt"], "")
        assert task.target_hdfs_dir == expected_target_dir

        files = list(self.hdfs.listdir(task.target_hdfs_dir))
        self.info("Target directory, files:\n{}".format(pformat(files)))
        csv_files = filter(lambda x: x.endswith(".csv"), files)
        assert len(csv_files) == expected_num_files

        got_rows = []
        for path in csv_files:
            with self.hdfs.open(path) as fd:
                lines = [line.strip() for line in fd]

            self.debug("File `{}` content:\n{}".format(path, pformat(lines)))
            assert 1 < len(lines) <= max_rows_per_file + 2  # +1 for header line, +1 for repartition margins
            assert lines[0] == expected_header
            got_rows += lines[1:]

        self.info("Got:\n{}".format(pformat(got_rows)))
        self.info("Expected:\n{}".format(pformat(expected_rows)))
        assert sorted(got_rows) == sorted(expected_rows)

    @classmethod
    def info(cls, message):
        print(message)

    @classmethod
    def debug(cls, message):
        print(message)
