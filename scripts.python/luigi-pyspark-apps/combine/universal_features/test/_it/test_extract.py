# flake8: noqa
import os

from pprint import pformat

import six
import pandas as pd
import pytest
import pandas.testing as pdt

from prj.apps.utils.common import add_days
from prj.apps.export.test.it.service import show_spark_df, e2e_data_to_it, show_pandas_df
from prj.apps.combine.universal_features.app import CombineUniversalFeaturesTask
from prj.apps.utils.control.client.status import FatalStatusException

from .data import (
    TEST_DB,
    FILTER_DATA,
    SOURCE_DATA,
    FILTER_TABLE,
    SOURCE_TABLE,
    TARGET_TABLE,
    FILTER_SCHEMA,
    SOURCE_TABLE_SCHEMA,
    TARGET_SCHEMA,
    FILTER_PARTITION_COLUMNS,
    SOURCE_PARTITION_COLUMNS,
    TARGET_PARTITION_COLUMNS,
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
        parallelism=1,
    )
    spark.sql("CREATE DATABASE IF NOT EXISTS user_dmdesc")
    spark.sql("DROP FUNCTION IF EXISTS user_dmdesc.map_key_values")
    spark.sql("CREATE FUNCTION user_dmdesc.map_key_values AS 'brickhouse.udf.collect.MapKeyValuesUDF'")
    spark.sql("DROP FUNCTION IF EXISTS user_dmdesc.collect")
    spark.sql("CREATE FUNCTION user_dmdesc.collect AS 'brickhouse.udf.collect.CollectUDAF'")

    def configs():
        for db, table, schema, parts, rows in [
            (
                    TEST_DB,
                    SOURCE_TABLE,
                    SOURCE_TABLE_SCHEMA,
                    SOURCE_PARTITION_COLUMNS,
                    SOURCE_DATA,
            ),
            (
                TEST_DB,
                FILTER_TABLE,
                FILTER_SCHEMA,
                FILTER_PARTITION_COLUMNS,
                FILTER_DATA,
            ),
            (
                TEST_DB,
                TARGET_TABLE,
                TARGET_SCHEMA,
                TARGET_PARTITION_COLUMNS,
                [],
            ),
        ]:
            yield {
                "db": db,
                "table": table,
                "schema": schema,
                "partition_columns": parts,
                "data": rows,
            }

    for conf in configs():
        data = e2e_data_to_it(**conf)
        print("\nLoading source data to Hive: {} ...".format(pformat(data).split("\n")[0]))
        hadoop_env.add_data(data, data.keys())

    return hadoop_env


class TestCombineUniversalFeaturesTask(object):
    test_data = generate_test_data("/tmp/foo/bar/")

    @pytest.mark.parametrize("task_conf, data_conf, idx", test_data)
    def test_task_main(self, task_conf, data_conf, idx, session_temp_dir, custom_hadoop_env):
        print("\nCombine app test #{}, temp dir `{}`...".format(idx, session_temp_dir))

        # stupid hack: temp dir fixture in test data
        self.test_data = generate_test_data(session_temp_dir)
        task_conf, data_conf, _ = self.test_data[idx - 1]
        hadoop_env = custom_hadoop_env

        def temp_path(*dirs):
            return os.path.join(session_temp_dir, *dirs)

        def run_task():
            # imitation of app.prepare_config
            tc = self._prepare_config(task_conf)
            print("\nTask config prepared: {}\n".format(pformat(tc)))
            print("\nData config: {}\n".format(pformat(data_conf)))

            # execute task main
            t = CombineUniversalFeaturesTask(**tc)
            t.tmp_hdfs_dir = temp_path("task-tmp_hdfs_dir_{}".format(idx))
            # for name, _ in six.iteritems(t.HIVE_UDF):
            #     hadoop_env.spark.sql("DROP TEMPORARY FUNCTION IF EXISTS {}".format(name))
            assert t.conf["spark.sql.shuffle.partitions"] == task_conf["shuffle_partitions"]
            t.main(hadoop_env.spark.sparkContext)

            return t

        if "exception" in data_conf:
            with pytest.raises(data_conf["exception"]) as e:
                task = run_task()
            print("Exception: {}".format(pformat(e)))
            return

        # happy path
        task = run_task()

        if data_conf.get("expect_fatal", False):
            print("\nChecking FATAL state in output_urls ...")
            assert task.control_client.get_status(task.output_urls[0]) == [
                {"name": "success", "type": "flag", "value": False}
            ]
        else:  # happy path
            self._check_result(task, task_conf, data_conf, hadoop_env)

    @staticmethod
    def _prepare_config(config):
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

    @staticmethod
    def _check_result(task, task_conf, data_conf, hadoop_env):
        columns = ["uid", "score", "score_list", "score_map", "cat_list", "feature_name", "dt", "uid_type"]
        expected_data = [
            {name: row.get(name, float("nan") if name == "score" else None) for name in columns}
            for row in data_conf["expected_data"]
        ]
        expected_pdf = pd.DataFrame(expected_data)[columns].sort_values(by=["uid", "uid_type"])

        print("\nExpected:")
        show_pandas_df(expected_pdf)

        df = hadoop_env.spark.sql(
            "select * from {}.{} where feature_name='{}' order by uid, uid_type".format(
                task_conf["target_db"], task_conf["target_table"], task_conf["feature_name"]
            )
        )
        df = _show(df, "Actual")
        assert df.count() == len(expected_data)
        got_pdf = df.toPandas()[columns]

        print("\nActual:")
        show_pandas_df(got_pdf)

        print("Got:\n{}".format(got_pdf.to_string(index=False)))
        print("Expected:\n{}".format(expected_pdf.to_string(index=False)))
        pdt.assert_frame_equal(got_pdf, expected_pdf.reset_index(drop=True), check_dtype=False)


def _show(df, msg="DF"):
    from prj.apps.export.test.it.service import show_spark_df
    return show_spark_df(df, msg)
