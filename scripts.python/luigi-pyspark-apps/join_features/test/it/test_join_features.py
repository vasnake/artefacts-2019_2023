# flake8: noqa

import os
import copy
import json

from pprint import pformat

import six
import numpy as np
import pandas as pd
import pytest
import pandas.testing as pdt

from prj.apps.joiner.features.app import JoinerFeaturesApp, JoinerFeaturesTask
from prj.apps.export.test.it.service import show_spark_df, e2e_data_to_it, show_pandas_df, fix_control_urls_path

from .data import (
    MD_DATA,
    TEST_DB,
    MD_TABLE,
    MD_SCHEMA,
    SOURCE_DATA,
    SOURCE_TABLE,
    MATCHING_DATA,
    SOURCE_SCHEMA,
    MATCHING_TABLE,
    MATCHING_SCHEMA,
    MD_PARTITION_COLUMNS,
    SOURCE_PARTITION_COLUMNS,
    MATCHING_PARTITION_COLUMNS,
    generate_test_data,
)


@pytest.fixture(scope="session")
def custom_hadoop_env(hadoop_env):
    print("\nJoinerFeatures custom hadoop env ...")

    # create custom spark session

    spark = hadoop_env.custom_spark(
        conf={
            "spark.hadoop.mapred.output.compress": "false",
            "spark.hadoop.mapred.output.compression.codec": "org.apache.hadoop.io.compress.SnappyCodec",

            # min, max functions returns null for null input if `spark.sql.legacy.sizeOfNull` is set to false
            "spark.sql.legacy.sizeOfNull": "false",
        },
        hive_support=True,
        parallelism=1,
    )

    # setup UDF

    spark.sql("CREATE DATABASE IF NOT EXISTS user_dmdesc")
    spark.sql("DROP FUNCTION IF EXISTS user_dmdesc.map_key_values")
    spark.sql("CREATE FUNCTION user_dmdesc.map_key_values AS 'brickhouse.udf.collect.MapKeyValuesUDF'")
    spark.sql("DROP FUNCTION IF EXISTS user_dmdesc.combine")
    spark.sql("CREATE FUNCTION user_dmdesc.combine AS 'brickhouse.udf.collect.CombineUDF'")
    spark.sql("DROP FUNCTION IF EXISTS user_dmdesc.collect")
    spark.sql("CREATE FUNCTION user_dmdesc.collect AS 'brickhouse.udf.collect.CollectUDAF'")

    # load hive tables

    def _tables_descr():
        # fmt: off
        for db, table, schema, parts, rows in [
            (TEST_DB, SOURCE_TABLE, SOURCE_SCHEMA, SOURCE_PARTITION_COLUMNS, SOURCE_DATA,),
            (TEST_DB, MD_TABLE, MD_SCHEMA, MD_PARTITION_COLUMNS, MD_DATA),
            (TEST_DB, MATCHING_TABLE, MATCHING_SCHEMA, MATCHING_PARTITION_COLUMNS, MATCHING_DATA,),
        ]:
            yield {"db": db, "table": table, "schema": schema, "partition_columns": parts, "data": rows}
        # fmt: on

    for td in _tables_descr():
        data = e2e_data_to_it(**td)
        print("\nLoading source data to Hive: {} ...".format(pformat(data).split("\n")[0]))
        hadoop_env.add_data(data, data.keys())

    print("\nJoinerFeatures custom hadoop env created.")
    return hadoop_env


class TestJoinerFeaturesTask(object):
    test_data = [
        (task_conf, expected_obj, i)
        for i, (task_conf, expected_obj) in enumerate(generate_test_data(), 1)
    ]

    @pytest.mark.parametrize("task_conf, expected_obj, idx", test_data)
    def test_task_run(self, task_conf, expected_obj, idx, custom_hadoop_env):
        hadoop_env = custom_hadoop_env
        print(
            "\nJoinerFeatures app test #{}/{}, session temp dir `{}` ...\n".format(
                idx, len(self.test_data), hadoop_env.temp_dir
            )
        )

        def _temp_path(*dirs):
            return os.path.join(hadoop_env.temp_dir, *dirs)

        def _run_task(task_conf):
            # JoinerFeaturesApp.prepare_config
            prepared_task_conf = self._prepare_config(task_conf)
            print("\nJoinerFeaturesTask config prepared: {}\n".format(pformat(prepared_task_conf)))

            # execute task
            task = JoinerFeaturesTask(**prepared_task_conf)
            for subtask in task.requires():
                subtask.tmp_hdfs_dir = _temp_path(
                    "JoinerFeaturesTask-tmp_hdfs_dir-{}-{}".format(idx, subtask.uid_type_conf["output"])
                )
                subtask.main(hadoop_env.spark.sparkContext)
            return task

        fixed_task_conf = fix_control_urls_path(task_conf, hadoop_env.temp_dir, hadoop_env)
        # fixed_task_conf["hdfs_basedir"] = _temp_path("signal_basedir", "_{}".format(idx))
        # fixed_task_conf["shuffle_partitions"] = fixed_task_conf.get("shuffle_partitions", 1)
        # fixed_task_conf["min_target_rows"] = fixed_task_conf.get("min_target_rows", 1)
        # fixed_task_conf["checkpoint_interval"] = fixed_task_conf.get("checkpoint_interval", 1)
        # fixed_task_conf["executor_memory_gb"] = fixed_task_conf.get("executor_memory_gb", 1)
        # fixed_task_conf["output_max_rows_per_bucket"] = fixed_task_conf.get("output_max_rows_per_bucket", 1)

        # TODO: remove after new jar deployment (added built-in read_as_big_orc support)
        for source_name, source_conf in six.iteritems(fixed_task_conf["sources"]):
            source_conf["read_as_big_orc"] = False

        if "exception" in expected_obj:
            with pytest.raises(expected_obj["exception"]) as e:
                _ = _run_task(fixed_task_conf)
            print("Exception: {}".format(pformat(e)))
            return

        # happy path
        task = _run_task(fixed_task_conf)
        self._check_result(task, fixed_task_conf, expected_obj, hadoop_env)

    @staticmethod
    def _prepare_config(config):
        class App(JoinerFeaturesApp):
            config = []

            def __init__(self, *args, **kwargs):
                # super(App, self).__init__(*args, **kwargs)
                from prj.apps.utils.common.hive import FindPartitionsEngine

                self.partitions_finder = FindPartitionsEngine(raise_on_invalid_table=False)

            def info(self, msg, *args, **kwargs):
                print(msg)

        print("\nJoinerFeaturesApp config:\n`{}`".format(json.dumps(config, indent=2)))
        return App().prepare_config(config)

    def _check_result(self, task, task_conf, expected_obj, hadoop_env, skip_status_check=True):

        # TODO: rewrite status checks
        if not expected_obj.get("rows"):
            print("\nExpected task failure")
            if not skip_status_check:
                print("\nChecking FATAL state in output_urls ...")
                assert task.control_client.get_status(task.output_urls[0]) == [
                    {"name": "success", "type": "flag", "value": False}
                ]
            return
        else:
            if not skip_status_check:
                assert task.control_client.get_status(task.output_urls[0]) == [
                    {"name": "success", "type": "flag", "value": True}
                ]

        columns = expected_obj["columns"]

        # expected
        expected_pdf = (pd.DataFrame(
            expected_obj["rows"]
        )[columns].sort_values(by=["uid", "uid_type"]).reset_index(drop=True))
        print("\nExpected, pandas DF:")
        show_pandas_df(expected_pdf)

        # actual
        df = hadoop_env.spark.sql(
            "select * from {}.{} where dt='{}' order by uid, uid_type".format(
                task_conf["target_db"], task_conf["target_table"], task_conf["target_dt"]
            )
        )
        df = show_spark_df(df, "Actual spark DF")
        got_pdf = df.toPandas()[columns]
        print("\nActual, pandas DF:")
        show_pandas_df(got_pdf)

        # check
        assert df.count() == len(expected_obj["rows"])

        self.info("Got:\n{}".format(got_pdf.to_string(index=False)))
        self.info("Expected:\n{}".format(expected_pdf.to_string(index=False)))
        pdt.assert_frame_equal(got_pdf, expected_pdf, check_dtype=False)

    def info(self, msg):
        print(msg)


@pytest.fixture(scope="session")
def micro_hadoop_env(hadoop_env):
    spark = hadoop_env.custom_spark(
        conf={
            "spark.hadoop.mapred.output.compress": "false",
            "spark.hadoop.mapred.output.compression.codec": "org.apache.hadoop.io.compress.SnappyCodec",
        },
        hive_support=False,
        parallelism=1,
    )

    return hadoop_env


class TestCreateHiveTableDDL(object):
    FINDER_CAMPAIGNS_SOURCE_TABLE_SCHEMA = (
        ("counter_id", "string"),
        ("uid", "string"),
        ("ev_type", "tinyint"),
        ("goal", "string"),
        ("goal_type", "string"),
        ("`timestamp`", "int"),
        ("dt", "string"),
        ("source_name", "string"),
        ("uid_type", "string"),
    )

    # def test_create_ddl(self, micro_hadoop_env):
    def test_create_ddl(self):
        # hadoop_env = micro_hadoop_env
        # spark = hadoop_env.spark
        # from prj.apps.utils.common import schema_from_hive, hive_from_schema

        schema = [
            ("uid", "string"),
            ("score", "struct<i_score:int,f_score:float,s_data:string,m_data:map<int,array<struct<foo:map<string,float>>>>>"),
            ("dt", "string"),
            ("uid_type", "string", "Type's of uid, e.g. VKID, EMAIL, PHONE"),
            ("category", "string"),
        ]
        schema = self.FINDER_CAMPAIGNS_SOURCE_TABLE_SCHEMA

        from prj.apps.utils.common.hive import create_hive_table_ddl

        ddl = create_hive_table_ddl(
            database="user_vfedulov",
            table="trg_80367_test",
            fields=schema,
            partition_columns=["dt", "source_name", "uid_type"],
        )

        print("\nDDL:\n{}\n".format(ddl))
        assert """``""" not in ddl
