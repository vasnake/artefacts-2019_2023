# flake8: noqa

import os
import json
from pprint import pformat

import pytest

from prj.apps.export.test.it.service import HiveMetastoreClient

from prj.apps.export.universal_features.app import ExportUniversalFeaturesApp, ExportUniversalFeaturesTask
from .data import generate_test_data


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

    spark.sql("DROP TEMPORARY FUNCTION IF EXISTS user_dmdesc_collect")
    spark.sql("CREATE TEMPORARY FUNCTION user_dmdesc_collect AS 'brickhouse.udf.collect.CollectUDAF'")

    return hadoop_env


class TestExportUniversalFeatures(object):
    @pytest.mark.parametrize("task_conf, data_conf, idx", generate_test_data("/tmp/export/uf/test_root"))
    def test_task_main(self, task_conf, data_conf, idx, session_temp_dir, custom_hadoop_env):
        # from sklearn.utils import murmurhash3_32
        # str(murmurhash3_32("SOCIAL", positive=True))

        hadoop_env = custom_hadoop_env

        def abs_path(*dirs):
            return os.path.normpath(os.path.join(session_temp_dir, *dirs))

        # set actual temp dir
        task_conf["target_hdfs_basedir"] = abs_path("result_{}".format(idx), "")
        task_conf["input_urls"] = [abs_path("input_{}".format(idx))]
        task_conf["output_urls"] = [abs_path("output_{}".format(idx))]
        task_conf["status_urls"] = [abs_path("status_{}".format(idx))]

        data_conf["source"]["db"] = task_conf["source_db"]
        data_conf["source"]["table"] = task_conf["source_table"]

        self._load_data(data_conf, hadoop_env)

        def run_task():
            # imitation of app.prepare_config
            tc = self._prepare_config(task_conf)
            print("\nTask config prepared: {}\n".format(pformat(tc)))
            print("\nTest config: {}\n".format(pformat(data_conf)))

            # execute task main
            t = ExportUniversalFeaturesTask(**tc)
            t.tmp_hdfs_dir = abs_path("task_root_temp_{}".format(idx))
            t.main(hadoop_env.spark.sparkContext)

            return t

        task = run_task()

        assert task.target_hdfs_dir.endswith(os.path.join(task_conf["features_subdir"], task_conf["target_dt"], ""))
        self._check_content(task.target_hdfs_dir, task_conf["output_max_rows_per_file"], data_conf["expected_data"])

    @staticmethod
    def _prepare_config(config):
        class App(ExportUniversalFeaturesApp):
            config = {}

            def __init__(self):
                from prj.apps.utils.common.hive import FindPartitionsEngine
                self.partitions_finder = FindPartitionsEngine(raise_on_invalid_table=False)

            def info(self, msg, *args, **kwargs):
                print(msg)

        print("\nApp config:\n`{}`".format(json.dumps(config, indent=4)))
        return App().prepare_config(config)

    def _check_content(self, dir_path, max_rows, expected_data):
        assert max_rows > 0
        assert os.path.isfile(os.path.join(dir_path, "_SUCCESS"))

        def list_files(path, suff):
            for p in os.listdir(path):
                if p.endswith(suff):
                    yield os.path.join(path, p)

        files = list(list_files(dir_path, ".csv"))
        assert len(files) > 0
        # n.b. +/- 1 related to header line
        collected = []
        for fn in files:
            with open(fn) as f:
                lines = [x[:-1] for x in f.readlines()]  # remove trailing `\n`
                if not lines:
                    print("empty file `{}`".format(fn))
                    continue
                assert len(lines) <= max_rows + 1 + 1  # +1 for header, +1 for repartition glitch
                assert lines[0] == expected_data[0]  # header eq
                collected += lines[1:]
        assert len(collected) == len(expected_data) - 1
        assert sorted(expected_data[1:]) == sorted(collected)

    @staticmethod
    def _load_data(data_conf, hadoop_env):
        def _values(row, schema):
            values = [row[n] for n, _ in schema]
            return tuple(values)

        src = data_conf["source"]
        table = "{}.{}".format(src["db"], src["table"])
        data = {
            table: {
                "partitions": src["partition_columns"],
                "schema": [(n, HiveMetastoreClient.Field.encode(t)) for n, t in src["schema"]],
                "rows": [_values(row, src["schema"]) for row in src["data"]],
            }
        }
        print("\nLoading data to Hive, conf:")
        print(pformat(data))

        hadoop_env.add_data(data, [table])
