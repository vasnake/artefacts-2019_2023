# flake8: noqa

import pytest
import os

from pprint import pformat

from prj.apps.export.audience.rb.app import ExportAudienceRbTask
from prj.apps.export.test.it.service import e2e_data_to_it

from .data import generate_test_data


@pytest.fixture(scope="session")
def custom_hadoop_env(hadoop_env):
    spark = hadoop_env.custom_spark(
        conf={
            "spark.hadoop.mapred.output.compress": "false",
            "spark.hadoop.mapred.output.compression.codec": "org.apache.hadoop.io.compress.SnappyCodec",
        },
        hive_support=True,
        parallelism=1,
    )
    return hadoop_env


class TestExportAudienceRB(object):
    test_data = generate_test_data("/tmp/foo/bar/")

    @pytest.mark.parametrize("task_conf, data_conf, idx", test_data)
    def test_task_main(self, task_conf, data_conf, idx, session_temp_dir, custom_hadoop_env):
        hadoop_env = custom_hadoop_env
        # stupid hack: temp dir fixture in test data
        self.test_data = generate_test_data(session_temp_dir)
        task_conf, data_conf, idx = self.test_data[idx - 1]

        def temp_path(*dirs):
            return os.path.join(session_temp_dir, *dirs)

        # load src data to hive
        data_conf["source"]["db"] = task_conf["source_db"]
        data_conf["source"]["table"] = task_conf["source_table"]
        data = e2e_data_to_it(**data_conf["source"])
        print("\nLoading data to Hive: {}".format(pformat(data)))
        hadoop_env.add_data(data, data.keys())

        # setup target dir, it should exists with some files
        hadoop_env.hdfs.mkdir(task_conf["target_hdfs_basedir"])
        hadoop_env.hdfs.touchz(os.path.join(task_conf["target_hdfs_basedir"], data_conf["extra_file"]))
        for x in data_conf["previous_attempt_files"]:
            hadoop_env.hdfs.touchz(os.path.join(task_conf["target_hdfs_basedir"], x))

        # imitation of app.prepare_config
        if "exception" in data_conf:
            with pytest.raises(data_conf["exception"]) as e:
                task_conf = self.prepare_config(task_conf)
            print("Exception: {}".format(pformat(e)))
            return
        task_conf = self.prepare_config(task_conf)

        print("\nData conf: {}\n".format(pformat(data_conf)))

        # execute task main
        task = ExportAudienceRbTask(**task_conf)
        task.tmp_hdfs_dir = temp_path("task-tmp_hdfs_dir_{}".format(idx))
        if "exception" in data_conf:
            with pytest.raises(data_conf["exception"]) as e:
                task.main(hadoop_env.spark.sparkContext)
            print("Exception: {}".format(pformat(e)))
            return

        task.main(hadoop_env.spark.sparkContext)
        task.on_success()

        if data_conf.get("expect_fatal", False):
            print("\nChecking FATAL state in output_urls ...")
            assert task.control_client.get_status(task.output_urls[0]) == [
                {"name": "success", "type": "flag", "value": False}
            ]
        else:
            self._check_content(
                success_file_path=data_conf["expected_success_file"],
                target_dir=task_conf["target_hdfs_basedir"],
                task_conf=task_conf,
                data_conf=data_conf,
                hadoop_env=hadoop_env,
            )

    @staticmethod
    def _check_content(success_file_path, target_dir, task_conf, data_conf, hadoop_env):
        """Compare result with expected data.

        - success_file_path: should exists.
        - target_dir: file _SUCCESS should not be found in target_dir;
            - result files from previous job attempt should be deleted;
            - result files from other jobs should NOT be deleted;
            - result files from this job should contain expected data.
        - each result file should contain rows per file less-or-eq than given limit.
        """
        # should exists: success file
        assert os.path.isfile(success_file_path)

        print("\nTarget dir {} contents: {}".format(target_dir, pformat(list(hadoop_env.hdfs.listdir(target_dir)))))

        # should not write _SUCCESS in target
        assert not os.path.exists(os.path.join(target_dir, "_SUCCESS"))

        # result files from previous job attempt should be deleted // UPD: not deleted
        # for x in data_conf["previous_attempt_files"]:
        #     assert not os.path.exists(os.path.join(target_dir, x))

        # should NOT delete files other than generated by this task.
        assert os.path.isfile(os.path.join(target_dir, data_conf["extra_file"]))

        max_rows = task_conf["output_max_rows_per_file"]
        expected_data = sorted(data_conf["expected_data"])  # type: list[str]

        got_data = []
        files = list(hadoop_env.hdfs.listdir(os.path.join(target_dir, data_conf["expected_data_files"])))

        for fn in files:
            print("Reading a data file: `{}`".format(fn))
            lines = []
            with hadoop_env.hdfs.open(fn) as fd:
                for line in fd:
                    print("read line: `{}`".format(line))
                    lines.append(line.strip())
            if not lines:
                continue
            print("File `{}` content:\n{}".format(fn, pformat(lines)))
            assert len(lines) <= max_rows
            got_data += lines
        got_data = sorted(got_data)

        print("Got:\n{}".format(pformat(got_data)))
        print("Expected:\n{}".format(pformat(expected_data)))
        assert expected_data == got_data

    @staticmethod
    def prepare_config(config):
        import json
        from prj.apps.export.audience.rb.app import ExportAudienceRbApp

        class App(ExportAudienceRbApp):
            config = {}

            def __init__(self):
                from prj.apps.utils.common.hive import FindPartitionsEngine
                self.partitions_finder = FindPartitionsEngine(raise_on_invalid_table=False)

            def info(self, msg, *args, **kwargs):
                print(msg)

        print("\nApp config:\n`{}`".format(json.dumps(config, indent=4)))
        app = App()
        tc = app.prepare_config(config)
        print("\nTask config:\n`{}`".format(json.dumps(tc, indent=4)))
        return tc


def _show(df, msg="DF"):
    print("\n# {}\npartitions: {}".format(msg, df.rdd.getNumPartitions()))
    df.printSchema()
    df.show(n=100, truncate=False)
