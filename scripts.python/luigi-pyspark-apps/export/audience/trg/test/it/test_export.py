import os
import time
import json

from pprint import pformat

import pandas as pd
import pandas.testing as pdt
import pytest

from prj.apps.export.test.it.service import e2e_data_to_it, show_pandas_df, show_spark_df
from prj.apps.export.audience.trg.app import ExportAudienceTrgTask, ExportAudienceTrgApp, TargetAudienceScorePbConfig as PbConfig

from .data import generate_test_data


class TestExportAudienceTrg(object):
    @pytest.mark.parametrize("task_conf, data_conf, idx", generate_test_data("/tmp/export/aud/trg/test_root"))
    def test_task_main(self, task_conf, data_conf, idx, session_temp_dir, hadoop_env):
        def _update_row_ts(row, ts):
            row["ts"] = ts
            return row

        def temp_path(*dirs):
            return os.path.join(session_temp_dir, *dirs)

        # set actual path
        task_conf["target_hdfs_basedir"] = temp_path("result_{}".format(idx))
        task_conf["success_hdfs_basedir"] = temp_path("success_{}".format(idx))
        task_conf["input_urls"] = [temp_path("input_{}".format(idx))]
        task_conf["output_urls"] = [temp_path("output_{}".format(idx))]
        task_conf["status_urls"] = [temp_path("status_{}".format(idx))]

        # update variables in data
        for n in [
            "expected_target_dir",
            "expected_data_files",
            "expected_success_file",
            "previous_attempt_file",
            "extra_file",
        ]:
            data_conf[n] = data_conf[n].format(**task_conf)

        # load src data to hive
        data_conf["source"]["db"] = task_conf["source_db"]
        data_conf["source"]["table"] = task_conf["source_table"]
        data = e2e_data_to_it(**data_conf["source"])
        print("\nLoading data to Hive: {}".format(pformat(data)))
        hadoop_env.add_data(data, data.keys())
        # reset session state
        # for name, path in six.iteritems(ExportAudienceTrgTask.HIVE_UDF):
        #     hadoop_env.spark.sql("DROP TEMPORARY FUNCTION IF EXISTS {}".format(name))

        # setup target dir, it should exists with some files
        hadoop_env.hdfs.mkdir(data_conf["expected_target_dir"])
        hadoop_env.hdfs.touchz(os.path.join(data_conf["expected_target_dir"], data_conf["extra_file"]))
        hadoop_env.hdfs.touchz(os.path.join(data_conf["expected_target_dir"], data_conf["previous_attempt_file"]))

        def run_task():
            # imitation of app.prepare_config
            tc = self._prepare_config(task_conf)
            print("\nTask config prepared: {}\n".format(pformat(tc)))
            print("\nTest config: {}\n".format(pformat(data_conf)))

            # execute task main
            t = ExportAudienceTrgTask(**tc)
            t.tmp_hdfs_dir = temp_path("task_root_temp_{}".format(idx))
            t.main(hadoop_env.spark.sparkContext)

            return t

        if "exception" in data_conf:
            with pytest.raises(data_conf["exception"]) as e:
                task = run_task()
            print("Exception: {}".format(pformat(e)))
            return

        task = run_task()
        task.on_success()

        if data_conf.get("expect_fatal", False):
            print("\nChecking FATAL state in output_urls ...")
            assert task.control_client.get_status(task.output_urls[0])[0]["type"] == "exception"
        else:
            data_conf["expected_data"] = [_update_row_ts(x, int(time.time())) for x in data_conf["expected_data"]]
            self._check_content(
                success_file_path=data_conf["expected_success_file"],
                target_dir=data_conf["expected_target_dir"],
                task_conf=task_conf,
                data_conf=data_conf,
                hadoop_env=hadoop_env,
            )

    @staticmethod
    def _prepare_config(config):
        class App(ExportAudienceTrgApp):
            config = {}

            def __init__(self):
                from prj.apps.utils.common.hive import FindPartitionsEngine
                self.partitions_finder = FindPartitionsEngine(raise_on_invalid_table=False)

            def info(self, msg, *args, **kwargs):
                print(msg)

        print("\nApp config:\n`{}`".format(json.dumps(config, indent=4)))
        return App().prepare_config(config)

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

        print("\nTarget dir contents: {}".format(pformat(list(hadoop_env.hdfs.listdir(target_dir)))))

        # should not write _SUCCESS in target
        assert not os.path.exists(os.path.join(target_dir, "_SUCCESS"))

        # result files from previous job attempt should be deleted // UPD: not deleted
        # assert not os.path.exists(os.path.join(target_dir, data_conf["previous_attempt_file"]))

        # should NOT delete files other than generated by this task.
        assert os.path.isfile(os.path.join(target_dir, data_conf["extra_file"]))

        # result should contain expected data
        expected_pdf = pd.DataFrame(data_conf["expected_data"], columns=data_conf["expected_columns"])
        print("\nExpected:\n{}".format(expected_pdf.to_string(index=False)))
        show_pandas_df(expected_pdf)

        path = os.path.join(target_dir, data_conf["expected_data_files"])
        print("\nLoad files: `{}`".format(path))
        # spark.sql("ADD JAR proto2-1.28.2.jar")
        # jars=...,${hadoop}/share/hadoop/tools/lib/hadoop-streaming-2.9.2.jar
        actual = (
            hadoop_env.spark.read.format(PbConfig.file_format)
            .option("proto_class_name", PbConfig.proto_class_name)
            .option("proto_header_type", PbConfig.proto_header_type)
            .option("proto_magic", PbConfig.proto_magic)
            .load(path)
            .sort(["id_number", "id_string", "score"])
        )
        _show(actual, "actual")
        actual_pdf = actual.toPandas()

        print("\nGot:\n{}".format(actual_pdf.to_string(index=False)))
        show_pandas_df(actual_pdf)

        pdt.assert_frame_equal(actual_pdf, expected_pdf, check_like=True)


def _show(df, msg="DF"):
    show_spark_df(df, msg)
