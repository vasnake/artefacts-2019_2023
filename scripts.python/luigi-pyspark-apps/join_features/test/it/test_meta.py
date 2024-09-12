# Experiments with TaskBaseMeta magic

# flake8: noqa

import os

from pprint import pformat
from datetime import datetime
from collections import defaultdict

import six
import luigi

import dill
import copy
import json

import logging

import numpy as np
import pandas as pd
import pandas.testing as pdt

import pytest

import pyspark.sql.functions as sqlfn

from pyspark.sql import SQLContext
from pyspark.sql.types import StringType, StructType, StructField

from luigi.contrib.hdfs import HdfsFlagTarget

from dmcore.utils.common import is_iterable
from prj.apps.utils.control.client.status import STATUS

from prj.apps.utils.control.client.status import (
    FatalStatusException,
    MissingDepsStatusException,
)

from prj.common.hive import HiveMetastoreClient
from prj.apps.utils.common import DT_FORMAT, METASTORE_PARTITION_SUCCESS, add_days
from prj.apps.utils.common.fs import HdfsClient
from prj.apps.utils.common.hive import format_table, select_clause, partition_filter_expr
from prj.apps.utils.common.spark import DataFrameSplitter, union_all, insert_into_hive, get_audience_strata_counts
from prj.apps.utils.common.luigix import DummyTarget, HiveExternalTask, HiveGenericTarget

from prj.apps.utils.control.luigix import (
    ControlApp,
    BasePySparkTask,
    ControlBaseMeta,
    ControlTaskMeta, ControlDynamicOutputPySparkTask, ControlDynamicOutputTaskMeta,
    CreateHiveTable,
    ControlDynamicOutputPySparkTask,
)

from prj.apps.utils.control.client.status import FatalStatusException, MissingDepsStatusException


class TestLuigiPipeline(object):
    # https://github.com/spotify/luigi/blob/master/examples/dynamic_requirements.py
    # https://joblib.readthedocs.io/en/latest/parallel.html

    def test_parallel_finish(self):
        from joblib import Parallel, delayed

        class SomeTask(luigi.Task):
            foo = luigi.Parameter()

            def on_finish(self, status_code, luigi_run_result):
                print("on_finish, task {}, status {}, run_result {}".format(self, status_code, luigi_run_result))

        def _status(task, summary):
            if task in {t for t in summary["completed"]}:
                return "SUCCESS"
            else:
                return "FAILED"

        taskA = SomeTask(foo="bar")
        taskB = SomeTask(foo="baz")
        summary = {"completed": [taskA]}
        luigi_run_result = {}
        task_graph = [
            [taskA, taskB],
            [taskB, taskA]
        ]

        # Can't reproduce pickling error
        with Parallel(n_jobs=2, prefer="processes", verbose=100) as parallel:
            while task_graph:
                tasks = task_graph.pop()

                parallel(
                    delayed(lambda a, b, c: a.on_finish(b, c))(task, _status(task, summary), luigi_run_result)
                    for task in tasks
                )

    def test_meta_inject(self, session_temp_dir):
        tmp_dir = session_temp_dir
        i = 1
        spec = {"expected_statuses": [STATUS.SUCCESS]}

        configs = [{
            "force": True,
            "storage_path": os.path.join(tmp_dir, "storage.txt"),
            "stage1_success_flag_path": os.path.join(tmp_dir, "stage1.success.txt"),
            "stage2_success_flag_path": os.path.join(tmp_dir, "stage2.success"),
            "stages12_success_flag_path": os.path.join(tmp_dir, "stages12.success.txt"),
            "ctid": "ctid_{}".format(i),
            "status_urls": [
                os.path.join(tmp_dir, "status__{}{}__v{}".format(s, j, i))
                for j, s in enumerate(spec["expected_statuses"])
            ],
            "output_urls": [
                os.path.join(tmp_dir, "output__{}{}__v{}".format(s, j, i))
                for j, s in enumerate(spec["expected_statuses"])
            ],
            "input_urls": [
                os.path.join(tmp_dir, "input__{}{}__v{}".format(s, j, i))
                for j, s in enumerate(spec["expected_statuses"])
            ],
        }]

        app = App(configs=configs)

        success = luigi.interface.build([app], local_scheduler=True, workers=2)
        assert success is True


class AppTask(six.with_metaclass(ControlDynamicOutputTaskMeta, luigi.Task)):
    storage_path = luigi.Parameter(description="Path to storage")  # type: str
    stage1_success_flag_path = luigi.Parameter(description="Path to success file")  # type: str
    stage2_success_flag_path = luigi.Parameter(description="Path to success file")  # type: str
    stages12_success_flag_path = luigi.Parameter(description="Path to success file")  # type: str

    def __init__(self, *args, **kwargs):
        super(AppTask, self).__init__(*args, **kwargs)
        self.log("init, args {}, kwargs {}".format(pformat(args), pformat(kwargs)))

    def _output(self):
        res = luigi.LocalTarget(path=self.stages12_success_flag_path)
        self.debug("output: {}".format(res.path))
        return res

    def output(self):
        res = self._output()
        self.log("output {}".format(pformat(res)))
        return res

    def requires(self):
        self.log("requires ...")
        yield CreateStorageTask(path=self.storage_path)
        yield WorkHubTask(
            stage1_success_flag_path=self.stage1_success_flag_path,
            stage2_success_flag_path=self.stage2_success_flag_path,
            success_flag_path=self.stages12_success_flag_path,
        )

    def run(self):
        super(AppTask, self).run()
        self.log("run ...")
        with self._output().open("w") as f:
            f.write("success")

    def main(self, sc, *args):
        self.log("main, args {}".format(pformat(args)))

    def log(self, message):
        print("AppTask {}, {}".format(self.task_id, message))

    def info(self, message):
        self.log("[INFO] {}".format(message))

    def debug(self, message):
        self.log("[DEBUG] {}".format(message))


class WorkHubTask(six.with_metaclass(ControlBaseMeta, luigi.Task)):
    stage1_success_flag_path = luigi.Parameter(description="Path to file")  # type: str
    stage2_success_flag_path = luigi.Parameter(description="Path to file")  # type: str
    success_flag_path = luigi.Parameter(description="Path to file")  # type: str

    def __init__(self, *args, **kwargs):
        super(WorkHubTask, self).__init__(*args, **kwargs)
        self.log("init, args {}, kwargs {}".format(pformat(args), pformat(kwargs)))

    def _output(self):
        res = luigi.LocalTarget(path=self.success_flag_path)
        self.debug("output: {}".format(res.path))
        return res

    def output(self):
        res = self._output()
        self.log("output {}".format(pformat(res)))
        return res

    def on_success(self):
        self.log("on_success")
        with self._output().open("w") as f:
            f.write("success")
        return super(WorkHubTask, self).on_success()

    def requires(self):
        res = Stage1Task(success_flag_path=self.stage1_success_flag_path)
        self.log("requires {}".format(res))
        yield res

    def run(self):
        self.log("run 1 ...")  # two times

        yield [
            Stage2Task(
                stage1_success_flag_path=self.stage1_success_flag_path,
                success_flag_path="{}.{}.txt".format(self.stage2_success_flag_path, i)
            )
            for i in range(1, 3)
        ]

        self.log("run 2 ...")  # one time
        super(WorkHubTask, self).run()

    def log(self, message):
        print("WorkHubTask {}, {}".format(self.task_id, message))

    def info(self, message):
        self.log("[INFO] {}".format(message))

    def debug(self, message):
        self.log("[DEBUG] {}".format(message))


class Stage1Task(six.with_metaclass(ControlBaseMeta, luigi.Task)):
    success_flag_path = luigi.Parameter(description="Path to file")  # type: str

    def __init__(self, *args, **kwargs):
        super(Stage1Task, self).__init__(*args, **kwargs)
        self.log("init, args {}, kwargs {}".format(pformat(args), pformat(kwargs)))

    def _output(self):
        res = luigi.LocalTarget(path=self.success_flag_path)
        self.debug("output: {}".format(res.path))
        return res

    def output(self):
        res = self._output()
        self.log("output {}".format(pformat(res)))
        return res

    def on_success(self):
        self.log("on_success")
        with self._output().open("w") as f:
            f.write("success")
        return super(Stage1Task, self).on_success()

    def run(self):
        super(Stage1Task, self).run()
        self.log("run ...")

    def main(self, sc, *args):
        self.log("main, args {}".format(pformat(args)))

    def log(self, message):
        print("Stage1Task {}, {}".format(self.task_id, message))

    def info(self, message):
        self.log("[INFO] {}".format(message))

    def debug(self, message):
        self.log("[DEBUG] {}".format(message))


class Stage2Task(six.with_metaclass(ControlBaseMeta, luigi.Task)):
    stage1_success_flag_path = luigi.Parameter(description="Path to file")  # type: str
    success_flag_path = luigi.Parameter(description="Path to file")  # type: str

    def __init__(self, *args, **kwargs):
        super(Stage2Task, self).__init__(*args, **kwargs)
        self.log("init, args {}, kwargs {}".format(pformat(args), pformat(kwargs)))

    def _output(self):
        res = luigi.LocalTarget(path=self.success_flag_path)
        self.debug("output: {}".format(res.path))
        return res

    def output(self):
        res = self._output()
        self.log("output {}".format(pformat(res)))
        return res

    def requires(self):
        res = Stage1Task(success_flag_path=self.stage1_success_flag_path)
        self.log("requires {}".format(res))
        return res

    def run(self):
        super(Stage2Task, self).run()
        self.log("run ...")
        with self._output().open("w") as f:
            f.write("success")

    def log(self, message):
        print("Stage2Task {}, {}".format(self.task_id, message))

    def info(self, message):
        self.log("[INFO] {}".format(message))

    def debug(self, message):
        self.log("[DEBUG] {}".format(message))


class CreateStorageTask(six.with_metaclass(ControlBaseMeta, luigi.Task)):
    path = luigi.Parameter(description="Path to storage")  # type: str

    def __init__(self, *args, **kwargs):
        super(CreateStorageTask, self).__init__(*args, **kwargs)
        self.log("init, args {}, kwargs {}".format(pformat(args), pformat(kwargs)))

    def _output(self):
        res = luigi.LocalTarget(path=self.path)
        self.debug("output: {}".format(res.path))
        return res

    def output(self):
        res = self._output()
        self.log("output {}".format(pformat(res)))
        return res

    def run(self):
        super(CreateStorageTask, self).run()
        self.log("run ...")
        with self._output().open("w") as f:
            f.write("success")

    def log(self, message):
        print("CreateStorageTask {}, {}".format(self.task_id, message))

    def info(self, message):
        self.log("[INFO] {}".format(message))

    def debug(self, message):
        self.log("[DEBUG] {}".format(message))


class App(ControlApp):
    task_class = AppTask
    control_xapp = "LocalApp"

    def prepare_config(self, config):
        self.log("prepare_config {}".format(pformat(config)))
        return config

    def log(self, message):
        print("App {}, {}".format(self.task_id, message))

    def info(self, message):
        self.log("[INFO] {}".format(message))

    def debug(self, message):
        self.log("[DEBUG] {}".format(message))
