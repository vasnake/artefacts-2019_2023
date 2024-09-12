# Experiments with missing partitions processing

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

from prj.apps.utils.common.luigix import HiveExternalTask, HiveGenericTarget, target
from prj.apps.utils.control.client.status import FatalStatusException, MissingDepsStatusException


class TestLuigiDAG(object):

    def test_missing_partitions(self, session_temp_dir):
        tmp_dir = session_temp_dir
        i = 1
        spec = {"expected_statuses": [STATUS.SUCCESS]}

        configs = [{
            "force": False,
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


class AppTask(six.with_metaclass(ControlTaskMeta, luigi.WrapperTask)):

    cfg = luigi.DictParameter()  # type: dict

    def __init__(self, *args, **kwargs):
        super(AppTask, self).__init__(*args, **kwargs)
        self.log("init, args {}, kwargs {}".format(pformat(args), pformat(kwargs)))

    def requires(self):
        self.log("requires ...")
        return [
            SubTask(cfg=self.cfg)
        ]

    def log(self, message):
        print("AppTask {}, {}".format(self.task_id, message))

    def info(self, message):
        self.log("[INFO] {}".format(message))

    def debug(self, message):
        self.log("[DEBUG] {}".format(message))


class SubTask(BasePySparkTask):

    cfg = luigi.DictParameter()  # type: dict

    def __init__(self, *args, **kwargs):
        super(SubTask, self).__init__(*args, **kwargs)
        self.log("init, args {}, kwargs {}".format(pformat(args), pformat(kwargs)))

    def requires(self):
        yield HiveExternalTask(
            database="test_db",
            table="test_table",
            partitions=self.cfg["partitions"],
        )

    def log(self, message):
        print("SubTask {}, {}".format(self.task_id, message))

    def info(self, message):
        self.log("[INFO] {}".format(message))

    def debug(self, message):
        self.log("[DEBUG] {}".format(message))


class App(ControlApp):
    task_class = AppTask
    control_xapp = "LocalApp"

    def prepare_config(self, config):
        self.log("prepare_config {}".format(pformat(config)))
        config["cfg"] = {"partitions": target.MISSING_LUIGI_TARGET}
        return config

    def log(self, message):
        print("App {}, {}".format(self.task_id, message))

    def info(self, message):
        self.log("[INFO] {}".format(message))

    def debug(self, message):
        self.log("[DEBUG] {}".format(message))
