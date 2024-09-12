# flake8: noqa

import os
import dill
import copy
import json
import luigi

from pprint import pformat

import six
import numpy as np
import pandas as pd
import pytest
import pandas.testing as pdt

from prj.apps.utils.control.client.status import (
    FatalStatusException,
    MissingDepsStatusException,
)

from pyspark.sql.utils import (AnalysisException, CapturedException,)

from prj.apps.utils.testing.tools import HiveDataIO

SCHEMA = (
    ("uid", "int"),
    ("some_array", "array<float>"),
    ("dt", "string"),
    ("uid_type", "string"),
)
PARTITION_COLUMNS = ["dt", "uid_type"]
DATA = [
    {"uid": "10013", "some_array": ["", ""], "dt": "2022-06-06", "uid_type": "VKID"},
]


class TestHiveIO(object):
    def test_arrays(self, session_temp_dir):
        import tempfile

        self._last_status = "running"
        fd, self._status_file_path = tempfile.mkstemp()
        os.write(fd, self._last_status)
        os.close(fd)

        with open(self._status_file_path, "wb") as f:
            f.write("success")

        with open(self._status_file_path, "rb") as f:
            self._last_status = f.read()

        assert self._last_status == "success"
        os.unlink(self._status_file_path)
        assert 0, self._status_file_path

        # HiveDataIO(database="test_db", table="test_table").drop_table().set_schema(
        #     SCHEMA, PARTITION_COLUMNS
        # ).create_table().insert_data(DATA, os.path.join(session_temp_dir, "test_hiveio_array"))

        temp_dir = os.path.join(session_temp_dir, "test_hiveio_array")
        from conftest import HdfsClient
        HdfsClient().mkdir(temp_dir)

        try:
            HiveDataIO(database="test_db", table="test_table").set_schema(
                SCHEMA, PARTITION_COLUMNS
            ).insert_data(DATA, temp_dir)
        except Exception as ex:
            print("Error: `{}`".format(ex))

        with HdfsClient().open(os.path.join(temp_dir, "data"), "r") as f:
            for line in f.readlines():
                print("hive table source line: `{}`".format(line))
