# run failure cases

import os

from prj.apps.utils.common.fs import HdfsClient
from prj.apps.joiner.features.app import JoinerFeaturesApp
from prj.apps.utils.common.luigix import HiveGenericTarget
from prj.apps.utils.testing.tools import HiveDataIO, ControlAppTest

from .data_fail import (
    TEST_DB,
    SOURCE_DATA,
    SOURCE_TABLE,
    TMP_HDFS_DIR,
    SOURCE_SCHEMA,
    SOURCE_PARTITION_COLUMNS,
    generate_test_data,
)


class TestJoinerFeaturesFail(ControlAppTest):
    app_class = JoinerFeaturesApp
    test_data = generate_test_data()
    test_luigi_success = False

    hdfs = HdfsClient()

    @classmethod
    def setup_cls(cls):
        cls.hdfs.mkdir(TMP_HDFS_DIR, remove_if_exists=True)

        HiveDataIO(database=TEST_DB, table=SOURCE_TABLE).drop_table().set_schema(
            SOURCE_SCHEMA, SOURCE_PARTITION_COLUMNS
        ).create_table().insert_data(SOURCE_DATA, os.path.join(TMP_HDFS_DIR, SOURCE_TABLE))

        for target_table in set(task_conf["target_table"] for task_conf, _ in cls.test_data):
            HiveDataIO(database=TEST_DB, table=target_table).drop_table()

    @classmethod
    def teardown_cls(cls):
        HiveDataIO(database=TEST_DB, table=SOURCE_TABLE).drop_table()

        for target_table in set(task_conf["target_table"] for task_conf, _ in cls.test_data):
            HiveDataIO(database=TEST_DB, table=target_table).drop_table()

        cls.hdfs.remove(TMP_HDFS_DIR, recursive=True, skip_trash=True, force=True)

    def check_output(self, task_config, expected_data):
        for url, expected_status in zip(task_config["status_urls"], expected_data["statuses"]):
            got_status = self.app_class.task_class.control_client.get_status(url)
            assert got_status == expected_status

        assert not HiveGenericTarget(database=TEST_DB, table=task_config["target_table"]).exists()
