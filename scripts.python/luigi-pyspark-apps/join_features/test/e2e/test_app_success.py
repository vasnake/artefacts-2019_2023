# run success tests

import os

import pandas as pd
import pandas.testing as pdt

from prj.apps.utils.common.fs import HdfsClient
from prj.apps.joiner.features.app import JoinerFeaturesApp
from prj.apps.utils.testing.tools import HiveDataIO, ControlAppTest

from .data_success import (
    MD_DATA,
    TEST_DB,
    MD_TABLE,
    MD_SCHEMA,
    SOURCE_DATA,
    SOURCE_TABLE,
    TMP_HDFS_DIR,
    MATCHING_DATA,
    SOURCE_SCHEMA,
    MATCHING_TABLE,
    MATCHING_SCHEMA,
    MD_PARTITION_COLUMNS,
    SOURCE_PARTITION_COLUMNS,
    MATCHING_PARTITION_COLUMNS,
    generate_test_data,
)


class TestJoinerFeaturesSuccess(ControlAppTest):
    app_class = JoinerFeaturesApp
    test_data = generate_test_data()

    hdfs = HdfsClient()

    @classmethod
    def setup_cls(cls):
        cls.hdfs.mkdir(TMP_HDFS_DIR, remove_if_exists=True)

        for table, schema, partition_columns, data in [
            (SOURCE_TABLE, SOURCE_SCHEMA, SOURCE_PARTITION_COLUMNS, SOURCE_DATA),
            (MD_TABLE, MD_SCHEMA, MD_PARTITION_COLUMNS, MD_DATA),
            (MATCHING_TABLE, MATCHING_SCHEMA, MATCHING_PARTITION_COLUMNS, MATCHING_DATA),
        ]:
            HiveDataIO(database=TEST_DB, table=table).drop_table().set_schema(
                schema, partition_columns
            ).create_table().insert_data(data, os.path.join(TMP_HDFS_DIR, table))

        for target_table in set(task_conf["target_table"] for task_conf, _ in cls.test_data):
            HiveDataIO(database=TEST_DB, table=target_table).drop_table()

    @classmethod
    def teardown_cls(cls):
        for table in [SOURCE_TABLE, MD_TABLE, MATCHING_TABLE]:
            HiveDataIO(database=TEST_DB, table=table).drop_table()

        for target_table in set(task_conf["target_table"] for task_conf, _ in cls.test_data):
            HiveDataIO(database=TEST_DB, table=target_table).drop_table()

        cls.hdfs.remove(TMP_HDFS_DIR, recursive=True, skip_trash=True, force=True)

    def check_output(self, task_config, expected_data):
        for url, expected_status in zip(task_config["status_urls"], expected_data["statuses"]):
            got_status = self.app_class.task_class.control_client.get_status(url)
            assert got_status == expected_status

        columns = expected_data["columns"]

        expected_pdf = (
            pd.DataFrame(expected_data["rows"])[columns].sort_values(by=["uid", "uid_type"]).reset_index(drop=True)
        )

        got_pdf = pd.DataFrame(
            HiveDataIO(TEST_DB, task_config["target_table"]).collect_data(
                partitions=[{"dt": task_config["target_dt"]}],
                tmp_hdfs_dir=os.path.join(TMP_HDFS_DIR, task_config["ctid"], "hdio"),
                sort_by=["uid", "uid_type"],
            )
        )[columns]

        self.info("Got:\n{}".format(got_pdf.to_string(index=False)))
        self.info("Expected:\n{}".format(expected_pdf.to_string(index=False)))
        pdt.assert_frame_equal(got_pdf, expected_pdf, check_dtype=False)
