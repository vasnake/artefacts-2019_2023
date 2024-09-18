import os

import pandas as pd
import pandas.testing as pdt

from prj.apps.utils.common.fs import HdfsClient
from prj.apps.utils.testing.tools import HiveDataIO, ControlAppTest
from prj.apps.combine.universal_features.app import CombineUniversalFeaturesApp

from .data import (
    TEST_DB,
    FILTER_DATA,
    SOURCE_DATA,
    FILTER_TABLE,
    SOURCE_TABLE,
    TARGET_TABLE,
    TMP_HDFS_DIR,
    FILTER_SCHEMA,
    MATCHING_DATA,
    SOURCE_SCHEMA,
    TARGET_SCHEMA,
    MATCHING_TABLE,
    MATCHING_SCHEMA,
    FILTER_PARTITION_COLUMNS,
    SOURCE_PARTITION_COLUMNS,
    TARGET_PARTITION_COLUMNS,
    MATCHING_PARTITION_COLUMNS,
    generate_test_data,
)


class TestCombineUniversalFeatures(ControlAppTest):
    app_class = CombineUniversalFeaturesApp
    test_data = generate_test_data()
    hdfs = HdfsClient()

    @classmethod
    def setup_cls(cls):
        cls.hdfs.mkdir(TMP_HDFS_DIR, remove_if_exists=True)

        HiveDataIO(database=TEST_DB, table=SOURCE_TABLE).drop_table().set_schema(
            SOURCE_SCHEMA, SOURCE_PARTITION_COLUMNS
        ).create_table().insert_data(SOURCE_DATA, os.path.join(TMP_HDFS_DIR, SOURCE_TABLE))

        HiveDataIO(database=TEST_DB, table=FILTER_TABLE).drop_table().set_schema(
            FILTER_SCHEMA, FILTER_PARTITION_COLUMNS
        ).create_table().insert_data(FILTER_DATA, os.path.join(TMP_HDFS_DIR, FILTER_TABLE))

        HiveDataIO(database=TEST_DB, table=MATCHING_TABLE).drop_table().set_schema(
            MATCHING_SCHEMA, MATCHING_PARTITION_COLUMNS
        ).create_table().insert_data(MATCHING_DATA, os.path.join(TMP_HDFS_DIR, MATCHING_TABLE))

        HiveDataIO(database=TEST_DB, table=TARGET_TABLE).drop_table().set_schema(
            TARGET_SCHEMA, TARGET_PARTITION_COLUMNS
        ).create_table()

    @classmethod
    def teardown_cls(cls):
        HiveDataIO(database=TEST_DB, table=SOURCE_TABLE).drop_table()
        HiveDataIO(database=TEST_DB, table=FILTER_TABLE).drop_table()
        HiveDataIO(database=TEST_DB, table=MATCHING_TABLE).drop_table()
        HiveDataIO(database=TEST_DB, table=TARGET_TABLE).drop_table()
        cls.hdfs.remove(TMP_HDFS_DIR, recursive=True, skip_trash=True, force=True)

    def check_output(self, task_config, expected_data):
        columns = expected_data["columns"]

        expected_pdf = (
            pd.DataFrame(expected_data["rows"])[columns].sort_values(by=["uid", "uid_type"]).reset_index(drop=True)
        )

        got_pdf = pd.DataFrame(
            HiveDataIO(TEST_DB, TARGET_TABLE).collect_data(
                partitions=[{"feature_name": task_config["feature_name"]}],
                tmp_hdfs_dir=os.path.join(TMP_HDFS_DIR, task_config["ctid"], "hdio"),
                sort_by=["uid", "uid_type"],
            )
        )[columns]

        self.info("Got:\n{}".format(got_pdf.to_string(index=False)))
        self.info("Expected:\n{}".format(expected_pdf.to_string(index=False)))
        pdt.assert_frame_equal(got_pdf, expected_pdf, check_dtype=False)
