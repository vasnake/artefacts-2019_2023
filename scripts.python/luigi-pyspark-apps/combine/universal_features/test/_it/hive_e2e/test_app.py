import os

import numpy as np
import pandas as pd

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
    SOURCE_TABLE_SCHEMA,
    TARGET_SCHEMA,
    FILTER_PARTITION_COLUMNS,
    SOURCE_PARTITION_COLUMNS,
    TARGET_PARTITION_COLUMNS,
    generate_test_data,
)


class TostCombineUniversalFeatures(ControlAppTest):
    app_class = CombineUniversalFeaturesApp
    test_data = generate_test_data()
    hdfs = HdfsClient()

    @classmethod
    def setup_cls(cls):
        cls.hdfs.mkdir(TMP_HDFS_DIR, remove_if_exists=True)

        HiveDataIO(database=TEST_DB, table=SOURCE_TABLE).drop_table().set_schema(
            SOURCE_TABLE_SCHEMA, SOURCE_PARTITION_COLUMNS
        ).create_table().insert_data(SOURCE_DATA, os.path.join(TMP_HDFS_DIR, SOURCE_TABLE))

        HiveDataIO(database=TEST_DB, table=FILTER_TABLE).drop_table().set_schema(
            FILTER_SCHEMA, FILTER_PARTITION_COLUMNS
        ).create_table().insert_data(FILTER_DATA, os.path.join(TMP_HDFS_DIR, FILTER_TABLE))

        HiveDataIO(database=TEST_DB, table=TARGET_TABLE).drop_table().set_schema(
            TARGET_SCHEMA, TARGET_PARTITION_COLUMNS
        ).create_table()

    @classmethod
    def teardown_cls(cls):
        HiveDataIO(database=TEST_DB, table=SOURCE_TABLE).drop_table()
        HiveDataIO(database=TEST_DB, table=FILTER_TABLE).drop_table()
        HiveDataIO(database=TEST_DB, table=TARGET_TABLE).drop_table()
        cls.hdfs.remove(TMP_HDFS_DIR, recursive=True, skip_trash=True, force=True)

    def check_output(self, task_config, spec):
        columns = ["uid", "score", "score_list", "score_map", "cat_list", "feature_name", "dt", "uid_type"]
        expected_data = [
            {name: row.get(name, np.nan if name in {"score", "score_list", "score_map"} else "") for name in columns}
            for row in spec["expected_data"]
        ]
        expected_pdf = pd.DataFrame(expected_data)[columns].sort_values(by=["uid", "uid_type"])

        got_pdf = pd.DataFrame(
            HiveDataIO(TEST_DB, TARGET_TABLE).collect_data(
                partitions=[{"feature_name": task_config["feature_name"]}],
                tmp_hdfs_dir=os.path.join(TMP_HDFS_DIR, task_config["ctid"], "hdio_read"),
                sort_by=["uid", "uid_type"],
            )
        )[columns]

        print("Got:\n{}".format(got_pdf.to_string(index=False)))
        print("Expected:\n{}".format(expected_pdf.to_string(index=False)))
        pdt.assert_frame_equal(got_pdf, expected_pdf.reset_index(drop=True), check_dtype=False)
