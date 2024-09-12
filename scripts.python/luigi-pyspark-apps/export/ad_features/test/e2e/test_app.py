import os

from pprint import pformat

from prj.apps.utils.common.fs import HdfsClient
from prj.apps.utils.testing.tools import HiveDataIO, ControlAppTest
from prj.apps.utils.testing.defines import TEST_DB
from prj.apps.export.ad_features.app import ExportAdFeaturesApp

from .data import SOURCE_DATA, SOURCE_TABLE, TMP_HDFS_DIR, SOURCE_SCHEMA, SOURCE_PARTITION_COLUMNS, generate_test_data


class TestExportAdFeatures(ControlAppTest):
    app_class = ExportAdFeaturesApp
    test_data = generate_test_data()
    hdfs = HdfsClient()

    @classmethod
    def setup_cls(cls):
        cls.hdfs.mkdir(TMP_HDFS_DIR, remove_if_exists=True)

        HiveDataIO(database=TEST_DB, table=SOURCE_TABLE).drop_table().set_schema(
            SOURCE_SCHEMA, SOURCE_PARTITION_COLUMNS
        ).create_table().insert_data(SOURCE_DATA, os.path.join(TMP_HDFS_DIR, SOURCE_TABLE))

    @classmethod
    def teardown_cls(cls):
        HiveDataIO(database=TEST_DB, table=SOURCE_TABLE).drop_table()
        cls.hdfs.remove(TMP_HDFS_DIR, recursive=True, skip_trash=True, force=True)

    def check_output(self, task_config, expected_data):
        for url, expected_status in zip(task_config["status_urls"], expected_data["expected_statuses"]):
            got_status = self.app_class.task_class.control_client.get_status(url)
            assert got_status == expected_status

        if not expected_data.get("expected_data"):
            self.info("Expected task failure")
            return

        expected_header = expected_data["expected_data"][0]
        expected_rows = sorted(expected_data["expected_data"][1:])

        got_files = list(self.hdfs.listdir(expected_data["expected_target_dir"].format(**task_config)))
        self.info("Target directory, got files:\n{}".format(pformat(got_files)))

        got_rows = []
        max_rows_per_file = task_config["output_max_rows_per_file"]

        for path in got_files:
            if path.endswith(".csv"):
                with self.hdfs.open(path) as fd:
                    lines = [line.strip() for line in fd]

                self.debug("File `{}` content:\n{}".format(path, pformat(lines)))
                assert 1 < len(lines) <= max_rows_per_file + 2  # +1 for header line, +1 for repartition margins
                assert lines[0] == expected_header
                got_rows += lines[1:]

        self.info("Got:\n{}".format(pformat(got_rows)))
        self.info("Expected:\n{}".format(pformat(expected_rows)))
        assert sorted(got_rows) == sorted(expected_rows)
