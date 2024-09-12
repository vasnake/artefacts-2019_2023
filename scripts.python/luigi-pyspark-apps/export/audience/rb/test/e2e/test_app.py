import os

from pprint import pformat

from prj.apps.utils.testing.tools import HiveDataIO, ControlAppTest
from prj.apps.utils.common.fs.hdfs import SUCCESS_FILENAME, HdfsClient
from prj.apps.utils.testing.defines import TEST_DB
from prj.apps.export.audience.rb.app import ExportAudienceRbApp

from .data import (
    SOURCE_DATA,
    SOURCE_TABLE,
    TMP_HDFS_DIR,
    SOURCE_TABLE_SCHEMA,
    SOURCE_PARTITION_COLUMNS,
    generate_test_data,
)


class TestExportAudienceRb(ControlAppTest):
    app_class = ExportAudienceRbApp
    test_data = generate_test_data()
    hdfs = HdfsClient()

    @classmethod
    def setup_cls(cls):
        cls.hdfs.mkdir(TMP_HDFS_DIR, remove_if_exists=True)

        HiveDataIO(database=TEST_DB, table=SOURCE_TABLE).drop_table().set_schema(
            SOURCE_TABLE_SCHEMA, SOURCE_PARTITION_COLUMNS
        ).create_table().insert_data(SOURCE_DATA, os.path.join(TMP_HDFS_DIR, SOURCE_TABLE))

    @classmethod
    def teardown_cls(cls):
        HiveDataIO(database=TEST_DB, table=SOURCE_TABLE).drop_table()
        cls.hdfs.remove(TMP_HDFS_DIR, recursive=True, skip_trash=True, force=True)

    def check_output(self, task_config, expected_data):
        for url, expected_status in zip(task_config["status_urls"], expected_data["expected_statuses"]):
            got_status = self.app_class.task_class.control_client.get_status(url)
            assert got_status == expected_status

        target_dir = task_config["target_hdfs_basedir"]
        target_files_mask = os.path.join(target_dir, expected_data["expected_data_files"].format(**task_config))

        # Should not write success-file in a target directory
        assert not self.hdfs.exists(os.path.join(target_dir, SUCCESS_FILENAME))

        has_success_file = self.hdfs.isfile(expected_data["expected_success_file"].format(**task_config))

        expected_rows = sorted(expected_data["expected_data"])

        if len(expected_rows) == 0:
            assert not self.hdfs.exists(target_files_mask)
            assert not has_success_file
            return

        # Should write separate success-file
        assert has_success_file

        self.debug("Target directory contents:\n{}".format(pformat(list(self.hdfs.listdir(target_dir)))))

        got_rows = []

        for path in self.hdfs.listdir(target_files_mask):
            self.debug("Reading a data file: `{}`".format(path))
            lines = []

            with self.hdfs.open(path) as fd:
                for line in fd:
                    lines.append(line.strip())

            if not lines:
                self.debug("Empty file")
            else:
                self.debug("File content:\n{}".format(pformat(lines)))
                assert len(lines) <= task_config["output_max_rows_per_file"]
                got_rows += lines

        got_rows = sorted(got_rows)

        self.info("Got:\n{}".format(pformat(got_rows)))
        self.info("Expected:\n{}".format(pformat(expected_rows)))
        assert expected_rows == got_rows
