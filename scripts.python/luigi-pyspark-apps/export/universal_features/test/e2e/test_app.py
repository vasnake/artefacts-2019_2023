import os

from pprint import pformat

from prj.apps.utils.common.fs import HdfsClient
from prj.apps.utils.testing.tools import HiveDataIO, ControlAppTest
from prj.apps.export.universal_features.app import ExportUniversalFeaturesApp

from .data import TMP_HDFS_DIR, generate_test_data


class TestExportUniversalFeatures(ControlAppTest):
    app_class = ExportUniversalFeaturesApp
    test_data = generate_test_data()
    hdfs = HdfsClient()

    @classmethod
    def setup_cls(cls):
        cls.hdfs.mkdir(TMP_HDFS_DIR, remove_if_exists=True)

        for task_config, spec in cls.test_data:
            src = spec["source"]
            HiveDataIO(database=task_config["source_db"], table=task_config["source_table"]).drop_table().set_schema(
                src["schema"], src["partition_columns"]
            ).create_table().insert_data(src["data"], os.path.join(TMP_HDFS_DIR, task_config["source_table"]))

    @classmethod
    def teardown_cls(cls):
        for task_config, _ in cls.test_data:
            HiveDataIO(database=task_config["source_db"], table=task_config["source_table"]).drop_table()
        cls.hdfs.remove(TMP_HDFS_DIR, recursive=True, skip_trash=True, force=True)

    @staticmethod
    def target_hdfs_dir(task_config):
        return os.path.join(
            task_config["target_hdfs_basedir"], task_config["features_subdir"], task_config["target_dt"], ""
        )

    @classmethod
    def setup_forced_run(cls, task_config):
        target_hdfs_dir = cls.target_hdfs_dir(task_config)
        cls.hdfs.mkdir(target_hdfs_dir, remove_if_exists=False)

        for path in cls.hdfs.listdir(target_hdfs_dir):
            if not cls.app_class.task_class.is_skipped(os.path.basename(path)):
                cls.hdfs.touchz("{}.success".format(path))

    def check_output(self, task_config, expected_data):
        max_rows = task_config["output_max_rows_per_file"]
        expected_header = expected_data["expected_data"][0]
        expected_rows = expected_data["expected_data"][1:]

        got_rows = []
        files = list(self.hdfs.listdir(self.target_hdfs_dir(task_config)))
        self.info("Target directory, files:\n{}".format(pformat(files)))

        for path in files:
            if path.endswith(".csv"):
                lines = []

                with self.hdfs.open(path) as fd:
                    for line in fd:
                        lines.append(line.strip())

                self.debug("File `{}` content:\n{}".format(path, pformat(lines)))

                if lines:
                    assert len(lines) <= max_rows + 1 + 1  # +1 for header line, +1 for repartition margins
                    assert lines[0] == expected_header
                    got_rows += lines[1:]

            else:
                self.debug("Not a data file: `{}`".format(path))

        self.info("Got:\n{}".format(pformat(got_rows)))
        self.info("Expected:\n{}".format(pformat(expected_rows)))
        assert sorted(got_rows) == sorted(expected_rows)
