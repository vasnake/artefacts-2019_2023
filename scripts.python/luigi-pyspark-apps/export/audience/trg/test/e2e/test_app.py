import os

from pprint import pformat

import luigi
import pandas as pd
import pandas.testing as pdt

from pyspark.sql import SparkSession
from pyspark.conf import SparkConf

from prj.apps.utils.testing.tools import HiveDataIO, ControlAppTest
from prj.apps.utils.common.fs.hdfs import SUCCESS_FILENAME, HdfsClient
from prj.apps.utils.testing.defines import TEST_DB
from prj.apps.export.audience.trg.app import ExportAudienceTrgApp, TargetAudienceScorePbConfig

from .data import (
    SOURCE_DATA,
    SOURCE_TABLE,
    TMP_HDFS_DIR,
    SOURCE_TABLE_SCHEMA,
    SOURCE_PARTITION_COLUMNS,
    generate_test_data,
)


class TestExportAudienceTrg(ControlAppTest):
    app_class = ExportAudienceTrgApp
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

        target_dir = expected_data["expected_target_dir"].format(**task_config)
        target_files_mask = os.path.join(target_dir, expected_data["expected_data_files"].format(**task_config))

        # Should not write success-file in a target directory
        assert not self.hdfs.exists(os.path.join(target_dir, SUCCESS_FILENAME))

        has_success_file = self.hdfs.isfile(expected_data["expected_success_file"].format(**task_config))

        expected_columns = expected_data["expected_columns"]
        expected_pdf = pd.DataFrame(expected_data["expected_data"], columns=expected_columns)

        if expected_pdf.empty:
            assert not self.hdfs.exists(target_files_mask)
            assert not has_success_file
            return

        # Should write separate success-file
        assert has_success_file

        self.debug("Target directory contents:\n{}".format(pformat(list(self.hdfs.listdir(target_dir)))))
        self.debug("Loading files: `{}` ...".format(target_files_mask))

        with SparkSession.builder.config(
            conf=SparkConf().setAll(
                [
                    ("spark.master", "local[1]"),
                    ("spark.driver.host", "localhost"),
                    ("spark.ui.enabled", "false"),
                    ("spark.default.parallelism", 1),
                    ("spark.sql.shuffle.partitions", 1),
                    ("spark.jars", luigi.configuration.get_config().get("spark", "jars", "")),
                ]
            )
        ).getOrCreate() as spark:
            got_pdf = (
                spark.read.format(TargetAudienceScorePbConfig.file_format)
                .option("proto_class_name", TargetAudienceScorePbConfig.proto_class_name)
                .option("proto_header_type", TargetAudienceScorePbConfig.proto_header_type)
                .option("proto_magic", TargetAudienceScorePbConfig.proto_magic)
                .load(target_files_mask)
                .sort(["id_number", "id_string"])
                .toPandas()
            )[expected_columns]

        self.info("Got:\n{}".format(pformat(got_pdf)))
        self.info("Expected:\n{}".format(pformat(expected_pdf)))
        pdt.assert_frame_equal(got_pdf, expected_pdf)
