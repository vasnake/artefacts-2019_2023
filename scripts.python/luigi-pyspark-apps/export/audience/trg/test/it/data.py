import os

from pyspark.sql.types import MapType, ArrayType, FloatType, DoubleType, StringType

from prj.apps.utils.control.client.status import FatalStatusException, MissingDepsStatusException

from prj.apps.utils.testing.defines import TEST_DB

NAME = "test_AUD_TRG_app"
TARGET_DT = "2020-06-01"

# flake8: noqa
# @formatter:off
# fmt: off


def generate_test_data(tmp_hdfs_dir):
    def temp_path(*dirs):
        return os.path.join(tmp_hdfs_dir, *dirs)

    source_schema = (
        ("uid", "string"),
        ("score", "double"),
        ("audience_name", "string"),
        ("category", "string"),
        ("dt", "string"),
        ("uid_type", "string"),
    )
    source_partition_columns = ["audience_name", "category", "dt", "uid_type"]
    expected_columns = ["id_type", "id_number", "id_string", "audience_id", "score", "ts"]

    expected_success_file = "{success_hdfs_basedir}/{target_dt}/ExportAudienceTrgTask_{audience_id}-{uid_type}-{target_dt}.success"
    #  actual file name example: /export/target/custom_audience/hid/
    #  `audience_score.20210702144712_1086915.TARGET_141912-simple-2021-07-02-HID.00001.pb.gz`
    expected_data_files = "audience_score.*-{ctid}-TRG-{audience_id}-{uid_type}-{target_dt}-*.pb.gz"
    previous_attempt_file = "audience_score.20210625090719-TRG-{audience_id}-{uid_type}-{target_dt}-66666.pb.gz"
    extra_file = "audience_score.20210628083040-TRG-999-{uid_type}-{target_dt}-90909.pb.gz"

    data_specs = [
        {
            "audience_id": 123,
            "uid_type": "VKID",
            "source_partition_conf": {"audience_name": "A1"},  # audience_name, category
            "min_score": 0.0,
            "max_score": 1.0,
            "scale_conf": {"scale": True, "min": 1, "max": 999, "revert": False},
            "min_target_rows": 1,
            "max_target_rows": 2,
            "output_max_rows_per_file": 1,
            "source": {
                "data": [
                    {"uid": "10001", "score": 0.91, "audience_name": "A1", "category": "positive", "dt": "2020-05-01", "uid_type": "VKID",},
                    {"uid": "10002", "score": 0.91, "audience_name": "A1", "category": "positive", "dt": "2020-05-01", "uid_type": "OKID",},
                ],
                "schema": source_schema,
                "partition_columns": source_partition_columns,
            },
            "expected_data": [
                {"id_type": "VKID", "id_number": 10001, "id_string": None, "audience_id": 123, "score": 999},
            ],
            "expected_columns": expected_columns,
            "expected_target_dir": "{target_hdfs_basedir}/vk/{target_dt}",
            "expected_success_file": expected_success_file,
            "expected_data_files": expected_data_files,
            "previous_attempt_file": previous_attempt_file,
            "extra_file": extra_file,
            "expect_fatal": False,
        },  # 1, VKID simple
        {
            "audience_id": 1234,
            "uid_type": "HID",
            "source_partition_conf": {"audience_name": "A1"},  # audience_name, category
            "min_score": 0.0,
            "max_score": 1.0,
            "scale_conf": {"scale": True, "min": 1, "max": 999, "revert": True},
            "min_target_rows": 1,
            "max_target_rows": 2,
            "output_max_rows_per_file": 1,
            "source": {
                "data": [
                    {"uid": "10001", "score": 0.91, "audience_name": "A1", "category": "positive", "dt": "2020-05-01", "uid_type": "HID"},
                    {"uid": "-10001", "score": 0.91, "audience_name": "A1", "category": "positive", "dt": "2020-05-01", "uid_type": "HID"},
                    {"uid": "10002", "score": 0.92, "audience_name": "A1", "category": "positive", "dt": "2020-05-01", "uid_type": "VID"},
                ],
                "schema": source_schema,
                "partition_columns": source_partition_columns,
            },
            "expected_data": [
                {"id_type": "HID", "id_number": -10001, "id_string": None, "audience_id": 1234, "score": 1},
                {"id_type": "HID", "id_number": 10001, "id_string": None, "audience_id": 1234, "score": 1},
            ],
            "expected_columns": expected_columns,
            "expected_target_dir": "{target_hdfs_basedir}/hid/{target_dt}/",
            "expected_success_file": expected_success_file,
            "expected_data_files": expected_data_files,
            "previous_attempt_file": previous_attempt_file,
            "extra_file": extra_file,
            "expect_fatal": False,
        },  # 2, HID
        {
            "uid_type": "HID",
            "source_partition_conf": {"audience_name": "A1"},
            "source": {
                "data": [
                    {"uid": "10001", "score": 0.91, "audience_name": "A1", "category": "positive", "dt": "2020-05-01", "uid_type": "OKID"},
                    {"uid": "-10001", "score": 0.92, "audience_name": "A1", "category": "positive", "dt": "2020-05-01", "uid_type": "VKID"},
                    {"uid": "10002", "score": 0.93, "audience_name": "A1", "category": "positive", "dt": "2020-05-01", "uid_type": "VID"},
                ],
                "schema": source_schema,
                "partition_columns": source_partition_columns,
            },
            "expected_data": [
            ],
            "expected_columns": expected_columns,
            "expected_target_dir": "{target_hdfs_basedir}/hid/{target_dt}/",
            "expected_success_file": expected_success_file,
            "expected_data_files": expected_data_files,
            "previous_attempt_file": previous_attempt_file,
            "extra_file": extra_file,
            "expect_fatal": True,
            "exception": MissingDepsStatusException,
        },  # 3, empty source
        {
            "uid_type": "FOO",
            "source_partition_conf": {"audience_name": "A1"},
            "source": {
                "data": [
                    {"uid": "10001", "score": 0.91, "audience_name": "A1", "category": "positive", "dt": "2020-05-01", "uid_type": "FOO"},
                    {"uid": "10002", "score": 0.92, "audience_name": "A1", "category": "positive", "dt": "2020-05-01", "uid_type": "BAR"},
                    {"uid": "10003", "score": 0.93, "audience_name": "A1", "category": "positive", "dt": "2020-05-01", "uid_type": "BAZ"},
                ],
                "schema": source_schema,
                "partition_columns": source_partition_columns,
            },
            "expected_data": [
            ],
            "expected_columns": expected_columns,
            "expected_target_dir": "{target_hdfs_basedir}/hid/{target_dt}/",
            "expected_success_file": expected_success_file,
            "expected_data_files": expected_data_files,
            "previous_attempt_file": previous_attempt_file,
            "extra_file": extra_file,
            "expect_fatal": True,
            "exception": FatalStatusException
        },  # 4, invalid uid_type
        {
            "uid_type": "HID",
            "source_partition_conf": {"audience_name": "A1"},
            "source": {
                "data": [
                    {"uid": "10001", "score": 0.910, "audience_name": "A1", "category": "positive", "dt": "2020-05-01", "uid_type": "HID"},
                    {"uid": "-10002", "score": None, "audience_name": "A1", "category": "positive", "dt": "2020-05-01", "uid_type": "HID"},
                    {"uid": "10003", "score": -0.01, "audience_name": "A1", "category": "positive", "dt": "2020-05-01", "uid_type": "HID"},
                    {"uid": "10004", "score": 1.001, "audience_name": "A1", "category": "positive", "dt": "2020-05-01", "uid_type": "HID"},
                    {"uid": "10005", "score": float("nan"), "audience_name": "A1", "category": "positive", "dt": "2020-05-01", "uid_type": "HID"},
                ],
                "schema": source_schema,
                "partition_columns": source_partition_columns,
            },
            "expected_data": [
                {"id_type": "HID", "id_number": 10001, "id_string": None, "audience_id": 123, "score": 999},
            ],
            "expected_target_dir": "{target_hdfs_basedir}/hid/{target_dt}/",
            "expected_columns": expected_columns,
            "expected_success_file": expected_success_file,
            "expected_data_files": expected_data_files,
            "previous_attempt_file": previous_attempt_file,
            "extra_file": extra_file,
        },  # 5, invalid score
        {
            "min_target_rows": 3,
            "max_target_rows": 3,
            "uid_type": "HID",
            "source_partition_conf": {"audience_name": "A1"},
            "source": {
                "data": [
                    {"uid": "10001", "score": 0.910, "audience_name": "A1", "category": "positive", "dt": "2020-05-01", "uid_type": "HID"},
                    {"uid": "-10002", "score": 0.01, "audience_name": "A1", "category": "positive", "dt": "2020-05-01", "uid_type": "HID"},
                ],
                "schema": source_schema,
                "partition_columns": source_partition_columns,
            },
            "expected_data": [
            ],
            "expect_fatal": True,
            "expected_target_dir": "{target_hdfs_basedir}/hid/{target_dt}/",
            "expected_columns": expected_columns,
            "expected_success_file": expected_success_file,
            "expected_data_files": expected_data_files,
            "previous_attempt_file": previous_attempt_file,
            "extra_file": extra_file,
        },  # 6, min records limit
        {
            "max_target_rows": 2,
            "shuffle_partitions": 1,
            "uid_type": "HID",
            "source_partition_conf": {"audience_name": "A1"},
            "source": {
                "data": [
                    {"uid": "10001", "score": 0.910, "audience_name": "A1", "category": "positive", "dt": "2020-05-01", "uid_type": "HID"},
                    {"uid": "10002", "score": 0.010, "audience_name": "A1", "category": "positive", "dt": "2020-05-01", "uid_type": "HID"},
                    {"uid": "10003", "score": 0.011, "audience_name": "A1", "category": "positive", "dt": "2020-05-01", "uid_type": "HID"},
                    {"uid": "10004", "score": 0.009, "audience_name": "A1", "category": "positive", "dt": "2020-05-01", "uid_type": "HID"},
                ],
                "schema": source_schema,
                "partition_columns": source_partition_columns,
            },
            "expected_data": [
                {"id_type": "HID", "id_number": 10001, "id_string": None, "audience_id": 123, "score": 999},
                {"id_type": "HID", "id_number": 10003, "id_string": None, "audience_id": 123, "score": 1},
            ],
            "expected_target_dir": "{target_hdfs_basedir}/hid/{target_dt}/",
            "expected_columns": expected_columns,
            "expected_success_file": expected_success_file,
            "expected_data_files": expected_data_files,
            "previous_attempt_file": previous_attempt_file,
            "extra_file": extra_file,
        },  # 7, top M records
        {
            "max_target_rows": 5,
            "uid_type": "HID",
            "source_partition_conf": {"category": "positive"},
            "source": {
                "data": [
                    {"uid": "10001", "score": 0.68, "audience_name": "A1", "category": "positive", "dt": "2020-05-01", "uid_type": "HID"},
                    {"uid": "10001", "score": 0.81, "audience_name": "A2", "category": "positive", "dt": "2020-05-01", "uid_type": "HID"},
                    {"uid": "10001", "score": 0.91, "audience_name": "A3", "category": "positive", "dt": "2020-05-01", "uid_type": "HID"},
                    {"uid": "10002", "score": 0.01, "audience_name": "A1", "category": "positive", "dt": "2020-05-01", "uid_type": "HID"},
                    {"uid": "10003", "score": 0.4, "audience_name": "A1", "category": "positive", "dt": "2020-05-01", "uid_type": "HID"},
                ],
                "schema": source_schema,
                "partition_columns": source_partition_columns,
            },
            "expected_data": [
                {"id_type": "HID", "id_number": 10001, "id_string": None, "audience_id": 123, "score": 999},
                {"id_type": "HID", "id_number": 10002, "id_string": None, "audience_id": 123, "score": 1},
                {"id_type": "HID", "id_number": 10003, "id_string": None, "audience_id": 123, "score": 494},
            ],
            "expected_target_dir": "{target_hdfs_basedir}/hid/{target_dt}/",
            "expected_columns": expected_columns,
            "expected_success_file": expected_success_file,
            "expected_data_files": expected_data_files,
            "previous_attempt_file": previous_attempt_file,
            "extra_file": extra_file,
        },  # 8, agg by user
        {
            "max_target_rows": 5,
            "scale_conf": {"scale": True, "min": 1, "max": 999, "revert": True, },
            "uid_type": "HID",
            "source_partition_conf": {"category": "positive"},
            "source": {
                "data": [
                    {"uid": "10001", "score": 0.68, "audience_name": "A1", "category": "positive", "dt": "2020-05-01", "uid_type": "HID"},
                    {"uid": "10001", "score": 0.81, "audience_name": "A2", "category": "positive", "dt": "2020-05-01", "uid_type": "HID"},
                    {"uid": "10001", "score": 0.91, "audience_name": "A3", "category": "positive", "dt": "2020-05-01", "uid_type": "HID"},
                    {"uid": "10002", "score": 0.01, "audience_name": "A1", "category": "positive", "dt": "2020-05-01", "uid_type": "HID"},
                    {"uid": "10003", "score": 0.4, "audience_name": "A1", "category": "positive", "dt": "2020-05-01", "uid_type": "HID"},
                ],
                "schema": source_schema,
                "partition_columns": source_partition_columns,
            },
            "expected_data": [
                {"id_type": "HID", "id_number": 10001, "id_string": None, "audience_id": 123, "score": 1},
                {"id_type": "HID", "id_number": 10002, "id_string": None, "audience_id": 123, "score": 999},
                {"id_type": "HID", "id_number": 10003, "id_string": None, "audience_id": 123, "score": 506},
            ],
            "expected_target_dir": "{target_hdfs_basedir}/hid/{target_dt}/",
            "expected_columns": expected_columns,
            "expected_success_file": expected_success_file,
            "expected_data_files": expected_data_files,
            "previous_attempt_file": previous_attempt_file,
            "extra_file": extra_file,
        },  # 9, scale, reverse
        {
            "max_target_rows": 5,
            "min_score": 0.0,
            "max_score": 1000.0,
            "scale_conf": {"scale": False, "min": 1, "max": 999, "revert": True, },
            "uid_type": "HID",
            "source_partition_conf": {"category": "positive"},
            "source": {
                "data": [
                    {"uid": "10001", "score": 680.0, "audience_name": "A1", "category": "positive", "dt": "2020-05-01", "uid_type": "HID"},
                    {"uid": "10001", "score": 810.0, "audience_name": "A2", "category": "positive", "dt": "2020-05-01", "uid_type": "HID"},
                    {"uid": "10001", "score": 910.0, "audience_name": "A3", "category": "positive", "dt": "2020-05-01", "uid_type": "HID"},
                    {"uid": "10002", "score": 10.0, "audience_name": "A1", "category": "positive", "dt": "2020-05-01", "uid_type": "HID"},
                    {"uid": "10003", "score": 400.0, "audience_name": "A1", "category": "positive", "dt": "2020-05-01", "uid_type": "HID"},
                ],
                "schema": source_schema,
                "partition_columns": source_partition_columns,
            },
            "expected_data": [
                {"id_type": "HID", "id_number": 10001, "id_string": None, "audience_id": 123, "score": 800},
                {"id_type": "HID", "id_number": 10002, "id_string": None, "audience_id": 123, "score": 10},
                {"id_type": "HID", "id_number": 10003, "id_string": None, "audience_id": 123, "score": 400},
            ],
            "expected_target_dir": "{target_hdfs_basedir}/hid/{target_dt}/",
            "expected_columns": expected_columns,
            "expected_success_file": expected_success_file,
            "expected_data_files": expected_data_files,
            "previous_attempt_file": previous_attempt_file,
            "extra_file": extra_file,
        },  # 10, no scale
        {
            "uid_type": "EMAIL",
            "source_partition_conf": {"category": "positive"},
            "source": {
                "data": [
                    {"uid": "1@mrg", "score": 0.68, "audience_name": "A1", "category": "positive", "dt": "2020-05-01", "uid_type": "EMAIL"},
                    {"uid": "foo@bar.baz", "score": 0.68, "audience_name": "A1", "category": "positive", "dt": "2020-05-01", "uid_type": "EMAIL"},
                    {"uid": "00D70A01E57B4A6BBDA12AA04CBEF113", "score": 0.81, "audience_name": "A2", "category": "positive", "dt": "2020-05-01", "uid_type": "IDFA"},
                    {"uid": "0039548579FC479BA594AB673BD3C255", "score": 0.91, "audience_name": "A3", "category": "positive", "dt": "2020-05-01", "uid_type": "GAID"},
                ],
                "schema": source_schema,
                "partition_columns": source_partition_columns,
            },
            "expected_data": [
                {"id_type": "MM", "id_number": -8194459535396332297, "id_string": None, "audience_id": 123, "score": 999},
                {"id_type": "MM", "id_number": 2627216423570133136, "id_string": None, "audience_id": 123, "score": 999},
            ],
            "expected_target_dir": "{target_hdfs_basedir}/mm/{target_dt}",
            "expected_columns": expected_columns,
            "expected_success_file": expected_success_file,
            "expected_data_files": expected_data_files,
            "previous_attempt_file": previous_attempt_file,
            "extra_file": extra_file,
        },  # 11, EMAIL
        {
            "uid_type": "IDFA",
            "source_partition_conf": {"category": "positive"},
            "source": {
                "data": [
                    {"uid": "foo@bar.baz", "score": 0.68, "audience_name": "A1", "category": "positive", "dt": "2020-05-01", "uid_type": "EMAIL"},
                    {"uid": "aaaabbbbccccddddeeeeffff00001111", "score": 0.81, "audience_name": "A2", "category": "positive", "dt": "2020-05-01", "uid_type": "IDFA"},
                    {"uid": "0039548579FC479BA594AB673BD3C255", "score": 0.91, "audience_name": "A3", "category": "positive", "dt": "2020-05-01", "uid_type": "GAID"},
                ],
                "schema": source_schema,
                "partition_columns": source_partition_columns,
            },
            "expected_data": [
                {"id_type": "DEVICE", "id_number": 0, "id_string": "AAAABBBB-CCCC-DDDD-EEEE-FFFF00001111", "audience_id": 123, "score": 999},
            ],
            "expected_target_dir": "{target_hdfs_basedir}/device/{target_dt}",
            "expected_columns": expected_columns,
            "expected_success_file": expected_success_file,
            "expected_data_files": expected_data_files,
            "previous_attempt_file": previous_attempt_file,
            "extra_file": extra_file,
        },  # 12, IDFA
        {
            "uid_type": "GAID",
            "source_partition_conf": {"category": "positive"},
            "source": {
                "data": [
                    {"uid": "foo@bar.baz", "score": 0.68, "audience_name": "A1", "category": "positive", "dt": "2020-05-01", "uid_type": "EMAIL"},
                    {"uid": "00D70A01E57B4A6BBDA12AA04CBEF113", "score": 0.81, "audience_name": "A2", "category": "positive", "dt": "2020-05-01", "uid_type": "IDFA"},
                    {"uid": "AAAABBBBCCCCDDDDEEEEFFFF00001111", "score": 0.91, "audience_name": "A3", "category": "positive", "dt": "2020-05-01", "uid_type": "GAID"},
                ],
                "schema": source_schema,
                "partition_columns": source_partition_columns,
            },
            "expected_data": [
                {"id_type": "DEVICE", "id_number": 0, "id_string": "aaaabbbb-cccc-dddd-eeee-ffff00001111", "audience_id": 123, "score": 999},
            ],
            "expected_target_dir": "{target_hdfs_basedir}/device/{target_dt}",
            "expected_columns": expected_columns,
            "expected_success_file": expected_success_file,
            "expected_data_files": expected_data_files,
            "previous_attempt_file": previous_attempt_file,
            "extra_file": extra_file,
        },  # 13, GAID
        {
            "audience_id": 123,
            "uid_type": "VKID",
            "source_partition_conf": {"audience_name": "A1"},
            "min_score": 0.0,
            "max_score": 1.0,
            "scale_conf": {"scale": True, "min": 1, "max": 999, "revert": True},
            "min_target_rows": 1,
            "max_target_rows": 5,
            "output_max_rows_per_file": 1,
            "source": {
                "data": [
                    {"uid": "10001", "score": 0.91, "audience_name": "A1", "category": "positive", "dt": "2020-05-01", "uid_type": "VKID",},
                    {"uid": "10002", "score": 0.91, "audience_name": "A1", "category": "positive", "dt": "2020-05-01", "uid_type": "VKID",},
                    {"uid": "10003", "score": 0.91, "audience_name": "A1", "category": "positive", "dt": "2020-05-01", "uid_type": "VKID",},
                    {"uid": "10004", "score": 0.91, "audience_name": "A1", "category": "positive", "dt": "2020-05-01", "uid_type": "VKID",},
                    {"uid": "10005", "score": 0.91, "audience_name": "A1", "category": "positive", "dt": "2020-05-01", "uid_type": "VKID",},
                ],
                "schema": source_schema,
                "partition_columns": source_partition_columns,
            },
            "expected_data": [
                {"id_type": "VKID", "id_number": 10001, "id_string": None, "audience_id": 123, "score": 1},
                {"id_type": "VKID", "id_number": 10002, "id_string": None, "audience_id": 123, "score": 1},
                {"id_type": "VKID", "id_number": 10003, "id_string": None, "audience_id": 123, "score": 1},
                {"id_type": "VKID", "id_number": 10004, "id_string": None, "audience_id": 123, "score": 1},
                {"id_type": "VKID", "id_number": 10005, "id_string": None, "audience_id": 123, "score": 1},
            ],
            "expected_target_dir": "{target_hdfs_basedir}/vk/{target_dt}",
            "expected_columns": expected_columns,
            "expected_success_file": expected_success_file,
            "expected_data_files": expected_data_files,
            "previous_attempt_file": previous_attempt_file,
            "extra_file": extra_file,
            "expect_fatal": False,
        },  # 14, VKID multiple files
        {
            "audience_id": 123,
            "uid_type": "VID",
            "source_partition_conf": {"audience_name": "A1"},
            "max_target_rows": 5,
            "scale_conf": {"scale": True, "min": 1, "max": 999, "revert": False},
            "source": {
                "data": [
                    {"uid": "8000000000000000", "score": 0.92, "audience_name": "A1", "category": "positive", "dt": "2020-05-01", "uid_type": "VID"},
                    {"uid": "10001", "score": 0.91, "audience_name": "A1", "category": "positive", "dt": "2020-05-01", "uid_type": "VID"},
                ],
                "schema": source_schema,
                "partition_columns": source_partition_columns,
            },
            "expected_data": [
                {"id_type": "VID", "id_number": -9223372036854775808, "id_string": None, "audience_id": 123, "score": 999},
                {"id_type": "VID", "id_number": 65537, "id_string": None, "audience_id": 123, "score": 1},
            ],
            "expected_target_dir": "{target_hdfs_basedir}/vid/{target_dt}",
            "expected_columns": expected_columns,
            "expected_success_file": expected_success_file,
            "expected_data_files": expected_data_files,
            "previous_attempt_file": previous_attempt_file,
            "extra_file": extra_file,
            "expect_fatal": False,
        },  # 15, VID
        {
         "audience_id": 123,
         "uid_type": "MMID",
         "source_partition_conf": {"audience_name": "A1"},
         "max_target_rows": 5,
         "scale_conf": {"scale": True, "min": 1, "max": 999, "revert": False},
         "source": {
             "data": [
                 {"uid": "10001", "score": 0.91, "audience_name": "A1", "category": "positive", "dt": "2020-05-01", "uid_type": "MMID"},
             ],
             "schema": source_schema,
             "partition_columns": source_partition_columns,
         },
         "expected_data": [
             {"id_type": "MM", "id_number": 10001, "id_string": None, "audience_id": 123, "score": 999},
         ],
         "expected_target_dir": "{target_hdfs_basedir}/mm/{target_dt}",
         "expected_columns": expected_columns,
         "expected_success_file": expected_success_file,
         "expected_data_files": expected_data_files,
         "previous_attempt_file": previous_attempt_file,
         "extra_file": extra_file,
         "expect_fatal": False,
        },  # 16, MMID
        {
            "audience_id": 1,
            "uid_type": "VKID",
            "source_partition_conf": {"audience_name": "A1"},
            "max_target_rows": 15,
            "scale_conf": {"scale": True, "min": 0, "max": 200, "revert": False},
            "source": {
                "data": [
                    {"uid": "10001", "score": 0.91, "audience_name": "A1", "category": "positive", "dt": "2020-05-01", "uid_type": "VKID"},
                    {"uid": "10002", "score": 0.92, "audience_name": "A1", "category": "positive", "dt": "2020-05-01", "uid_type": "VKID"},
                    {"uid": "10003", "score": 0.93, "audience_name": "A1", "category": "positive", "dt": "2020-05-01", "uid_type": "VKID"},
                    {"uid": "10004", "score": 0.94, "audience_name": "A1", "category": "positive", "dt": "2020-05-01", "uid_type": "VKID"},
                    {"uid": "10005", "score": 0.95, "audience_name": "A1", "category": "positive", "dt": "2020-05-01", "uid_type": "VKID"},
                    {"uid": "10006", "score": 0.96, "audience_name": "A1", "category": "positive", "dt": "2020-05-01", "uid_type": "VKID"},
                    {"uid": "10007", "score": 0.97, "audience_name": "A1", "category": "positive", "dt": "2020-05-01", "uid_type": "VKID"},
                    {"uid": "10006", "score": 0.86, "audience_name": "A1", "category": "positive", "dt": "2020-05-01", "uid_type": "VKID"},
                    {"uid": "10005", "score": 0.85, "audience_name": "A1", "category": "positive", "dt": "2020-05-01", "uid_type": "VKID"},
                    {"uid": "10004", "score": 0.84, "audience_name": "A1", "category": "positive", "dt": "2020-05-01", "uid_type": "VKID"},
                    {"uid": "10003", "score": 0.83, "audience_name": "A1", "category": "positive", "dt": "2020-05-01", "uid_type": "VKID"},
                    {"uid": "10002", "score": 0.82, "audience_name": "A1", "category": "positive", "dt": "2020-05-01", "uid_type": "VKID"},
                    {"uid": "10001", "score": 0.81, "audience_name": "A1", "category": "positive", "dt": "2020-05-01", "uid_type": "VKID"},
                ],
                "schema": source_schema,
                "partition_columns": source_partition_columns,
            },
            "expected_data": [
                {"id_type": "VKID", "id_number": 10001, "id_string": None, "audience_id": 1, "score": 0},
                {"id_type": "VKID", "id_number": 10002, "id_string": None, "audience_id": 1, "score": 18},
                {"id_type": "VKID", "id_number": 10003, "id_string": None, "audience_id": 1, "score": 36},
                {"id_type": "VKID", "id_number": 10004, "id_string": None, "audience_id": 1, "score": 55},
                {"id_type": "VKID", "id_number": 10005, "id_string": None, "audience_id": 1, "score": 73},
                {"id_type": "VKID", "id_number": 10006, "id_string": None, "audience_id": 1, "score": 91},
                {"id_type": "VKID", "id_number": 10007, "id_string": None, "audience_id": 1, "score": 200},
            ],
            "expected_target_dir": "{target_hdfs_basedir}/vk/{target_dt}",
            "expected_columns": expected_columns,
            "expected_success_file": expected_success_file,
            "expected_data_files": expected_data_files,
            "previous_attempt_file": previous_attempt_file,
            "extra_file": extra_file,
            "expect_fatal": False,
        },  # 17, VKID scale
        {
            "audience_id": 1,
            "uid_type": "VKID",
            "source_partition_conf": {"audience_name": "A1"},
            "max_target_rows": 15,
            "scale_conf": {"scale": True, "min": 0, "max": 200, "revert": True},
            "source": {
                "data": [
                    {"uid": "10001", "score": 0.91, "audience_name": "A1", "category": "positive", "dt": "2020-05-01", "uid_type": "VKID"},
                    {"uid": "10002", "score": 0.92, "audience_name": "A1", "category": "positive", "dt": "2020-05-01", "uid_type": "VKID"},
                    {"uid": "10003", "score": 0.93, "audience_name": "A1", "category": "positive", "dt": "2020-05-01", "uid_type": "VKID"},
                    {"uid": "10004", "score": 0.94, "audience_name": "A1", "category": "positive", "dt": "2020-05-01", "uid_type": "VKID"},
                    {"uid": "10005", "score": 0.95, "audience_name": "A1", "category": "positive", "dt": "2020-05-01", "uid_type": "VKID"},
                    {"uid": "10006", "score": 0.96, "audience_name": "A1", "category": "positive", "dt": "2020-05-01", "uid_type": "VKID"},
                    {"uid": "10007", "score": 0.97, "audience_name": "A1", "category": "positive", "dt": "2020-05-01", "uid_type": "VKID"},
                    {"uid": "10006", "score": 0.86, "audience_name": "A1", "category": "positive", "dt": "2020-05-01", "uid_type": "VKID"},
                    {"uid": "10005", "score": 0.85, "audience_name": "A1", "category": "positive", "dt": "2020-05-01", "uid_type": "VKID"},
                    {"uid": "10004", "score": 0.84, "audience_name": "A1", "category": "positive", "dt": "2020-05-01", "uid_type": "VKID"},
                    {"uid": "10003", "score": 0.83, "audience_name": "A1", "category": "positive", "dt": "2020-05-01", "uid_type": "VKID"},
                    {"uid": "10002", "score": 0.82, "audience_name": "A1", "category": "positive", "dt": "2020-05-01", "uid_type": "VKID"},
                    {"uid": "10001", "score": 0.81, "audience_name": "A1", "category": "positive", "dt": "2020-05-01", "uid_type": "VKID"},
                ],
                "schema": source_schema,
                "partition_columns": source_partition_columns,
            },
            "expected_data": [
                {"id_type": "VKID", "id_number": 10001, "id_string": None, "audience_id": 1, "score": 200},
                {"id_type": "VKID", "id_number": 10002, "id_string": None, "audience_id": 1, "score": 182},
                {"id_type": "VKID", "id_number": 10003, "id_string": None, "audience_id": 1, "score": 164},
                {"id_type": "VKID", "id_number": 10004, "id_string": None, "audience_id": 1, "score": 145},
                {"id_type": "VKID", "id_number": 10005, "id_string": None, "audience_id": 1, "score": 127},
                {"id_type": "VKID", "id_number": 10006, "id_string": None, "audience_id": 1, "score": 109},
                {"id_type": "VKID", "id_number": 10007, "id_string": None, "audience_id": 1, "score": 0},
            ],
            "expected_target_dir": "{target_hdfs_basedir}/vk/{target_dt}",
            "expected_columns": expected_columns,
            "expected_success_file": expected_success_file,
            "expected_data_files": expected_data_files,
            "previous_attempt_file": previous_attempt_file,
            "extra_file": extra_file,
            "expect_fatal": False,
        },  # 18, VKID reversed scale
    ]

    # test cases, list of (app_cfg, expected_cfg, i,)
    return [
        (
            {
                # target
                "audience_id": spec.get("audience_id", 123),  # uint32
                "uid_type": spec.get("uid_type", "EMAIL"),
                "target_dt": spec.get("target_dt", TARGET_DT),
                "max_dt_diff": spec.get("max_dt_diff", 60),
                "target_hdfs_basedir": temp_path("result_{}".format(i)),  # hdfs:/export/target/custom_audience/
                "success_hdfs_basedir": temp_path("success_{}".format(i)),  # hdfs:/data/dev/apps/export/audience/trg/
                # source
                "source_db": TEST_DB,
                "source_table": "grinder_{}_source_v{}".format(NAME, i),
                "source_partition_conf": spec["source_partition_conf"],  # no dt or uid_type
                # transform
                "scale_conf": spec.get(
                    "scale_conf",
                    {
                        "scale": True,  # perform scaling
                        "min": 1,  # uint32
                        "max": 999,  # uint32
                        "revert": False,  # reverted scale needed
                    },
                ),
                # limits
                "min_score": spec.get("min_score", 0.0),  # float, score low limit
                "max_score": spec.get("max_score", 1.0),  # float, score top limit
                "min_target_rows": spec.get("min_target_rows", 1),  # min rows to consider as enough data
                "output_max_rows_per_file": spec.get("output_max_rows_per_file", 1),
                "max_target_rows": spec.get("max_target_rows", 2),  # total max rows
                # spark
                "shuffle_partitions": spec.get("shuffle_partitions", 1),
                # control
                "ctid": "ctid_{}".format(i),
                "input_urls": [temp_path("input_{}".format(i))],
                "output_urls": [temp_path("output_{}".format(i))],
                "status_urls": [temp_path("status_{}".format(i))],
            },  # task conf
            spec,  # data conf
            i,  # index
        )
        for i, spec in enumerate(data_specs, 1)
    ]

# @formatter:on
# fmt: on
