import os

import numpy as np

from prj.apps.utils.testing.defines import TEST_DB, TEST_HDFS_DIR
from prj.apps.utils.control.client.status import STATUS

NAME = "test_export_audience_trg_app"
TARGET_DT = "2020-06-01"
TMP_HDFS_DIR = os.path.join(TEST_HDFS_DIR, NAME)

# flake8: noqa
# fmt: off

SOURCE_TABLE = "grinder_{}_source".format(NAME)
SOURCE_TABLE_SCHEMA = (
    ("uid", "string"),
    ("score", "double"),
    ("audience_name", "string"),
    ("category", "string"),
    ("dt", "string"),
    ("uid_type", "string"),
)
SOURCE_PARTITION_COLUMNS = ["audience_name", "category", "dt", "uid_type"]
SOURCE_DATA = [
    {"uid": "10001", "score": 0.91, "audience_name": "A1", "category": "positive", "dt": "2020-05-01", "uid_type": "VKID"},
    {"uid": "10002", "score": 0.91, "audience_name": "A1", "category": "positive", "dt": "2020-05-01", "uid_type": "OKID"},
    {"uid": "10001", "score": 0.91, "audience_name": "A2", "category": "positive", "dt": "2020-05-01", "uid_type": "HID"},
    {"uid": "-10001", "score": 0.91, "audience_name": "A2", "category": "positive", "dt": "2020-05-01", "uid_type": "HID"},
    {"uid": "10002", "score": 0.92, "audience_name": "A2", "category": "positive", "dt": "2020-05-01", "uid_type": "VID"},
    {"uid": "10001", "score": 0.910, "audience_name": "A3", "category": "positive", "dt": "2020-05-01", "uid_type": "HID"},
    {"uid": "-10002", "score": np.nan, "audience_name": "A3", "category": "positive", "dt": "2020-05-01", "uid_type": "HID"},
    {"uid": "10003", "score": -0.01, "audience_name": "A3", "category": "positive", "dt": "2020-05-01", "uid_type": "HID"},
    {"uid": "10004", "score": 1.001, "audience_name": "A3", "category": "positive", "dt": "2020-05-01", "uid_type": "HID"},
    {"uid": "10005", "score": float("nan"), "audience_name": "A3", "category": "positive", "dt": "2020-05-01", "uid_type": "HID"},
    {"uid": "10001", "score": 0.910, "audience_name": "A4", "category": "positive", "dt": "2020-05-01", "uid_type": "HID"},
    {"uid": "10002", "score": 0.010, "audience_name": "A4", "category": "positive", "dt": "2020-05-01", "uid_type": "HID"},
    {"uid": "10003", "score": 0.011, "audience_name": "A4", "category": "positive", "dt": "2020-05-01", "uid_type": "HID"},
    {"uid": "10004", "score": 0.009, "audience_name": "A4", "category": "positive", "dt": "2020-05-01", "uid_type": "HID"},
    {"uid": "10001", "score": 0.68, "audience_name": "A5", "category": "positive", "dt": "2020-05-01", "uid_type": "HID"},
    {"uid": "10001", "score": 0.81, "audience_name": "A5", "category": "positive", "dt": "2020-05-01", "uid_type": "HID"},
    {"uid": "10001", "score": 0.91, "audience_name": "A5", "category": "positive", "dt": "2020-05-01", "uid_type": "HID"},
    {"uid": "10002", "score": 0.01, "audience_name": "A5", "category": "positive", "dt": "2020-05-01", "uid_type": "HID"},
    {"uid": "10003", "score": 0.4, "audience_name": "A5", "category": "positive", "dt": "2020-05-01", "uid_type": "HID"},
    {"uid": "10001", "score": 0.68, "audience_name": "A6", "category": "positive", "dt": "2020-05-01", "uid_type": "HID"},
    {"uid": "10001", "score": 0.81, "audience_name": "A6", "category": "positive", "dt": "2020-05-01", "uid_type": "HID"},
    {"uid": "10001", "score": 0.91, "audience_name": "A6", "category": "positive", "dt": "2020-05-01", "uid_type": "HID"},
    {"uid": "10002", "score": 0.01, "audience_name": "A6", "category": "positive", "dt": "2020-05-01", "uid_type": "HID"},
    {"uid": "10003", "score": 0.4, "audience_name": "A6", "category": "positive", "dt": "2020-05-01", "uid_type": "HID"},
    {"uid": "10001", "score": 680.0, "audience_name": "A7", "category": "positive", "dt": "2020-05-01", "uid_type": "HID"},
    {"uid": "10001", "score": 810.0, "audience_name": "A7", "category": "positive", "dt": "2020-05-01", "uid_type": "HID"},
    {"uid": "10001", "score": 910.0, "audience_name": "A7", "category": "positive", "dt": "2020-05-01", "uid_type": "HID"},
    {"uid": "10002", "score": 10.0, "audience_name": "A7", "category": "positive", "dt": "2020-05-01", "uid_type": "HID"},
    {"uid": "10003", "score": 400.0, "audience_name": "A7", "category": "positive", "dt": "2020-05-01", "uid_type": "HID"},
    {"uid": "foo@bar.baz", "score": 0.68, "audience_name": "A8", "category": "positive", "dt": "2020-05-01", "uid_type": "EMAIL"},
    {"uid": "00D70A01E57B4A6BBDA12AA04CBEF113", "score": 0.81, "audience_name": "A8", "category": "positive", "dt": "2020-05-01", "uid_type": "IDFA"},
    {"uid": "0039548579FC479BA594AB673BD3C255", "score": 0.91, "audience_name": "A8", "category": "positive", "dt": "2020-05-01", "uid_type": "GAID"},
    {"uid": "foo@bar.baz", "score": 0.68, "audience_name": "A9", "category": "positive", "dt": "2020-05-01", "uid_type": "EMAIL"},
    {"uid": "00D70A01E57B4A6BBDA12AA04CBEF113", "score": 0.81, "audience_name": "A9", "category": "positive", "dt": "2020-05-01", "uid_type": "IDFA"},
    {"uid": "0039548579FC479BA594AB673BD3C255", "score": 0.91, "audience_name": "A9", "category": "positive", "dt": "2020-05-01", "uid_type": "GAID"},
    {"uid": "foo@bar.baz", "score": 0.68, "audience_name": "A10", "category": "positive", "dt": "2020-05-01", "uid_type": "EMAIL"},
    {"uid": "00D70A01E57B4A6BBDA12AA04CBEF113", "score": 0.81, "audience_name": "A10", "category": "positive", "dt": "2020-05-01", "uid_type": "IDFA"},
    {"uid": "0039548579FC479BA594AB673BD3C255", "score": 0.91, "audience_name": "A10", "category": "positive", "dt": "2020-05-01", "uid_type": "GAID"},
    {"uid": "10001", "score": 0.91, "audience_name": "A11", "category": "positive", "dt": "2020-05-01", "uid_type": "VKID"},
    {"uid": "10002", "score": 0.91, "audience_name": "A11", "category": "positive", "dt": "2020-05-01", "uid_type": "VKID"},
    {"uid": "10003", "score": 0.91, "audience_name": "A11", "category": "positive", "dt": "2020-05-01", "uid_type": "VKID"},
    {"uid": "10004", "score": 0.91, "audience_name": "A11", "category": "positive", "dt": "2020-05-01", "uid_type": "VKID"},
    {"uid": "10005", "score": 0.91, "audience_name": "A11", "category": "positive", "dt": "2020-05-01", "uid_type": "VKID"},
]


def generate_test_data():

    data_specs = [
        {
            "audience_id": 123,
            "uid_type": "VKID",
            "source_partition_conf": {"audience_name": "A1"},
            "expected_data": [
                {"id_type": "VKID", "id_number": 10001, "id_string": None, "audience_id": 123, "score": 999},
            ],
            "target_idtype_subdir": "vk",
        },  # 1: VKID simple
        {
            "audience_id": 1234,
            "uid_type": "HID",
            "source_partition_conf": {"audience_name": "A2"},
            "scale_conf": {"scale": True, "min": 1, "max": 999, "revert": True},
            "expected_data": [
                {"id_type": "HID", "id_number": -10001, "id_string": None, "audience_id": 1234, "score": 1},
                {"id_type": "HID", "id_number": 10001, "id_string": None, "audience_id": 1234, "score": 1},
            ],
            "target_idtype_subdir": "hid",
        },  # 2: HID
        {
            "audience_id": 123,
            "uid_type": "HID",
            "source_partition_conf": {"audience_name": "A3"},
            "min_score": 0.0,
            "max_score": 1.0,
            "min_target_rows": 2,
            "expected_data": [],
            "expected_statuses": [STATUS.FATAL],
            "target_idtype_subdir": "hid",
        },  # 3: invalid score leading to too few rows
        {
            "audience_id": 123,
            "uid_type": "HID",
            "source_partition_conf": {"audience_name": "A4"},
            "max_target_rows": 2,
            "expected_data": [
                {"id_type": "HID", "id_number": 10001, "id_string": None, "audience_id": 123, "score": 999},
                {"id_type": "HID", "id_number": 10003, "id_string": None, "audience_id": 123, "score": 1},
            ],
            "target_idtype_subdir": "hid",
        },  # 4: top records
        {
            "audience_id": 123,
            "uid_type": "HID",
            "source_partition_conf": {"category": "positive", "audience_name": "A5"},
            "max_target_rows": 5,
            "expected_data": [
                {"id_type": "HID", "id_number": 10001, "id_string": None, "audience_id": 123, "score": 999},
                {"id_type": "HID", "id_number": 10002, "id_string": None, "audience_id": 123, "score": 1},
                {"id_type": "HID", "id_number": 10003, "id_string": None, "audience_id": 123, "score": 494},
            ],
            "target_idtype_subdir": "hid",
        },  # 5: agg by user
        {
            "audience_id": 123,
            "uid_type": "HID",
            "source_partition_conf": {"category": "positive", "audience_name": "A6"},
            "max_target_rows": 5,
            "scale_conf": {"scale": True, "min": 1, "max": 999, "revert": True},
            "expected_data": [
                {"id_type": "HID", "id_number": 10001, "id_string": None, "audience_id": 123, "score": 1},
                {"id_type": "HID", "id_number": 10002, "id_string": None, "audience_id": 123, "score": 999},
                {"id_type": "HID", "id_number": 10003, "id_string": None, "audience_id": 123, "score": 506},
            ],
            "target_idtype_subdir": "hid",
        },  # 6: scale, reverse
        {
            "audience_id": 123,
            "uid_type": "HID",
            "source_partition_conf": {"category": "positive", "audience_name": "A7"},
            "max_target_rows": 5,
            "scale_conf": {"scale": False, "min": 1, "max": 999, "revert": True},
            "expected_data": [
                {"id_type": "HID", "id_number": 10001, "id_string": None, "audience_id": 123, "score": 800},
                {"id_type": "HID", "id_number": 10002, "id_string": None, "audience_id": 123, "score": 10},
                {"id_type": "HID", "id_number": 10003, "id_string": None, "audience_id": 123, "score": 400},
            ],
            "target_idtype_subdir": "hid",
        },  # 7: no scale, no min/max
        {
            "audience_id": 123,
            "uid_type": "EMAIL",
            "source_partition_conf": {"category": "positive", "audience_name": "A8"},
            "expected_data": [
                {"id_type": "MM", "id_number": 2627216423570133136, "id_string": None, "audience_id": 123, "score": 999},
            ],
            "target_idtype_subdir": "mm",
        },  # 8: EMAIL
        {
            "audience_id": 123,
            "uid_type": "IDFA",
            "source_partition_conf": {"category": "positive", "audience_name": "A9"},
            "expected_data": [
                {"id_type": "DEVICE", "id_number": 0, "id_string": "00D70A01-E57B-4A6B-BDA1-2AA04CBEF113", "audience_id": 123, "score": 999},
            ],
            "target_idtype_subdir": "device",
        },  # 9: IDFA
        {
            "audience_id": 123,
            "uid_type": "GAID",
            "source_partition_conf": {"category": "positive", "audience_name": "A10"},
            "expected_data": [
                {"id_type": "DEVICE", "id_number": 0, "id_string": "00395485-79fc-479b-a594-ab673bd3c255", "audience_id": 123, "score": 999},
            ],
            "target_idtype_subdir": "device",
        },  # 10: GAID
        {
            "audience_id": 123,
            "uid_type": "VKID",
            "source_partition_conf": {"audience_name": "A11"},
            "max_target_rows": 5,
            "scale_conf": {"scale": True, "min": 1, "max": 999, "revert": True},
            "output_max_rows_per_file": 1,
            "expected_data": [
                {"id_type": "VKID", "id_number": 10001, "id_string": None, "audience_id": 123, "score": 1},
                {"id_type": "VKID", "id_number": 10002, "id_string": None, "audience_id": 123, "score": 1},
                {"id_type": "VKID", "id_number": 10003, "id_string": None, "audience_id": 123, "score": 1},
                {"id_type": "VKID", "id_number": 10004, "id_string": None, "audience_id": 123, "score": 1},
                {"id_type": "VKID", "id_number": 10005, "id_string": None, "audience_id": 123, "score": 1},
            ],
            "target_idtype_subdir": "vk",
        },  # 11: VKID multiple files
        {
            "audience_id": 456,
            "uid_type": "HID",
            "source_partition_conf": {"audience_name": ["A2", "A3", "A4"]},  # Union
            "source_where": "uid != '-10001'",
            "extra_filters": [
                {
                    "db": TEST_DB,
                    "table": SOURCE_TABLE,
                    "partition_conf": {"audience_name": "A5"},
                    "filter_type": "intersection",
                },
                {
                    "db": TEST_DB,
                    "table": SOURCE_TABLE,
                    "partition_conf": {"audience_name": "A6"},
                    "where": "score > 0.5",
                    "filter_type": "subtraction",
                },
            ],
            "expected_data": [
                {"id_type": "HID", "id_number": 10002, "id_string": None, "audience_id": 456, "score": 999},
                {"id_type": "HID", "id_number": 10003, "id_string": None, "audience_id": 456, "score": 1},
            ],
            "target_idtype_subdir": "hid",
        },  # 12: use extra filters
        {
            "audience_id": 123,
            "uid_type": "HID",
            "source_partition_conf": {"audience_name": "A7"},
            "max_dt_diff": 14,
            "expected_data": [],
            "expected_statuses": [STATUS.MISSING_DEPS],
            "target_idtype_subdir": "hid",
        },  # 13: not found source partitions
    ]

    # test cases, list of (app_config, expected_config)
    return [
        (
            {
                "audience_id": spec["audience_id"],
                "uid_type": spec["uid_type"],
                "target_dt": TARGET_DT,
                "source_db": TEST_DB,
                "source_table": SOURCE_TABLE,
                "source_partition_conf": spec["source_partition_conf"],
                "max_dt_diff": spec.get("max_dt_diff"),
                "source_where": spec.get("source_where"),
                "min_score": spec.get("min_score"),
                "max_score": spec.get("max_score"),
                "extra_filters": spec.get("extra_filters", []),
                "min_target_rows": spec.get("min_target_rows", 1),
                "max_target_rows": spec.get("max_target_rows", 2),
                "scale_conf": spec.get("scale_conf", {"scale": True, "min": 1, "max": 999, "revert": False}),
                "shuffle_partitions": spec.get("shuffle_partitions", 1),
                "target_hdfs_basedir": os.path.join(TMP_HDFS_DIR, "result_{}".format(i), ""),
                "success_hdfs_basedir": os.path.join(TMP_HDFS_DIR, "success_{}".format(i), ""),
                "output_max_rows_per_file": spec.get("output_max_rows_per_file", 1),
                "ctid": "ctid_{}".format(i),
                "input_urls": [os.path.join(TMP_HDFS_DIR, "input_{}".format(i))],
                "output_urls": [os.path.join(TMP_HDFS_DIR, "output_{}".format(i))],
                "status_urls": [os.path.join(TMP_HDFS_DIR, "status_{}".format(i))],
            },
            dict(
                spec,
                expected_statuses=spec.get("expected_statuses", [STATUS.SUCCESS]),
                expected_columns=["id_type", "id_number", "id_string", "audience_id", "score"],
                expected_target_dir="{{target_hdfs_basedir}}/{}/{{target_dt}}".format(spec["target_idtype_subdir"]),
                expected_data_files="audience_score.*-{ctid}-TRG-{audience_id}-{uid_type}-{target_dt}-*.pb.gz",
                expected_success_file="{success_hdfs_basedir}/{target_dt}/ExportAudienceTrgTask_{audience_id}-{uid_type}-{target_dt}.success",
            )
        )
        for i, spec in enumerate(data_specs, 1)
    ]
