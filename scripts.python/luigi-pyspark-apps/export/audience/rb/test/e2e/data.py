import os

import numpy as np

from prj.apps.utils.testing.defines import TEST_DB, TEST_HDFS_DIR
from prj.apps.utils.control.client.status import STATUS

NAME = "test_export_audience_rb_app"
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
    {"uid": "10001", "score": 0.91, "audience_name": "A2", "category": "positive", "dt": "2020-05-01", "uid_type": "VID"},
    {"uid": "-10001", "score": 0.91, "audience_name": "A2", "category": "positive", "dt": "2020-05-01", "uid_type": "VID"},
    {"uid": "10002", "score": 0.92, "audience_name": "A2", "category": "positive", "dt": "2020-05-01", "uid_type": "OKID"},
    {"uid": "10001", "score": 0.910, "audience_name": "A3", "category": "positive", "dt": "2020-05-01", "uid_type": "VID"},
    {"uid": "-10002", "score": np.nan, "audience_name": "A3", "category": "positive", "dt": "2020-05-01", "uid_type": "VID"},
    {"uid": "10003", "score": -9.01, "audience_name": "A3", "category": "positive", "dt": "2020-05-01", "uid_type": "VID"},
    {"uid": "10004", "score": 10.01, "audience_name": "A3", "category": "positive", "dt": "2020-05-01", "uid_type": "VID"},
    {"uid": "10005", "score": float("nan"), "audience_name": "A3", "category": "positive", "dt": "2020-05-01", "uid_type": "VID"},
    {"uid": "10001", "score": 0.910, "audience_name": "A4", "category": "positive", "dt": "2020-05-01", "uid_type": "VID"},
    {"uid": "10002", "score": 0.010, "audience_name": "A4", "category": "positive", "dt": "2020-05-01", "uid_type": "VID"},
    {"uid": "10003", "score": 0.011, "audience_name": "A4", "category": "positive", "dt": "2020-05-01", "uid_type": "VID"},
    {"uid": "10004", "score": 0.009, "audience_name": "A4", "category": "positive", "dt": "2020-05-01", "uid_type": "VID"},
    {"uid": "10001", "score": 0.68, "audience_name": "A5", "category": "positive", "dt": "2020-05-01", "uid_type": "VID"},
    {"uid": "10001", "score": 0.81, "audience_name": "A5", "category": "positive", "dt": "2020-05-01", "uid_type": "VID"},
    {"uid": "10001", "score": 0.91, "audience_name": "A5", "category": "positive", "dt": "2020-05-01", "uid_type": "VID"},
    {"uid": "10002", "score": 0.01, "audience_name": "A5", "category": "positive", "dt": "2020-05-01", "uid_type": "VID"},
    {"uid": "10003", "score": 0.4, "audience_name": "A5", "category": "positive", "dt": "2020-05-01", "uid_type": "VID"},
    {"uid": "foo@bar.baz", "score": 0.68, "audience_name": "A6", "category": "positive", "dt": "2020-05-01", "uid_type": "EMAIL"},
    {"uid": "00D70A01E57B4A6BBDA12AA04CBEF113", "score": 0.81, "audience_name": "A6", "category": "positive", "dt": "2020-05-01", "uid_type": "IDFA"},
    {"uid": "0039548579FC479BA594AB673BD3C255", "score": 0.91, "audience_name": "A6", "category": "positive", "dt": "2020-05-01", "uid_type": "GAID"},
    {"uid": "foo@bar.baz", "score": 0.68, "audience_name": "A7", "category": "positive", "dt": "2020-05-01", "uid_type": "EMAIL"},
    {"uid": "aabbccddeeff00112233445566778899", "score": 0.81, "audience_name": "A7", "category": "positive", "dt": "2020-05-01", "uid_type": "IDFA"},
    {"uid": "0039548579FC479BA594AB673BD3C255", "score": 0.91, "audience_name": "A7", "category": "positive", "dt": "2020-05-01", "uid_type": "GAID"},
    {"uid": "foo@bar.baz", "score": 0.68, "audience_name": "A8", "category": "positive", "dt": "2020-05-01", "uid_type": "EMAIL"},
    {"uid": "00D70A01E57B4A6BBDA12AA04CBEF113", "score": 0.81, "audience_name": "A8", "category": "positive", "dt": "2020-05-01", "uid_type": "IDFA"},
    {"uid": "0039548579FC479BA594AB673BD3C255", "score": 0.91, "audience_name": "A8", "category": "positive", "dt": "2020-05-01", "uid_type": "GAID"},
    {"uid": "10001", "score": 0.91, "audience_name": "A9", "category": "positive", "dt": "2020-05-01", "uid_type": "VKID"},
    {"uid": "10002", "score": 0.91, "audience_name": "A9", "category": "positive", "dt": "2020-05-01", "uid_type": "VKID"},
    {"uid": "10003", "score": 0.91, "audience_name": "A9", "category": "positive", "dt": "2020-05-01", "uid_type": "VKID"},
    {"uid": "10004", "score": 0.91, "audience_name": "A9", "category": "positive", "dt": "2020-05-01", "uid_type": "VKID"},
    {"uid": "10005", "score": 0.91, "audience_name": "A9", "category": "positive", "dt": "2020-05-01", "uid_type": "VKID"},
]


def generate_test_data():

    data_specs = [
        {
            "audience_id": 123123,
            "uid_type": "VKID",
            "life_time": 333,
            "exclude_audience_ids": [122, 124],
            "source_partition_conf": {"audience_name": "A1"},
            "max_target_rows": 2,
            "expected_data": [
                "vk\t10001\t123123\t333",
                "vk\t10001\t122\t-1",
                "vk\t10001\t124\t-1",
            ],
        },  # 1: VKID, delete list
        {
            "audience_id": 1234,
            "uid_type": "VID",
            "life_time": 33,
            "source_partition_conf": {"audience_name": "A2"},
            "max_target_rows": 2,
            "output_max_rows_per_file": 2,
            "expected_data": [
                "vid\t-10001\t1234\t33",
                "vid\t10001\t1234\t33",
            ],
        },  # 2: VID
        {
            "audience_id": 123,
            "uid_type": "VID",
            "life_time": 33,
            "source_partition_conf": {"audience_name": "A3"},
            "min_score": 0.0,
            "max_score": 1.0,
            "expected_data": [
                "vid\t10001\t123\t33",
            ],
        },  # 3: invalid score
        {
            "audience_id": 123,
            "uid_type": "VID",
            "life_time": 33,
            "source_partition_conf": {"audience_name": "A4"},
            "max_target_rows": 2,
            "expected_data": [
                "vid\t10001\t123\t33",
                "vid\t10003\t123\t33",
            ],
        },  # 4: top records
        {
            "audience_id": 123,
            "uid_type": "VID",
            "life_time": 33,
            "source_partition_conf": {"category": "positive", "audience_name": "A5"},
            "max_target_rows": 5,
            "expected_data": [
                "vid\t10001\t123\t33",
                "vid\t10002\t123\t33",
                "vid\t10003\t123\t33",
            ],
        },  # 5: agg by user
        {
            "audience_id": 123,
            "uid_type": "EMAIL",
            "life_time": 33,
            "source_partition_conf": {"category": "positive", "audience_name": "A6"},
            "min_target_rows": 2,
            "expected_data": [],
            "expected_statuses": [STATUS.FATAL],
        },  # 6: EMAIL with too few rows
        {
            "audience_id": 123,
            "uid_type": "IDFA",
            "life_time": 33,
            "source_partition_conf": {"category": "positive", "audience_name": "A7"},
            "expected_data": [
                "idfa\tAABBCCDD-EEFF-0011-2233-445566778899\t123\t33",
            ],
        },  # 7: IDFA
        {
            "audience_id": 123,
            "uid_type": "GAID",
            "life_time": 33,
            "source_partition_conf": {"category": "positive", "audience_name": "A8"},
            "expected_data": [
                "gaid\t00395485-79fc-479b-a594-ab673bd3c255\t123\t33",
            ],
        },  # 8: GAID
        {
            "audience_id": 99,
            "uid_type": "VKID",
            "life_time": 11,
            "source_partition_conf": {"audience_name": "A9"},
            "expected_data": [
                "vk\t10001\t99\t11",
                "vk\t10002\t99\t11",
                "vk\t10003\t99\t11",
                "vk\t10004\t99\t11",
                "vk\t10005\t99\t11",
            ],
        },  # 9: VKID multiple files
        {
            "audience_id": 1234,
            "uid_type": "VID",
            "life_time": 33,
            "source_partition_conf": {"audience_name": ["A2", "A3"]},  # Union
            "source_where": "score > 0",
            "extra_filters": [
                {
                    "db": TEST_DB,
                    "table": SOURCE_TABLE,
                    "partition_conf": {"audience_name": "A4"},
                    "filter_type": "intersection",
                },
                {
                    "db": TEST_DB,
                    "table": SOURCE_TABLE,
                    "partition_conf": {"audience_name": "A5"},
                    "where": "score > 0.5",
                    "filter_type": "subtraction",
                },
            ],
            "expected_data": [
                "vid\t10004\t1234\t33",
            ],
        },  # 10: VID with extra filters
        {
            "audience_id": 123,
            "uid_type": "EMAIL",
            "life_time": 33,
            "source_partition_conf": {"category": "positive", "audience_name": "A6"},
            "max_dt_diff": 14,
            "expected_data": [],
            "expected_statuses": [STATUS.MISSING_DEPS],
        },  # 11: EMAIL with not found source partitions
    ]

    # test cases, list of (app_config, expected_config)
    return [
        (
            {
                "audience_id": spec["audience_id"],
                "uid_type": spec["uid_type"],
                "life_time": spec["life_time"],
                "exclude_audience_ids": spec.get("exclude_audience_ids", []),
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
                "max_target_rows": spec.get("max_target_rows", 0),
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
                expected_data_files="task.*-{ctid}-RB-{audience_id}-{uid_type}-{target_dt}-*",
                expected_success_file="{success_hdfs_basedir}/{target_dt}/ExportAudienceRbTask_{audience_id}-{uid_type}-{target_dt}.success",
            )
        )
        for i, spec in enumerate(data_specs, 1)
    ]
