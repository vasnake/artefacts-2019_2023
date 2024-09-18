import os

import numpy as np

from prj.apps.utils.testing.defines import TEST_DB, TEST_HDFS_DIR

NAME = "test_combine_universal_features_app"
TARGET_DT = "2020-06-01"
TMP_HDFS_DIR = os.path.join(TEST_HDFS_DIR, NAME)

# flake8: noqa
# fmt: off
# @formatter:off

TARGET_TABLE = "prj_{}_target".format(NAME)
TARGET_SCHEMA = (
    ("uid", "string"),
    ("score", "float"),
    ("score_list", "array<float>"),
    ("score_map", "map<string,float>"),
    ("cat_list", "array<string>"),
    ("feature_name", "string"),
    ("dt", "string"),
    ("uid_type", "string"),
)
TARGET_PARTITION_COLUMNS = ["feature_name", "dt", "uid_type"]

SOURCE_TABLE = "prj_{}_source".format(NAME)
SOURCE_TABLE_SCHEMA = (
    ("uid", "string"),
    ("score", "float"),
    ("score_map", "map<string,float>"),
    ("score_list", "array<float>"),
    ("cat_list", "array<string>"),
    ("audience_name", "string"),
    ("part", "string"),
    ("dt", "string"),
    ("uid_type", "string"),
)
SOURCE_PARTITION_COLUMNS = ["part", "dt", "uid_type"]
SOURCE_DATA = [
    # P1
    {"uid": "10001", "score": 0.91, "part": "P1", "dt": "2020-05-01", "uid_type": "VKID"},
    {"uid": "10002", "score": 0.92, "part": "P1", "dt": "2020-05-01", "uid_type": "VKID"},
    {"uid": "12312", "score": 0.99, "part": "P1", "dt": "2020-05-01", "uid_type": "VKID"},
    # P2
    {"uid": "10001", "score_map": {"1": 0.91}, "part": "P2", "dt": "2020-05-01", "uid_type": "VKID"},
    {"uid": "10002", "score_map": {"92": 0.2, "93": 0.3}, "part": "P2", "dt": "2020-05-01", "uid_type": "VKID"},
    {"uid": "12345", "score_map": {"1": 0.99, "2": 0.99}, "part": "P2", "dt": "2020-05-01", "uid_type": "VKID"},
    {"uid": "10006", "score_map": {"-1": 0.91}, "part": "P2", "dt": "2020-05-01", "uid_type": "VKID"},
    {"uid": "10007", "score_map": {"foo": 0.91}, "part": "P2", "dt": "2020-05-01", "uid_type": "VKID"},
    {"uid": "10008", "score_map": {"4294967296": 0.91}, "part": "P2", "dt": "2020-05-01", "uid_type": "VKID"},
    {"uid": "10009", "score_map": {"1": 0.1, "2": 0.2, "3": 0.3}, "part": "P2", "dt": "2020-05-01", "uid_type": "VKID"},
    # P3
    {"uid": "10001", "score_list": [1.0, 0.91], "part": "P3", "dt": "2020-05-01", "uid_type": "VKID"},
    {"uid": "10002", "score_list": [0.92, 2.0], "part": "P3", "dt": "2020-05-01", "uid_type": "VKID"},
    {"uid": "12345", "score_list": [0.99, 0.9], "part": "P3", "dt": "2020-05-01", "uid_type": "VKID"},
    {"uid": "10006", "score_list": [1.0, 2.0, 3.0], "part": "P3", "dt": "2020-05-01", "uid_type": "VKID"},
    # P4
    {"uid": "10001", "cat_list": ["1", "91"], "part": "P4", "dt": "2020-05-01", "uid_type": "VKID"},
    {"uid": "10002", "cat_list": ["92", "2"], "part": "P4", "dt": "2020-05-01", "uid_type": "VKID"},
    {"uid": "10006", "cat_list": ["2", "-1"], "part": "P4", "dt": "2020-05-01", "uid_type": "VKID"},
    {"uid": "10007", "cat_list": ["3", "4294967296"], "part": "P4", "dt": "2020-05-01", "uid_type": "VKID"},
    {"uid": "10008", "cat_list": ["1", "2", "3"], "part": "P4", "dt": "2020-05-01", "uid_type": "VKID"},
    # P5
    {"uid": "10001", "score": 0.91, "audience_name": "foo", "part": "P5", "dt": "2020-05-01", "uid_type": "VKID",},
    {"uid": "10001", "score": 0.20, "audience_name": "bar", "part": "P5", "dt": "2020-05-01", "uid_type": "VKID",},
    # P6
    {"uid": "10001", "score_map": {"1": 0.1}, "part": "P6", "dt": "2020-05-01", "uid_type": "VKID"},
    {"uid": "10001", "score_map": {"4294967296": 0.2}, "part": "P6", "dt": "2020-05-01", "uid_type": "OKID"},
    {"uid": "10001", "score_map": {"-1": 0.3, "foo": 0.4}, "part": "P6", "dt": "2020-05-01", "uid_type": "HID"},
]
SOURCE_DATA = [
    {name: row.get(name, np.nan if name == "score" else "") for name, _ in SOURCE_TABLE_SCHEMA}
    for row in SOURCE_DATA
]  # adding missing columns

FILTER_TABLE = "prj_{}_filter".format(NAME)
FILTER_SCHEMA = (
    ("id", "string"),
    ("id_type", "string"),
    ("dt", "string"),
)
FILTER_PARTITION_COLUMNS = ["id_type", "dt"]
FILTER_DATA = [
    {"id": "10001", "id_type": "VKID", "dt": "2020-05-01"},
    {"id": "10002", "id_type": "VKID", "dt": "2020-05-01"},
    {"id": "10003", "id_type": "VKID", "dt": "2020-05-01"},
    {"id": "10004", "id_type": "VKID", "dt": "2020-05-01"},
    {"id": "10005", "id_type": "VKID", "dt": "2020-05-01"},
    {"id": "10006", "id_type": "VKID", "dt": "2020-05-01"},
    {"id": "10007", "id_type": "VKID", "dt": "2020-05-01"},
    {"id": "10008", "id_type": "VKID", "dt": "2020-05-01"},
    {"id": "10009", "id_type": "VKID", "dt": "2020-05-01"},
]


def generate_test_data():
    def abs_tmp_path(*dirs):
        return os.path.normpath(os.path.join(TMP_HDFS_DIR, *dirs))

    data_specs = [
        {
            "feature_name": "1_sf",
            "source_partition_conf": {"uid_type": "VKID", "part": "P1"},
            "combine_columns": {
                "uid": "uid",
                "uid_type": "uid_type",
                "feature": "max(score)",
            },
            "filter_db": TEST_DB,
            "filter_table": FILTER_TABLE,
            "filter_partition_conf": {"id_type": "VKID"},
            "filter_columns": {"uid": "id", "uid_type": "id_type"},
            "expected_data": [
                {"uid": "10001", "score": 0.91, "feature_name": "1_sf", "dt": TARGET_DT, "uid_type": "VKID"},
                {"uid": "10002", "score": 0.92, "feature_name": "1_sf", "dt": TARGET_DT, "uid_type": "VKID"},
            ],
        },  # 1 simple float
        {
            "feature_name": "2_map",
            "source_partition_conf": {"uid_type": "VKID", "part": "P2"},
            "combine_columns": {
                "uid": "uid",
                "uid_type": "uid_type",
                "feature": "gavg(score_map)",
            },
            "filter_db": TEST_DB,
            "filter_table": FILTER_TABLE,
            "filter_partition_conf": {"id_type": "VKID"},
            "filter_columns": {"uid": "id", "uid_type": "id_type"},
            "expected_data": [
                {"uid": "10001", "score_map": {"1": 0.91}, "feature_name": "2_map", "dt": TARGET_DT, "uid_type": "VKID"},
                {"uid": "10002", "score_map": {"92": 0.2, "93": 0.3}, "feature_name": "2_map", "dt": TARGET_DT, "uid_type": "VKID"},
                {"uid": "10009", "score_map": {"2": 0.2, "3": 0.3}, "feature_name": "2_map", "dt": TARGET_DT, "uid_type": "VKID"},
            ],
        },  # 2 map<string,float>
        {
            "feature_name": "3_arr",
            "source_partition_conf": {"uid_type": "VKID", "part": "P3"},
            "combine_columns": {
                "uid": "uid",
                "uid_type": "uid_type",
                "feature": "first(score_list)",
            },
            "filter_db": TEST_DB,
            "filter_table": FILTER_TABLE,
            "filter_partition_conf": {"id_type": "VKID"},
            "filter_columns": {"uid": "id", "uid_type": "id_type"},
            "expected_data": [
                {"uid": "10001", "score_list": [1.0, 0.91], "feature_name": "3_arr", "dt": TARGET_DT, "uid_type": "VKID"},
                {"uid": "10002", "score_list": [0.92, 2.0], "feature_name": "3_arr", "dt": TARGET_DT, "uid_type": "VKID"},
                {"uid": "10006", "score_list": [1.0, 2.0], "feature_name": "3_arr", "dt": TARGET_DT, "uid_type": "VKID"},
            ],
        },  # 3 array<float>
        {
            "feature_name": "4_arr",
            "source_partition_conf": {"uid_type": "VKID", "part": "P4"},
            "combine_columns": {
                "uid": "uid",
                "uid_type": "uid_type",
                "feature": "first(cat_list)",
            },
            "expected_data": [
                {"uid": "10001", "cat_list": ["1", "91"], "feature_name": "4_arr", "dt": TARGET_DT, "uid_type": "VKID"},
                {"uid": "10002", "cat_list": ["92", "2"], "feature_name": "4_arr", "dt": TARGET_DT, "uid_type": "VKID"},
                {"uid": "10008", "cat_list": ["1", "2"], "feature_name": "4_arr", "dt": TARGET_DT, "uid_type": "VKID"},
            ],
        },  # 4 array<bigint>
        {
            "feature_name": "5_arr",
            "source_partition_conf": {"part": "P5"},
            "combine_columns": {
                "uid": "uid",
                "uid_type": "uid_type",
                "feature": "map_values_ordered(user_dmdesc.collect(audience_name, score), array('foo', 'bar'))",
            },
            "expected_data": [
                {"uid": "10001", "score_list": [0.91, 0.2], "feature_name": "5_arr", "dt": TARGET_DT, "uid_type": "VKID"},
            ],
        },  # 5 group to map to array
        {
            "feature_hashing": True,
            "feature_name": "6_map",
            "source_partition_conf": {"part": "P6"},
            "combine_columns": {
                "uid": "uid",
                "uid_type": "uid_type",
                "feature": "gmax(score_map)",
            },
            "expected_data": [
                {"uid": "10001", "score_map": {"2622836501": 0.1}, "feature_name": "6_map", "dt": TARGET_DT, "uid_type": "VKID"},
                {"uid": "10001", "score_map": {"1633052041": 0.2}, "feature_name": "6_map", "dt": TARGET_DT, "uid_type": "OKID"},
                {"uid": "10001", "score_map": {"1771093052": 0.3, "176538449": 0.4}, "feature_name": "6_map", "dt": TARGET_DT, "uid_type": "HID"},
            ],
        },  # 6 hash map keys
    ]

    # list of (app_cfg, expected_cfg)
    return [
        (
            {
                "feature_name": spec["feature_name"],
                "feature_hashing": spec.get("feature_hashing", False),
                "target_dt": TARGET_DT,
                "max_dt_diff": spec.get("max_dt_diff", 60),
                "target_db": TEST_DB,
                "target_table": TARGET_TABLE,
                "source_db": TEST_DB,
                "source_table": SOURCE_TABLE,
                "source_partition_conf": spec["source_partition_conf"],
                "combine_columns": spec["combine_columns"],
                "filter_db": spec.get("filter_db", ""),
                "filter_table": spec.get("filter_table", ""),
                "filter_partition_conf": spec.get("filter_partition_conf", {}),
                "filter_columns": spec.get("filter_columns", {}),
                "min_target_rows": spec.get("min_target_rows", 1),
                "max_collection_size": spec.get("max_collection_size", 2),
                "shuffle_partitions": spec.get("shuffle_partitions", 2),
                "ctid": "ctid_{}".format(i),
                "input_urls": [abs_tmp_path("input_{}".format(i))],
                "output_urls": [abs_tmp_path("output_{}".format(i))],
                "status_urls": [abs_tmp_path("status_{}".format(i))],
            },
            spec,
        )
        for i, spec in enumerate(data_specs, 1)
    ]
