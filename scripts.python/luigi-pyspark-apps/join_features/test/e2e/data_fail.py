# fail test cases

import os

import numpy as np

from prj.apps.utils.testing.defines import TEST_DB, TEST_HDFS_DIR
from prj.apps.utils.control.client.status import STATUS

NAME = "test_join_features_app_fail"
TMP_HDFS_DIR = os.path.join(TEST_HDFS_DIR, NAME)

# flake8: noqa
# fmt: off
# @formatter:off

SOURCE_TABLE = "grinder_{}_source".format(NAME)
SOURCE_SCHEMA = (
    ("uid", "string"),
    ("score", "float"),
    ("score_map", "map<string,float>"),
    ("score_list", "array<float>"),
    ("dt", "string"),
    ("uid_type", "string"),
)
SOURCE_PARTITION_COLUMNS = ["dt", "uid_type"]

SOURCE_DATA = [
    # 2022-06-06/VKID # wrong array size
    {"uid": "10011", "score": 0.911, "score_map": {"score": 0.912}, "score_list": [0.913], "dt": "2022-06-06", "uid_type": "VKID"},
    {"uid": "10012", "score": np.nan, "score_map": {}, "score_list": [], "dt": "2022-06-06", "uid_type": "VKID"},
    # good array size
    {"uid": "10013", "score": 0.911, "score_map": {"1": np.nan}, "score_list": [np.nan, np.nan], "dt": "2022-06-06", "uid_type": "VKID"},
    {"uid": "10014", "score": np.nan, "score_map": np.nan, "score_list": np.nan, "dt": "2022-06-06", "uid_type": "VKID"},
    {"uid": "10015", "score": np.nan, "score_map": {"1": 0.921, "double_score": 0.922}, "score_list": [0.931, 0.932], "dt": "2022-06-06", "uid_type": "VKID"},
    {"uid": "10016", "score": 0.911, "score_map": {"1": 0.921, "double_score": 0.922}, "score_list": [0.931, 0.932], "dt": "2022-06-06", "uid_type": "VKID"},
]


# tests table
def generate_test_data():
    data_specs = [
        {
            "target_dt": "2022-07-06",
            "uid_types": ["VKID"],
            "sources": {
                "some_features_source": {
                    "db": TEST_DB,
                    "table": SOURCE_TABLE,
                    "partition_conf": {"uid_type": ["VKID"]},
                    "period": 1,
                    "dt_selection_mode": "single_last",
                    "features": [
                        "first(score_list) as score_list",
                    ],
                    "uids": {"uid_type": "uid"},
                },
            },
            "join_rule": "some_features_source",
            "domains": [
                {"name": "array_domain", "type": "array<float>", "columns": ["some_features_source.*"]},
            ],
            "expected_data": {
                "statuses": [STATUS.MISSING_DEPS],
            }
        },  # 1: missing dependencies
        {
            "target_dt": "2022-06-06",
            "uid_types": ["VKID"],
            "sources": {
                "some_features_source": {
                    "db": TEST_DB,
                    "table": SOURCE_TABLE,
                    "partition_conf": {"uid_type": ["VKID"]},
                    "period": 1,
                    "dt_selection_mode": "single_last",
                    "features": [
                        "unknown_udaf(score_list) as score_list",
                    ],
                    "uids": {"uid_type": "uid"},
                },
            },
            "join_rule": "some_features_source",
            "domains": [
                {"name": "array_domain", "type": "array<float>", "columns": ["some_features_source.*"]},
            ],
            "expected_data": {
                "statuses": [STATUS.FATAL],
            }
        },  # 2: invalid config
        {
            "target_dt": "2022-06-06",
            "uid_types": [{
                "output": "VKID",
                "min_target_rows": 55,
            }],
            "sources": {
                "some_features_source": {
                    "db": TEST_DB,
                    "table": SOURCE_TABLE,
                    "partition_conf": {"uid_type": ["VKID"]},
                    "period": 1,
                    "dt_selection_mode": "single_last",
                    "where": "score is not NULL and size(score_list) = 2",
                    "features": [
                        "first(score) as score",
                        "first(score_list) as score_list",
                    ],
                    "uids": {"uid_type": "uid"},
                },
            },
            "join_rule": "some_features_source",
            "domains": [
                {"name": "array_domain", "type": "array<float>", "columns": ["some_features_source.*"]},
            ],
            "strict_check_array": True,
            "expected_data": {
                "statuses": [STATUS.FATAL],
            }
        },  # 3: not enough rows
    ]

    # list of (app_config, check_data)
    return [
        (
            {
                "target_dt": spec["target_dt"],
                "target_db": TEST_DB,
                "target_table": "grinder_{}_target_{}".format(NAME, i),
                "uid_types": spec["uid_types"],
                "sources": spec["sources"],
                "join_rule": spec["join_rule"],
                "domains": spec["domains"],
                "strict_check_array": spec.get("strict_check_array", True),
                "nan_inf_to_null": spec.get("nan_inf_to_null", True),
                "ctid": "ctid_{}_{}".format(NAME, i),
                "input_urls": [os.path.join(TMP_HDFS_DIR, "input_{}".format(i))],
                "output_urls": [os.path.join(TMP_HDFS_DIR, "output_{}".format(i))],
                "status_urls": [os.path.join(TMP_HDFS_DIR, "status_{}".format(i))],
            },
            dict(
                spec["expected_data"],
                statuses=spec["expected_data"]["statuses"]
            ),
        ) for i, spec in enumerate(data_specs, 1)
    ]
