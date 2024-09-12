# coding=utf-8
# success test cases

import os

import numpy as np

from prj.apps.utils.testing.defines import TEST_DB, TEST_HDFS_DIR
from prj.apps.utils.control.client.status import STATUS

NAME = "test_join_features_app_success"
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
    # 2022-06-05/VKID
    {"uid": "10013", "score": np.nan, "score_map": np.nan, "score_list": np.nan, "dt": "2022-06-05", "uid_type": "VKID"},
    # 2022-06-06/VKID # wrong array size
    {"uid": "10011", "score": 0.911, "score_map": {"score": 0.912}, "score_list": [0.913], "dt": "2022-06-06", "uid_type": "VKID"},
    {"uid": "10012", "score": np.nan, "score_map": {}, "score_list": [], "dt": "2022-06-06", "uid_type": "VKID"},
    # good array size
    {"uid": "10013", "score": 0.911, "score_map": {"1": np.nan}, "score_list": [np.nan, np.nan], "dt": "2022-06-06", "uid_type": "VKID"},
    {"uid": "10014", "score": np.nan, "score_map": np.nan, "score_list": np.nan, "dt": "2022-06-06", "uid_type": "VKID"},
    {"uid": "10015", "score": np.nan, "score_map": {"1": 0.921, "double_score": 0.922}, "score_list": [0.931, 0.932], "dt": "2022-06-06", "uid_type": "VKID"},
    {"uid": "10016", "score": 0.911, "score_map": {"1": 0.921, "double_score": 0.922}, "score_list": [0.931, 0.932], "dt": "2022-06-06", "uid_type": "VKID"},
    # 2022-06-06/OKID
    {"uid": "10021", "score": np.nan, "score_map": np.nan, "score_list": np.nan, "dt": "2022-06-06", "uid_type": "OKID"},
    {"uid": "10022", "score": np.nan, "score_map": np.nan, "score_list": [0.1, 0.2, 0.3], "dt": "2022-06-06", "uid_type": "OKID"},
    # 2022-06-06/VID
    {"uid": "10031", "score": np.nan, "score_map": np.nan, "score_list": [0.2, 0.1, np.nan], "dt": "2022-06-06", "uid_type": "VID"},
    {"uid": "10032", "score": np.nan, "score_map": np.nan, "score_list": [0.2, 0.4, 0.6], "dt": "2022-06-06", "uid_type": "VID"},
    # 2022-06-06/HID
    {"uid": "90011", "score": np.nan, "score_map": np.nan, "score_list": [0.01], "dt": "2022-06-06", "uid_type": "HID"},
    # 2022-06-01/HID, dt_selection_mode=multiple_any
    {"uid": "90011", "score": np.nan, "score_map": np.nan, "score_list": [np.nan, np.nan, np.nan], "dt": "2022-06-01", "uid_type": "HID"},
]

MD_TABLE = "grinder_{}_md".format(NAME)
MD_SCHEMA = (
    ("id", "int"),
    ("some_value", "int"),
    ("dt", "string"),
    ("uid_type", "string"),
)
MD_PARTITION_COLUMNS = ["dt", "uid_type"]

MD_DATA = [
    {"id": 10011, "some_value": 1, "dt": "2022-06-06", "uid_type": "VKID"},
    {"id": 10012, "some_value": 0, "dt": "2022-06-06", "uid_type": "VKID"},
    {"id": 10013, "some_value": 2, "dt": "2022-06-05", "uid_type": "VKID"},
    {"id": 10014, "some_value": np.nan, "dt": "2022-06-05", "uid_type": "VKID"},
]

MATCHING_TABLE = "grinder_{}_matching".format(NAME)
MATCHING_SCHEMA = (
    ("uid1", "string"),
    ("uid2", "string"),
    ("dt", "string"),
    ("uid1_type", "string"),
    ("uid2_type", "string"),
)
MATCHING_PARTITION_COLUMNS = ["dt", "uid1_type", "uid2_type"]

MATCHING_DATA = [
    {"uid1": "10021", "uid2": "90011", "dt": "2022-06-01", "uid1_type": "OKID", "uid2_type": "HID"},
    {"uid1": "10022", "uid2": "90011", "dt": "2022-06-01", "uid1_type": "OKID", "uid2_type": "HID"},
    {"uid1": "10031", "uid2": "90012", "dt": "2022-06-01", "uid1_type": "VID", "uid2_type": "HID"},
    {"uid1": "10032", "uid2": "90012", "dt": "2022-06-01", "uid1_type": "VID", "uid2_type": "HID"},
    {"uid1": "10011", "uid2": "90012", "dt": "2022-06-01", "uid1_type": "VKID", "uid2_type": "HID"},
    {"uid1": "90011", "uid2": "90031", "dt": "2022-06-01", "uid1_type": "HID", "uid2_type": "HID"},  # loop
]


# test table
def generate_test_data():
    data_specs = [
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
                        "first(score) as score",
                        "first(score_list) as score_list",
                    ],
                    "uids": {"uid_type": "uid"}
                },
            },
            "join_rule": "some_features_source",
            "domains": [
                {
                    "name": "array_domain", "type": "array<float>", "columns": ["some_features_source.*"],
                    "extra_expr": "case when array_domain is null then null else concat(array(0.1), array_domain) end",
                    "comment": "Some features from some source, dense",
                },
                {
                    "name": "array_from_primitives", "type": "array<float>", "columns": ["cast(some_features_source.score as double)"],
                    "extra_expr": "case when array_from_primitives is null then array(0.1) else array_from_primitives end",
                },
            ],
            "strict_check_array": False,
            "expected_data": {
                "columns": ["uid", "array_domain", "array_from_primitives", "dt", "uid_type"],
                "rows": [
                    {"uid": "10011", "array_domain": [0.1, 0.911, 0.913, np.nan], "array_from_primitives": [0.911], "dt": "2022-06-06", "uid_type": "VKID"},
                    {"uid": "10012", "array_domain": np.nan, "array_from_primitives": [0.1], "dt": "2022-06-06", "uid_type": "VKID"},
                    {"uid": "10013", "array_domain": [0.1, 0.911, np.nan, np.nan], "array_from_primitives": [0.911], "dt": "2022-06-06", "uid_type": "VKID"},
                    {"uid": "10014", "array_domain": np.nan, "array_from_primitives": [0.1], "dt": "2022-06-06", "uid_type": "VKID"},
                    {"uid": "10015", "array_domain": [0.1, np.nan, 0.931, 0.932], "array_from_primitives": [0.1], "dt": "2022-06-06", "uid_type": "VKID"},
                    {"uid": "10016", "array_domain": [0.1, 0.911, 0.931, 0.932], "array_from_primitives": [0.911], "dt": "2022-06-06", "uid_type": "VKID"},
                ]
            }
        },  # 1: simple array domain
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
                        "first(cast(score * 10 as int)) as score",
                        "first(case uid when '10015' then 0.911 when '10013' then null else score * 2 end) as double_score",
                        "first(score_map) as score_map",
                        "first(cast(score_map as map<string,double>)) as score_map_2",
                    ],
                    "uids": {"uid_type": "uid"}
                },
            },
            "join_rule": "some_features_source",
            "domains": [
                {
                    "name": "map_domain", "type": "map<string,float>",
                    "columns": ["some_features_source.*"],
                    "comment": "Some sparse features",
                },
                {
                    "name": "map_from_maps", "type": "map<string,float>",
                    "columns": [
                        "cast(some_features_source.score_map as map<string,double>)",
                        "cast(some_features_source.score_map_2 as map<string, double>)"
                    ],
                },
            ],
            "expected_data": {
                "columns": ["uid", "map_domain", "map_from_maps", "dt", "uid_type"],
                "rows": [
                    {"uid": "10011", "map_domain": {"score": 0.912, "double_score": 1.822}, "map_from_maps": {"score": 0.912}, "dt": "2022-06-06", "uid_type": "VKID"},
                    {"uid": "10012", "map_domain": np.nan, "map_from_maps": np.nan, "dt": "2022-06-06", "uid_type": "VKID"},
                    {"uid": "10013", "map_domain": {"score": 9.0}, "map_from_maps": np.nan, "dt": "2022-06-06", "uid_type": "VKID"},
                    {"uid": "10014", "map_domain": np.nan, "map_from_maps": np.nan, "dt": "2022-06-06", "uid_type": "VKID"},
                    {"uid": "10015", "map_domain": {"double_score": 0.922, "1": 0.921}, "map_from_maps": {"double_score": 0.922, "1": 0.921}, "dt": "2022-06-06", "uid_type": "VKID"},
                    {"uid": "10016", "map_domain": {"score": 9.0, "double_score": 0.922, "1": 0.921}, "map_from_maps": {"double_score": 0.922, "1": 0.921}, "dt": "2022-06-06", "uid_type": "VKID"},
                ]
            }
        },  # 2: simple map domain
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
                        "first(cast(score * 10 as int)) as int_score",
                        "first(case uid when '10015' then 0.915 when '10013' then 0.913 else score * 2 end) as double_score",
                        "first(cast(case uid when '10015' then 0.0915 when '10013' then 0.0913 else score * 3 end as float)) as float_score",
                    ],
                    "uids": {"uid_type": "uid"}
                },
            },
            "join_rule": "some_features_source",
            "domains": [
                {
                    "name": "some_prefix", "type": "float", "comment": "Some primitive domain columns group",
                    "columns": [
                        "(some_features_source.int_score * 2) as int_score",
                        "some_features_source.double_score",
                        "some_features_source.float_score"
                    ]
                },
            ],
            "expected_data": {
                "columns": ["uid", "some_prefix_int_score", "some_prefix_double_score", "some_prefix_float_score", "dt", "uid_type"],
                "rows": [
                    {"uid": "10011", "some_prefix_int_score": 18.0, "some_prefix_double_score": 1.822, "some_prefix_float_score": 2.733, "dt": "2022-06-06", "uid_type": "VKID"},
                    {"uid": "10012", "some_prefix_int_score": np.nan, "some_prefix_double_score": np.nan, "some_prefix_float_score": np.nan, "dt": "2022-06-06", "uid_type": "VKID"},
                    {"uid": "10013", "some_prefix_int_score": 18.0, "some_prefix_double_score": 0.913, "some_prefix_float_score": 0.0913, "dt": "2022-06-06", "uid_type": "VKID"},
                    {"uid": "10014", "some_prefix_int_score": np.nan, "some_prefix_double_score": np.nan, "some_prefix_float_score": np.nan, "dt": "2022-06-06", "uid_type": "VKID"},
                    {"uid": "10015", "some_prefix_int_score": np.nan, "some_prefix_double_score": 0.915, "some_prefix_float_score": 0.0915, "dt": "2022-06-06", "uid_type": "VKID"},
                    {"uid": "10016", "some_prefix_int_score": 18.0, "some_prefix_double_score": 1.822, "some_prefix_float_score": 2.733, "dt": "2022-06-06", "uid_type": "VKID"},
                ]
            }
        },  # 3: simple 'prefix' domain
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
                        "first(cast(score * 10 as int)) as score",
                        "first(case uid when '10015' then 0.911 when '10013' then null else score * 2 end) as double_score",
                        "first(score_map) as score_map",
                        "first(cast(score_map as map<string,double>)) as score_map_2",
                        "first(score_list) as score_list",
                    ],
                    "uids": {"uid_type": "uid"}
                },
            },
            "join_rule": "some_features_source",
            "domains": [
                {
                    "name": "map_domain", "type": "map<string,float>",
                    "columns": [
                        "(cast(score as float) / 2) as score",
                        "double_score",
                        "score_map",
                        "score_map_2"
                    ],
                },
                {
                    "name": "array_domain", "type": "array<float>",
                    "columns": ["cast(score as double) as score", "double_score", "score_list"],
                },
            ],
            "strict_check_array": False,
            "expected_data": {
                "columns": ["uid", "map_domain", "array_domain", "dt", "uid_type"],
                "rows": [
                    {"uid": "10011", "map_domain": {"double_score": 1.822, "score": 0.912}, "array_domain": [9.0, 1.822, 0.913, np.nan], "dt": "2022-06-06", "uid_type": "VKID"},
                    {"uid": "10012", "map_domain": np.nan, "array_domain": np.nan, "dt": "2022-06-06", "uid_type": "VKID"},
                    {"uid": "10013", "map_domain": {"score": 4.5}, "array_domain": [9.0, np.nan, np.nan, np.nan], "dt": "2022-06-06", "uid_type": "VKID"},
                    {"uid": "10014", "map_domain": np.nan, "array_domain": np.nan, "dt": "2022-06-06", "uid_type": "VKID"},
                    {"uid": "10015", "map_domain": {"1": 0.921, "double_score": 0.922}, "array_domain": [np.nan, 0.911, 0.931, 0.932], "dt": "2022-06-06", "uid_type": "VKID"},
                    {"uid": "10016", "map_domain": {"1": 0.921, "double_score": 0.922, "score": 4.5}, "array_domain": [9.0, 1.822, 0.931, 0.932], "dt": "2022-06-06", "uid_type": "VKID"},
                ]
            }
        },  # 4: two domains from one source
        {
            "target_dt": "2022-06-06",
            "uid_types": [{
                "output": "VKID",
                "checkpoint_interval": 1,
                "min_target_rows": 5,
            }],
            "sources": {
                "feature_source_1": {
                    "db": TEST_DB,
                    "table": SOURCE_TABLE,
                    "partition_conf": {"uid_type": ["VKID"]},
                    "period": 1,
                    "dt_selection_mode": "single_last",
                    "features": [
                        "first(score_map) as score_map",
                        "first(cast(score_map as map<string,double>)) as score_map_2",
                    ],
                    "uids": {"uid_type": "uid"},
                },
                "feature_source_2": {
                    "db": TEST_DB,
                    "table": SOURCE_TABLE,
                    "partition_conf": {"uid_type": ["VKID"]},
                    "period": 1,
                    "dt_selection_mode": "single_last",
                    "features": [
                        "first(score_list) as score_list",
                        "first(cast(score_list as array<string>)) as score_list_2",
                    ],
                    "uids": {"uid_type": "uid"},
                },
            },
            "join_rule": "feature_source_1 full_outer feature_source_2",
            "domains": [
                {
                    "name": "map_domain", "type": "map<string,float>",
                    "columns": ["score_map", "feature_source_1.score_map_2"],
                },
                {
                    "name": "array_domain", "type": "array<float>",
                    "columns": ["score_list", "feature_source_2.score_list_2"],
                },
            ],
            "strict_check_array": False,
            "expected_data": {
                "columns": ["uid", "map_domain", "array_domain", "dt", "uid_type"],
                "rows": [
                    {"uid": "10011", "map_domain": {"score": 0.912}, "array_domain": [0.913, np.nan, 0.913, np.nan], "dt": "2022-06-06", "uid_type": "VKID"},
                    {"uid": "10012", "map_domain": np.nan, "array_domain": np.nan, "dt": "2022-06-06", "uid_type": "VKID"},
                    {"uid": "10013", "map_domain": np.nan, "array_domain": np.nan, "dt": "2022-06-06", "uid_type": "VKID"},
                    {"uid": "10014", "map_domain": np.nan, "array_domain": np.nan, "dt": "2022-06-06", "uid_type": "VKID"},
                    {"uid": "10015", "map_domain": {"1": 0.921, "double_score": 0.922}, "array_domain": [0.931, 0.932, 0.931, 0.932], "dt": "2022-06-06", "uid_type": "VKID"},
                    {"uid": "10016", "map_domain": {"1": 0.921, "double_score": 0.922}, "array_domain": [0.931, 0.932, 0.931, 0.932], "dt": "2022-06-06", "uid_type": "VKID"},
                ]
            }
        },  # 5: join_rule
        {
            "target_dt": "2022-06-06",
            "uid_types": ["VKID", "OKID"],
            "sources": {
                "some_features_source1": {
                    "db": TEST_DB,
                    "table": SOURCE_TABLE,
                    "partition_conf": {"uid_type": ["VKID", "OKID"]},
                    "period": 1,
                    "dt_selection_mode": "single_last",
                    "features": [
                        "first(cast(score as string)) as feature1",
                    ],
                    "where": "uid_type = 'VKID' or uid = '10021'",
                    "uids": {"uid_type": "uid"}
                },
                "some_features_source2": {
                    "db": TEST_DB,
                    "table": SOURCE_TABLE,
                    "partition_conf": {"uid_type": ["VKID", "OKID"]},
                    "period": 1,
                    "dt_selection_mode": "single_last",
                    "features": [
                        "first(score) as feature2",
                        "first(score_list) as feature3",
                        "first(score * 2) as feature4",
                        "first(score_map) as feature5"
                    ],
                    "where": "uid_type = 'VKID' or uid = '10021'",
                    "uids": {"uid_type": "uid"}
                }
            },
            "join_rule": "some_features_source1 left_outer some_features_source2",
            "domains": [
                {"name": "f1", "type": "double", "columns": ["some_features_source1.*"]},
                {"name": "f2", "type": "array<double>", "columns": ["feature2", "feature3"]},
                {"name": "f3", "type": "map<string,double>", "columns": ["feature4", "feature5"]}
            ],
            "strict_check_array": False,
            "expected_data": {
                "columns": ["uid", "f1_feature1", "f2", "f3", "dt", "uid_type"],
                "rows": [
                    {"uid": "10021", "f1_feature1": np.nan, "f2": np.nan, "f3": np.nan, "dt": "2022-06-06", "uid_type": "OKID"},
                    {"uid": "10011", "f1_feature1": 0.911, "f2": [0.911, 0.913, np.nan], "f3": {"feature4": 1.822, "score": 0.912}, "dt": "2022-06-06", "uid_type": "VKID"},
                    {"uid": "10012", "f1_feature1": np.nan, "f2": np.nan, "f3": np.nan, "dt": "2022-06-06", "uid_type": "VKID"},
                    {"uid": "10013", "f1_feature1": 0.911, "f2": [0.911, np.nan, np.nan], "f3": {"feature4": 1.822}, "dt": "2022-06-06", "uid_type": "VKID"},
                    {"uid": "10014", "f1_feature1": np.nan, "f2": np.nan, "f3": np.nan, "dt": "2022-06-06", "uid_type": "VKID"},
                    {"uid": "10015", "f1_feature1": np.nan, "f2": [np.nan, 0.931, 0.932], "f3": {"1": 0.921, "double_score": 0.922}, "dt": "2022-06-06", "uid_type": "VKID"},
                    {"uid": "10016", "f1_feature1": 0.911, "f2": [0.911, 0.931, 0.932], "f3": {"1": 0.921, "double_score": 0.922, "feature4": 1.822}, "dt": "2022-06-06", "uid_type": "VKID"},
                ]
            }
        },  # 6: complex config w/o matching
        {
            "target_dt": "2022-06-06",
            "uid_types": [{
                "output": "HID",
                "matching": {
                    "input": ["VID", "OKID", "HID"],
                    "db": TEST_DB,
                    "table": MATCHING_TABLE,
                    "max_dt_diff": 5,
                }
            }],
            "sources": {
                "some_features_source": {
                    "db": TEST_DB,
                    "table": SOURCE_TABLE,
                    "period": 33,
                    "dt_selection_mode": "multiple_any",
                    "partition_conf": {"uid_type": ["VID", "OKID", "HID"]},
                    "features": [
                        "gsum(cast(score_list as array<double>)) as f1",
                    ],
                    "uids": {"uid_type": "uid"},
                    "checkpoint": True,
                },
            },
            "join_rule": "some_features_source",
            "domains": [
                {"name": "array_domain", "type": "array<float>", "columns": ["some_features_source.*"]},
            ],
            "strict_check_array": False,
            "expected_data": {
                "columns": ["uid", "array_domain", "dt", "uid_type"],
                "rows": [
                    {"uid": "90031", "array_domain": [0.01, np.nan, np.nan], "dt": "2022-06-06", "uid_type": "HID"},
                    {"uid": "90011", "array_domain": [0.1, 0.2, 0.3], "dt": "2022-06-06", "uid_type": "HID"},
                    {"uid": "90012", "array_domain": [0.4, 0.5, 0.6], "dt": "2022-06-06", "uid_type": "HID"},
                ]
            }
        },  # 7: matching, simple VID,OKID,HID => HID
        {
            "target_dt": "2022-06-06",
            "uid_types": [{
                "output": "HID",
                "matching": {
                    "input": ["VID", "OKID"],
                    "db": TEST_DB,
                    "table": MATCHING_TABLE,
                    "max_dt_diff": 5,
                }
            }],
            "sources": {
                "some_features_source": {
                    "db": TEST_DB,
                    "table": SOURCE_TABLE,
                    "partition_conf": {"uid_type": ["HID", "VID", "OKID"]},
                    "period": 1,
                    "dt_selection_mode": "single_last",
                    "read_as_big_orc": True,
                    "features": [
                        "gsum(score_list) as score_list",
                    ],
                    "uids": {"uid_type": "uid"},
                },
            },
            "join_rule": "some_features_source",
            "domains": [
                {"name": "array_domain", "type": "array<float>", "columns": ["some_features_source.*"]},
            ],
            "strict_check_array": False,
            "expected_data": {
                "columns": ["uid", "array_domain", "dt", "uid_type"],
                "rows": [
                    {"uid": "90011", "array_domain": [0.11, 0.2, 0.3], "dt": "2022-06-06", "uid_type": "HID"},
                    {"uid": "90012", "array_domain": [0.4, 0.5, 0.6], "dt": "2022-06-06", "uid_type": "HID"},
                ]
            }
        },  # 8: matching, complex VID,OKID => HID union HID
        {
            "target_dt": "2022-06-06",
            "uid_types": [{"output": "VKID"}],
            "sources": {
                "another_features_source": {
                    "db": TEST_DB,
                    "table": SOURCE_TABLE,
                    "partition_conf": {"uid_type": ["VKID"]},
                    "period": 2,
                    "dt_selection_mode": "multiple_any",
                    "md_sources": [
                        {
                            "db": TEST_DB,
                            "table": MD_TABLE,
                            "partition_conf": {"uid_type": ["VKID"]},
                            "where": "id is not null and some_value is not null",
                            "select": {
                                "uid": "cast(id as string)",
                                "uid_type": "uid_type",
                                "md_score": "cast(some_value as string)",
                            },
                            "join_conf": {
                                "on": ["uid", "uid_type"],
                                "how": "left",
                                "type": "skewed",
                                "salt_parts": 2,
                            }
                        }
                    ],
                    "features": [
                        "gsum(cast(score_map as map<string,double>)) as f2",
                        "sum(cast(md_score as double)) as f3",
                    ],
                    "uids": {"uid_type": "uid"},
                },
            },
            "join_rule": "another_features_source",
            "domains": [
                {"name": "map_domain", "type": "map<string,float>", "columns": ["another_features_source.*"]},
            ],
            "expected_data": {
                "columns": ["uid", "map_domain", "dt", "uid_type"],
                "rows": [
                    {"uid": "10011", "map_domain": {"f3": 1.0, "score": 0.912}, "dt": "2022-06-06", "uid_type": "VKID"},
                    {"uid": "10012", "map_domain": {"f3": 0.0}, "dt": "2022-06-06", "uid_type": "VKID"},
                    {"uid": "10013", "map_domain": {"f3": 2.0}, "dt": "2022-06-06", "uid_type": "VKID"},
                    {"uid": "10014", "map_domain": np.nan, "dt": "2022-06-06", "uid_type": "VKID"},
                    {"uid": "10015", "map_domain": {"1": 0.921, "double_score": 0.922}, "dt": "2022-06-06", "uid_type": "VKID"},
                    {"uid": "10016", "map_domain": {"1": 0.921, "double_score": 0.922}, "dt": "2022-06-06", "uid_type": "VKID"},
                ]
            }
        },  # 9: md_sources
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
                statuses=spec["expected_data"].get("statuses", [STATUS.SUCCESS])
            ),
        ) for i, spec in enumerate(data_specs, 1)
    ]
