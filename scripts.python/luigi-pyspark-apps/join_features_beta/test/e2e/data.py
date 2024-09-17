# coding=utf-8

import os

import numpy as np

from prj.apps.utils.testing.defines import TEST_DB, TEST_HDFS_DIR
from prj.apps.utils.control.client.const import STATUS

NAME = "test_join_features_app"
TMP_HDFS_DIR = os.path.join(TEST_HDFS_DIR, NAME)

# flake8: noqa
# fmt: off
# @formatter:off

SOURCE_TABLE = "prj_{}_source".format(NAME)
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
    {"uid": "10013", "score": 0.911, "score_map": {"1": np.nan}, "score_list": ["", ""], "dt": "2022-06-06", "uid_type": "VKID"},
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
    {"uid": "90011", "score": np.nan, "score_map": np.nan, "score_list": np.nan, "dt": "2022-06-06", "uid_type": "HID"},
    # 2022-06-01/HID, match_mode=any
    {"uid": "90011", "score": np.nan, "score_map": np.nan, "score_list": [np.nan, np.nan, np.nan], "dt": "2022-06-01", "uid_type": "HID"},
]

MD_TABLE = "prj_{}_md".format(NAME)
MD_SCHEMA = (
    ("id", "int"),
    ("some_value", "int"),
    ("dt", "string"),
    ("uid_type", "string"),
)
MD_PARTITION_COLUMNS = ["dt", "uid_type"]

MD_DATA = [
    {"id": 10011, "some_value": 1, "dt": "2022-06-01", "uid_type": "VKID"},
    {"id": 10012, "some_value": 0, "dt": "2022-06-01", "uid_type": "VKID"},
    {"id": 10013, "some_value": np.nan, "dt": "2022-06-01", "uid_type": "VKID"},
]

MATCHING_TABLE = "prj_{}_matching".format(NAME)
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
    {"uid1": "90011", "uid2": "90031", "dt": "2022-06-01", "uid1_type": "HID", "uid2_type": "HID"},  # loop in mapping graph
]


def generate_test_data():
    data_specs = [
        {
            "target_table": "prj_{}_target_1".format(NAME),
            "target_dt": "2022-06-06",
            "uid_types": ["VKID"],
            "feature_sources": {
                "some_features_source": {
                    "db": TEST_DB,
                    "table": SOURCE_TABLE,
                    "partition_conf": {"uid_type": ["VKID"]},
                    "features": [
                        {"score": "score"},
                        {"score_list": "score_list"},
                    ]
                },
            },
            "join_rule": "some_features_source",
            "domains": [
                {"name": "array_domain", "type": "array<float>", "source": "some_features_source"},
            ],
            "strict_check_array": False,
            "expected_data": {
                "columns": ["uid", "array_domain", "dt", "uid_type"],
                "rows": [
                    {"uid": "10011", "array_domain": [0.911, 0.913, np.nan], "dt": "2022-06-06", "uid_type": "VKID"},
                    {"uid": "10012", "array_domain": np.nan, "dt": "2022-06-06", "uid_type": "VKID"},
                    {"uid": "10013", "array_domain": [0.911, np.nan, np.nan], "dt": "2022-06-06", "uid_type": "VKID"},
                    {"uid": "10014", "array_domain": np.nan, "dt": "2022-06-06", "uid_type": "VKID"},
                    {"uid": "10015", "array_domain": [np.nan, 0.931, 0.932], "dt": "2022-06-06", "uid_type": "VKID"},
                    {"uid": "10016", "array_domain": [0.911, 0.931, 0.932], "dt": "2022-06-06", "uid_type": "VKID"},
                ]
            }
        },  # part 1, simple array domain
        {
            "target_table": "prj_{}_target_2".format(NAME),
            "target_dt": "2022-06-06",
            "uid_types": ["VKID"],
            "feature_sources": {
                "some_features_source": {
                    "db": TEST_DB,
                    "table": SOURCE_TABLE,
                    "partition_conf": {"uid_type": ["VKID"]},
                    "features": [
                        {"score": "cast(score * 10 as int)"},
                        {"double_score": "case uid when '10015' then 0.911 when '10013' then null else score * 2 end"},
                        {"score_map": "score_map"},
                        {"score_map_2": "cast(score_map as map<string,double>)"},
                    ]
                },
            },
            "join_rule": "some_features_source",
            "domains": [
                {"name": "map_domain", "type": "map<string,float>", "source": "some_features_source"},
            ],
            "expected_data": {
                "columns": ["uid", "map_domain", "dt", "uid_type"],
                "rows": [
                    {"uid": "10011", "map_domain": {"score": 0.912, "double_score": 1.822}, "dt": "2022-06-06", "uid_type": "VKID"},
                    {"uid": "10012", "map_domain": np.nan, "dt": "2022-06-06", "uid_type": "VKID"},
                    {"uid": "10013", "map_domain": {"score": 9.0}, "dt": "2022-06-06", "uid_type": "VKID"},
                    {"uid": "10014", "map_domain": np.nan, "dt": "2022-06-06", "uid_type": "VKID"},
                    {"uid": "10015", "map_domain": {"double_score": 0.922, "1": 0.921}, "dt": "2022-06-06", "uid_type": "VKID"},
                    {"uid": "10016", "map_domain": {"score": 9.0, "double_score": 0.922, "1": 0.921}, "dt": "2022-06-06", "uid_type": "VKID"},
                ]
            }
        },  # part 2, simple map domain
        {
            "target_table": "prj_{}_target_3".format(NAME),
            "target_dt": "2022-06-06",
            "uid_types": ["VKID"],
            "feature_sources": {
                "some_features_source": {
                    "db": TEST_DB,
                    "table": SOURCE_TABLE,
                    "partition_conf": {"uid_type": ["VKID"]},
                    "features": [
                        {"int_score": "cast(score * 10 as int)"},
                        {"double_score": "case uid when '10015' then 0.915 when '10013' then 0.913 else score * 2 end"},
                        {"float_score": "cast(case uid when '10015' then 0.0915 when '10013' then 0.0913 else score * 3 end as float)"},
                    ]
                },
            },
            "join_rule": "some_features_source",
            "domains": [
                {"name": "some_prefix", "type": "float", "source": "some_features_source", "columns": ["(int_score * 2) as int_score", "double_score", "float_score"]},
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
        },  # part 3, simple 'prefix' domain
        {
            "target_table": "prj_{}_target_4".format(NAME),
            "target_dt": "2022-06-06",
            "uid_types": ["VKID"],
            "feature_sources": {
                "some_features_source": {
                    "db": TEST_DB,
                    "table": SOURCE_TABLE,
                    "partition_conf": {"uid_type": ["VKID"]},
                    "features": [
                        {"score": "cast(score * 10 as int)"},
                        {"double_score": "case uid when '10015' then 0.911 when '10013' then null else score * 2 end"},
                        {"score_map": "score_map"},
                        {"score_map_2": "cast(score_map as map<string,double>)"},
                        {"score_list": "score_list"},
                    ]
                },
            },
            "join_rule": "some_features_source",
            "domains": [
                {
                    "name": "map_domain", "type": "map<string,float>", "source": "some_features_source",
                    "columns": ["(cast(score as float) / 2) as score", "double_score", "score_map", "score_map_2"],
                },
                {
                    "name": "array_domain", "type": "array<float>", "source": "some_features_source",
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
        },  # part 4, two domains from one source
        {
            "target_table": "prj_{}_target_5".format(NAME),
            "target_dt": "2022-06-06",
            "uid_types": [{
                "output": "VKID",
                "checkpoint_interval": 1,
                "min_target_rows": 5,
            }],
            "feature_sources": {
                "feature_source_1": {
                    "db": TEST_DB,
                    "table": SOURCE_TABLE,
                    "partition_conf": {"uid_type": ["VKID"]},
                    "features": [
                        {"score_map": "score_map"},
                        {"score_map_2": "cast(score_map as map<string,double>)"},
                    ],
                },
                "feature_source_2": {
                    "db": TEST_DB,
                    "table": SOURCE_TABLE,
                    "partition_conf": {"uid_type": ["VKID"]},
                    "features": [
                        {"score_list": "score_list"},
                        {"score_list_2": "cast(score_list as array<string>)"},
                    ],
                },
            },
            "join_rule": "feature_source_1 full_outer feature_source_2",
            "domains": [
                {
                    "name": "map_domain", "type": "map<string,float>", "source": "feature_source_1",
                    "columns": ["score_map", "score_map_2"],
                },
                {
                    "name": "array_domain", "type": "array<float>", "source": "feature_source_2",
                    "columns": ["score_list", "score_list_2"],
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
        },  # part 5, join_rule
        {
            "target_table": "prj_{}_target_6".format(NAME),
            "target_dt": "2022-06-06",
            "uid_types": ["VKID", "OKID"],
            "feature_sources": {
                "some_features_source1": {
                    "db": TEST_DB,
                    "table": SOURCE_TABLE,
                    "partition_conf": {"uid_type": ["VKID", "OKID"]},
                    "features": [
                        {"feature1": "cast(score as string)"}
                    ],
                    "where": "uid_type = 'VKID' or uid = '10021'",
                },
                "some_features_source2": {
                    "db": TEST_DB,
                    "table": SOURCE_TABLE,
                    "partition_conf": {"uid_type": ["VKID", "OKID"]},
                    "features": [
                        {"feature2": "score"},
                        {"feature3": "score_list"},
                        {"feature4": "score * 2"},
                        {"feature5": "score_map"}
                    ],
                    "where": "uid_type = 'VKID' or uid = '10021'",
                }
            },
            "join_rule": "some_features_source1 left_outer some_features_source2",
            "domains": [
                {"name": "primitive_domain", "type": "double", "source": "some_features_source1"},
                {"name": "array_domain", "type": "array<double>", "source": "some_features_source2", "columns": ["feature2", "feature3"]},
                {"name": "map_domain", "type": "map<string,double>", "source": "some_features_source2", "columns": ["feature4", "feature5"]}
            ],
            "final_columns": [
                "cast(primitive_domain_feature1 as float) as f1",
                "cast(array_domain as array<float>) as f2",
                "cast(map_domain as map<string,float>) as f3"
            ],
            "strict_check_array": False,
            "expected_data": {
                "columns": ["uid", "f1", "f2", "f3", "dt", "uid_type"],
                "rows": [
                    {"uid": "10021", "f1": np.nan, "f2": np.nan, "f3": np.nan, "dt": "2022-06-06", "uid_type": "OKID"},
                    {"uid": "10011", "f1": 0.911, "f2": [0.911, 0.913, np.nan], "f3": {"feature4": 1.822, "score": 0.912}, "dt": "2022-06-06", "uid_type": "VKID"},
                    {"uid": "10012", "f1": np.nan, "f2": np.nan, "f3": np.nan, "dt": "2022-06-06", "uid_type": "VKID"},
                    {"uid": "10013", "f1": 0.911, "f2": [0.911, np.nan, np.nan], "f3": {"feature4": 1.822}, "dt": "2022-06-06", "uid_type": "VKID"},
                    {"uid": "10014", "f1": np.nan, "f2": np.nan, "f3": np.nan, "dt": "2022-06-06", "uid_type": "VKID"},
                    {"uid": "10015", "f1": np.nan, "f2": [np.nan, 0.931, 0.932], "f3": {"1": 0.921, "double_score": 0.922}, "dt": "2022-06-06", "uid_type": "VKID"},
                    {"uid": "10016", "f1": 0.911, "f2": [0.911, 0.931, 0.932], "f3": {"1": 0.921, "double_score": 0.922, "feature4": 1.822}, "dt": "2022-06-06", "uid_type": "VKID"},
                ]
            }
        },  # part 6, complex config w/o matching
        {
            "target_table": "prj_{}_target_7".format(NAME),
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
            "feature_sources": {
                "some_features_source": {
                    "db": TEST_DB,
                    "table": SOURCE_TABLE,
                    "period": 33,
                    "dt_selection_mode": "multiple_any",
                    "partition_conf": {"uid_type": ["VID", "OKID", "HID"]},
                    "features": [{"f1": "gsum(cast(score_list as array<double>))"}],
                    "agg": True,
                    "checkpoint": True,
                },
            },
            "join_rule": "some_features_source",
            "domains": [
                {"name": "array_domain", "type": "array<float>", "source": "some_features_source"},
            ],
            "strict_check_array": True,
            "expected_data": {
                "columns": ["uid", "array_domain", "dt", "uid_type"],
                "rows": [
                    {"uid": "90031", "array_domain": np.nan, "dt": "2022-06-06", "uid_type": "HID"},
                    {"uid": "90011", "array_domain": [0.1, 0.2, 0.3], "dt": "2022-06-06", "uid_type": "HID"},
                    {"uid": "90012", "array_domain": [0.4, 0.5, 0.6], "dt": "2022-06-06", "uid_type": "HID"},
                ]
            }
        },  # part 7, matching
        {
            "target_table": "prj_{}_target_8".format(NAME),
            "target_dt": "2022-06-06",
            "uid_types": [{"output": "VKID"}],
            "feature_sources": {
                "another_features_source": {
                    "db": TEST_DB,
                    "table": SOURCE_TABLE,
                    "partition_conf": {"uid_type": ["VKID"]},
                    "md_sources": [
                        {
                            "db": TEST_DB,
                            "table": MD_TABLE,
                            "partition_conf": {"uid_type": ["VKID"]},
                            "max_dt_diff": 33,
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
                        {"f2": "cast(score_map as map<string,double>)"},
                        {"f3": "cast(md_score as double)"}
                    ],
                },
            },
            "join_rule": "another_features_source",
            "domains": [
                {"name": "map_domain", "type": "map<string,float>", "source": "another_features_source"},
            ],
            "expected_data": {
                "columns": ["uid", "map_domain", "dt", "uid_type"],
                "rows": [
                    {"uid": "10011", "map_domain": {"f3": 1.0, "score": 0.912}, "dt": "2022-06-06", "uid_type": "VKID"},
                    {"uid": "10012", "map_domain": {"f3": 0.0}, "dt": "2022-06-06", "uid_type": "VKID"},
                    {"uid": "10013", "map_domain": np.nan, "dt": "2022-06-06", "uid_type": "VKID"},
                    {"uid": "10014", "map_domain": np.nan, "dt": "2022-06-06", "uid_type": "VKID"},
                    {"uid": "10015", "map_domain": {"1": 0.921, "double_score": 0.922}, "dt": "2022-06-06", "uid_type": "VKID"},
                    {"uid": "10016", "map_domain": {"1": 0.921, "double_score": 0.922}, "dt": "2022-06-06", "uid_type": "VKID"},
                ]
            }
        },  # part 8, md_sources
        {
            "target_table": "prj_{}_target_9".format(NAME),
            "target_dt": "2022-07-06",
            "uid_types": ["VKID"],
            "feature_sources": {
                "some_features_source": {
                    "db": TEST_DB,
                    "table": SOURCE_TABLE,
                    "partition_conf": {"uid_type": ["VKID"]},
                    "period": 1,
                    "features": [{"score_list": "score_list"}]
                },
            },
            "join_rule": "some_features_source",
            "domains": [
                {"name": "array_domain", "type": "array<float>", "source": "some_features_source"},
            ],
            "expected_data": {
                "statuses": [STATUS.MISSING_DEPS],
            }
        },  # part 9, error: missing dependencies
        {
            "target_table": "prj_{}_target_10".format(NAME),
            "target_dt": "2022-06-06",
            "uid_types": ["VKID"],
            "feature_sources": {
                "some_features_source": {
                    "db": TEST_DB,
                    "table": SOURCE_TABLE,
                    "partition_conf": {"uid_type": ["VKID"]},
                    "features": [{"score_list": "build_array_udf_unknown(a, b, c)"}]
                },
            },
            "join_rule": "some_features_source",
            "domains": [
                {"name": "array_domain", "type": "array<float>", "source": "some_features_source"},
            ],
            "expected_data": {
                "exception": Exception,
                "statuses": [STATUS.FATAL],
            }
        },  # part 10, error: invalid config
        {
            "target_table": "prj_{}_target_11".format(NAME),
            "target_dt": "2022-06-06",
            "uid_types": [{
                    "output": "VKID",
                    "min_target_rows": 55,
            }],
            "feature_sources": {
                "some_features_source": {
                    "db": TEST_DB,
                    "table": SOURCE_TABLE,
                    "partition_conf": {"uid_type": ["VKID"]},
                    "where": "score is not NULL and size(score_list) = 2",
                    "features": [
                        {"score": "score"},
                        {"score_list": "score_list"},
                    ],
                },
            },
            "join_rule": "some_features_source",
            "domains": [
                {"name": "array_domain", "type": "array<float>", "source": "some_features_source"},
            ],
            "strict_check_array": True,
            "expected_data": {
                "statuses": [STATUS.FATAL],
            }
        },  # part 11, error: not enough rows
        {
            "target_table": "prj_{}_target_12".format(NAME),
            "target_dt": "2022-06-06",
            "uid_types": [{
                "output": "HID",
                "matching": {
                    "input": ["HID"],
                    "db": TEST_DB,
                    "table": MATCHING_TABLE,
                    "max_dt_diff": 5,
                }
            }],
            "feature_sources": {
                "some_features_source": {
                    "db": TEST_DB,
                    "table": SOURCE_TABLE,
                    "partition_conf": {"uid_type": "HID"},
                    "read_as_big_orc": True,
                    "features": [{"score_list": "gsum(score_list)"}],
                    "agg": True,
                },
            },
            "join_rule": "some_features_source",
            "domains": [
                {"name": "array_domain", "type": "array<float>", "source": "some_features_source"},
            ],
            "strict_check_array": False,
            "expected_data": {
                "columns": ["uid", "array_domain", "dt", "uid_type"],
                "rows": [
                    {"uid": "90031", "array_domain": np.nan, "dt": "2022-06-06", "uid_type": "HID"},
                ]
            }
        },  # part 12, HID => HID matching
    ][8:12]

    # data_specs = data_specs[0:8] + data_specs[11:12]

    # list of (app_cfg, expect_cfg)
    return [
        (
            {
                "target_dt": spec["target_dt"],
                "target_db": TEST_DB,
                "target_table": spec["target_table"],
                "uid_types": spec["uid_types"],
                "feature_sources": spec["feature_sources"],
                "join_rule": spec["join_rule"],
                "domains": spec["domains"],
                "final_columns": spec.get("final_columns"),
                "strict_check_array": spec.get("strict_check_array", True),
                "ctid": "ctid_{}".format(i),
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
