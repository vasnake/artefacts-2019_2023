import os

import numpy as np

from prj.apps.utils.testing.defines import TEST_DB, TEST_HDFS_DIR
from prj.apps.utils.control.client.status import STATUS

NAME = "test_export_ad_features_app"
TARGET_DT = "2021-09-27"
TMP_HDFS_DIR = os.path.join(TEST_HDFS_DIR, NAME)

# flake8: noqa
# fmt: off

SOURCE_TABLE = "grinder_{}_source".format(NAME)
SOURCE_SCHEMA = (
    ("uid", "string"),
    ("score", "float"),
    ("score_list", "array<float>"),
    ("score_map", "map<string,float>"),
    ("cat_list", "array<string>"),
    ("part", "string"),
    ("feature_name", "string"),
    ("dt", "string"),
    ("uid_type", "string"),
)
SOURCE_PARTITION_COLUMNS = ["part", "feature_name", "dt", "uid_type"]
SOURCE_DATA = [
    # P1
    {"uid": "10001", "score": 0.91, "score_list": np.nan, "score_map": np.nan, "cat_list": "", "part": "P1", "feature_name": "some_feature", "dt": "2021-09-27", "uid_type": "BANNER"},
    {"uid": "10002", "score": 0.91, "score_list": np.nan, "score_map": np.nan, "cat_list": "", "part": "P1", "feature_name": "some_feature", "dt": "2021-09-27", "uid_type": "BANNER"},
    {"uid": "10002", "score": 0.93, "score_list": np.nan, "score_map": np.nan, "cat_list": "", "part": "P1", "feature_name": "some_feature", "dt": "2021-09-27", "uid_type": "BANNER"},
    {"uid": "4294967296", "score": 0.91, "score_list": np.nan, "score_map": np.nan, "cat_list": "", "part": "P1", "feature_name": "some_feature", "dt": "2021-09-27", "uid_type": "BANNER"},
    {"uid": "-1", "score": 0.91, "score_list": np.nan, "score_map": np.nan, "cat_list": "", "part": "P1", "feature_name": "some_feature", "dt": "2021-09-27", "uid_type": "BANNER"},
    {"uid": "foo", "score": 0.99, "score_list": np.nan, "score_map": np.nan, "cat_list": "", "part": "P1", "feature_name": "some_feature", "dt": "2021-09-27", "uid_type": "BANNER"},
    # P2
    {"uid": "10001", "score": 0.91, "score_list": np.nan, "score_map": np.nan, "cat_list": "", "part": "P2", "feature_name": "some_feature", "dt": "2021-09-27", "uid_type": "BANNER"},
    {"uid": "10002", "score": 0.92, "score_list": np.nan, "score_map": np.nan, "cat_list": "", "part": "P2", "feature_name": "some_feature", "dt": "2021-09-27", "uid_type": "BANNER"},
    {"uid": "10003", "score": 0.93, "score_list": np.nan, "score_map": np.nan, "cat_list": "", "part": "P2", "feature_name": "some_feature", "dt": "2021-09-27", "uid_type": "BANNER"},
    {"uid": "10004", "score": 0.94, "score_list": np.nan, "score_map": np.nan, "cat_list": "", "part": "P2", "feature_name": "some_feature", "dt": "2021-09-27", "uid_type": "BANNER"},
    {"uid": "4294967295", "score": 0.95, "score_list": np.nan, "score_map": np.nan, "cat_list": "", "part": "P2", "feature_name": "some_feature", "dt": "2021-09-27", "uid_type": "BANNER"},
    # P6
    {"uid": "10001", "score": 0.91, "score_list": [0.11, 0.12], "score_map": {"13": 0.13, "14": 0.14}, "cat_list": ["15", "16"], "part": "P6", "feature_name": "some_feature", "dt": "2021-09-27", "uid_type": "BANNER"},
    {"uid": "10002", "score": 0.92, "score_list": [0.21, 0.22], "score_map": {"24": 0.24, "23": 0.23}, "cat_list": ["25", "26"], "part": "P6", "feature_name": "some_feature", "dt": "2021-09-27", "uid_type": "BANNER"},
    {"uid": "10007", "score": 0.31, "score_list": [0.32, 0.33, 0.34], "score_map": {"35": 0.35, "34": 0.34}, "cat_list": ["36", "37"], "part": "P6", "feature_name": "some_feature", "dt": "2021-09-27", "uid_type": "BANNER"},
    {"uid": "10027", "score": 0.31, "score_list": [], "score_map": {"35": 0.35, "34": 0.34}, "cat_list": ["36", "37"], "part": "P6", "feature_name": "some_feature", "dt": "2021-09-27", "uid_type": "BANNER"},
    {"uid": "10011", "score": 0.31, "score_list": [0.32, 0.33], "score_map": {"36": 0.36, "35": 0.35, "34": 0.34}, "cat_list": ["36", "37"], "part": "P6", "feature_name": "some_feature", "dt": "2021-09-27", "uid_type": "BANNER"},
    {"uid": "10111", "score": 0.31, "score_list": [0.32, 0.33], "score_map": {"0.35": 0.35, "34": 0.34}, "cat_list": ["36", "37"], "part": "P6", "feature_name": "some_feature", "dt": "2021-09-27", "uid_type": "BANNER"},
    {"uid": "10021", "score": 0.31, "score_list": [0.32, 0.33], "score_map": {}, "cat_list": ["36", "37"], "part": "P6", "feature_name": "some_feature", "dt": "2021-09-27", "uid_type": "BANNER"},
    {"uid": "10022", "score": 0.31, "score_list": [0.32, 0.33], "score_map": {"35": 0.35, "34": 0.34}, "cat_list": ["36", "4294967296"], "part": "P6", "feature_name": "some_feature", "dt": "2021-09-27", "uid_type": "BANNER"},
    {"uid": "10013", "score": 0.31, "score_list": [0.32, 0.33], "score_map": {"35": 0.35, "34": 0.34}, "cat_list": ["36", "37", "38"], "part": "P6", "feature_name": "some_feature", "dt": "2021-09-27", "uid_type": "BANNER"},
    {"uid": "10014", "score": 0.31, "score_list": [0.32, 0.33], "score_map": {"35": 0.35, "34": 0.34}, "cat_list": [], "part": "P6", "feature_name": "some_feature", "dt": "2021-09-27", "uid_type": "BANNER"},
]


def generate_test_data():

    data_specs = [
        {
            "source_partition_conf": {"feature_name": "some_feature", "uid_type": "BANNER", "part": "P1"},
            "export_columns": {
                "uid": {
                    "name": "banner",
                    "expr": "uid"
                },
                "features": [{"name": "num_banner_feature_0", "expr": "gavg(score)"}]
            },
            "expected_data": [
                "banner;num_banner_feature_0",
                "10001;0.91",
                "10002;0.92"
            ],
        },  # 1: simple float
        {
            "max_target_rows": 5,
            "output_max_rows_per_file": 3,
            "source_partition_conf": {"part": "P2"},
            "export_columns": {
                "uid": {
                    "name": "banner",
                    "expr": "uid"
                },
                "features": [{"name": "num_banner_feature_0", "expr": "gmax(score)"}]
            },
            "expected_data": [
                "banner;num_banner_feature_0",
                "10001;0.91",
                "10002;0.92",
                "10003;0.93",
                "10004;0.94",
                "4294967295;0.95",
            ],
        },  # 2: produced_rows = max_target_rows
        {
            "source_partition_conf": {"part": "P1"},
            "export_columns": {
                "uid": {
                    "name": "banner",
                    "expr": "uid"
                },
                "features": [{"name": "num_banner_feature_0", "expr": "gavg(score)"}]
            },
            "expected_data": [
                "banner;num_banner_feature_0",
                "10001;0.91",
                "10002;0.92"
            ],
        },  # 3: extra files present
        {
            "source_partition_conf": {"part": "P6"},
            "export_columns": {
                "uid": {
                    "name": "banner",
                    "expr": "uid"
                },
                "features": [
                    {"name": "banner_feature_0", "expr": "first(cast(cat_list as array<long>))"},
                    {"name": "num_banner_feature_0", "expr": "gmin(score)"},
                    {"name": "num_banner_feature_1", "expr": "gsum(score_list)"},
                    {"name": "num_banner_feature_2", "expr": "gmax(score_map)"},
                ]
            },
            "expected_data": [
                "banner;banner_feature_0;num_banner_feature_0;num_banner_feature_1;num_banner_feature_2",
                "10001;15,16;0.91;0.11,0.12;14:0.14,13:0.13",
                "10002;25,26;0.92;0.21,0.22;24:0.24,23:0.23"
            ],
        },  # 4: more than one feature in file; float, array<float>, map<string,float>, array<string>
        {
            "max_target_rows": 4,
            "source_partition_conf": {"part": "P2"},
            "export_columns": {
                "uid": {
                    "name": "banner",
                    "expr": "uid"
                },
                "features": [{"name": "num_banner_feature_0", "expr": "most_freq(score)"}]
            },
            "expected_statuses": [STATUS.FATAL],
        },  # 5: FATAL, too many rows
    ]

    # list of (app_config, expected)
    return [
        (
            {
                "target_dt": TARGET_DT,
                "target_hdfs_basedir": os.path.join(TMP_HDFS_DIR, "result", ""),
                "features_subdir": "test_{}".format(i),
                "source_db": TEST_DB,
                "source_table": SOURCE_TABLE,
                "source_partition_conf": spec["source_partition_conf"],
                "max_dt_diff": spec.get("max_dt_diff"),
                "export_columns": spec["export_columns"],
                "min_target_rows": spec.get("min_target_rows", 1),
                "max_target_rows": spec.get("max_target_rows", 10),
                "output_max_rows_per_file": spec.get("output_max_rows_per_file", 10),
                "max_collection_size": spec.get("max_collection_size", 2),
                "shuffle_partitions": spec.get("shuffle_partitions", 4),
                "ctid": "ctid_{}".format(i),
                "input_urls": [os.path.join(TMP_HDFS_DIR, "input_{}".format(i))],
                "output_urls": [os.path.join(TMP_HDFS_DIR, "output_{}".format(i))],
                "status_urls": [os.path.join(TMP_HDFS_DIR, "status_{}".format(i))],
            },
            dict(
                spec,
                expected_statuses=spec.get("expected_statuses", [STATUS.SUCCESS]),
                expected_target_dir="{target_hdfs_basedir}/{features_subdir}/{target_dt}/",
            )
        )
        for i, spec in enumerate(data_specs, 1)
    ]
