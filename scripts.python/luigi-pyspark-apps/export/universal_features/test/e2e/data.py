import os

import numpy as np

from prj.apps.utils.testing.defines import TEST_DB, TEST_HDFS_DIR

NAME = "test_export_universal_features_app"
TARGET_DT = "2020-06-01"
TMP_HDFS_DIR = os.path.join(TEST_HDFS_DIR, NAME)


# flake8: noqa
def generate_test_data():

    # fmt: off
    data_specs = [
        {
            "source_partition_conf": {"uid_type": ["VKID", "HID"]},
            "export_columns": {
                "uid": "uid",
                "uid_type": "uid_type",
                "features": [{"name": "feature_foo", "expr": "gavg(score)"}]
            },
            "output_max_rows_per_file": 1,
            "source": {
                "data": [
                    {"uid": "10001", "score": {"1": 0.91}, "dt": "2020-05-01", "uid_type": "VKID"},
                    {"uid": "10002", "score": {"92": 0.2, "93": 0.3}, "dt": "2020-05-01", "uid_type": "VKID"},
                    {"uid": "10003", "score": {"92": 0.2, "foo": np.nan}, "dt": "2020-05-01", "uid_type": "VKID"},
                    {"uid": "10004", "score": np.nan, "dt": "2020-05-01", "uid_type": "VKID"},
                    {"uid": "10005", "score": {"-1": 0.91}, "dt": "2020-05-01", "uid_type": "VKID"},
                    {"uid": "10006", "score": {"4294967296": 0.91}, "dt": "2020-05-01", "uid_type": "VKID"},
                    {"uid": "10007", "score": {"1": 0.1, "2": 0.2, "3": 0.3}, "dt": "2020-05-01", "uid_type": "VKID"},
                    {"uid": "9223372036854775807", "score": {"2": 0.98}, "dt": "2020-05-01", "uid_type": "HID"},
                    {"uid": "-9223372036854775808", "score": {"3": 0.99}, "dt": "2020-05-01", "uid_type": "HID"},
                ],
                "schema": (
                    ("uid", "string"),
                    ("score", "map<string,float>"),
                    ("dt", "string"),
                    ("uid_type", "string")
                ),
                "partition_columns": ["dt", "uid_type"],
            },
            "expected_data": [
                "user;num:feature_foo",
                "vk:10001;1:0.91",
                "vk:10002;92:0.2,93:0.3",
                "hid:9223372036854775807;2:0.98",
                "hid:-9223372036854775808;3:0.99",
            ],
        },  # 1: map<string,float>
        {
            "source_partition_conf": {"uid_type": "VKID"},
            "export_columns": {
                "uid": "uid",
                "uid_type": "uid_type",
                "features": [{"name": "score", "expr": "first(score)"}]
            },
            "source": {
                "data": [
                    {"uid": "10001", "score": [1.0, 0.91], "dt": "2020-05-01", "uid_type": "VKID"},
                    {"uid": "10002", "score": [0.92, 2.0], "dt": "2020-05-01", "uid_type": "VKID"},
                    {"uid": "10003", "score": [0.92, np.nan], "dt": "2020-05-01", "uid_type": "VKID"},
                    {"uid": "10004", "score": [0.92, float("nan")], "dt": "2020-05-01", "uid_type": "VKID"},
                    {"uid": "10005", "score": np.nan, "dt": "2020-05-01", "uid_type": "VKID"},
                    {"uid": "10006", "score": [1.0, 2.0, 3.0], "dt": "2020-05-01", "uid_type": "VKID"},
                ],
                "schema": (
                    ("uid", "string"),
                    ("score", "array<float>"),
                    ("dt", "string"),
                    ("uid_type", "string")
                ),
                "partition_columns": ["dt", "uid_type"],
            },
            "expected_data": [
                "user;num:score",
                "vk:10001;1.0,0.91",
                "vk:10002;0.92,2.0",
            ],
        },  # 2: array<float>
        {
            "source_partition_conf": {"uid_type": "VKID"},
            "export_columns": {
                "uid": "uid",
                "uid_type": "uid_type",
                "features": [{"name": "score", "expr": "first(score)"}]
            },
            "source": {
                "data": [
                    {"uid": "10001", "score": [1, 91], "dt": "2020-05-01", "uid_type": "VKID"},
                    {"uid": "10002", "score": [92, 2], "dt": "2020-05-01", "uid_type": "VKID"},
                    {"uid": "10003", "score": [92, -999], "dt": "2020-05-01", "uid_type": "VKID"},
                    {"uid": "10005", "score": -999, "dt": "2020-05-01", "uid_type": "VKID"},
                    {"uid": "10006", "score": [2, -1], "dt": "2020-05-01", "uid_type": "VKID"},
                    {"uid": "10007", "score": [3, 4294967296], "dt": "2020-05-01", "uid_type": "VKID"},
                    {"uid": "10008", "score": [1, 2, 3], "dt": "2020-05-01", "uid_type": "VKID"},
                ],
                "schema": (
                    ("uid", "string"),
                    ("score", "array<bigint>"),
                    ("dt", "string"),
                    ("uid_type", "string")
                ),
                "partition_columns": ["dt", "uid_type"],
            },
            "expected_data": [
                "user;cat:score",
                "vk:10001;1,91",
                "vk:10002;92,2",
            ],
        },  # 3: array<bigint>
        {
            "source_partition_conf": {"uid_type": "VKID"},
            "export_columns": {
                "uid": "uid",
                "uid_type": "uid_type",
                "features": [{"name": "score", "expr": "max(score)"}]
            },
            "source": {
                "data": [
                    {"uid": "10001", "score": "91", "dt": "2020-05-01", "uid_type": "VKID"},
                    {"uid": "10002", "score": "92", "dt": "2020-05-01", "uid_type": "VKID"},
                    {"uid": "10003", "score": "-1", "dt": "2020-05-01", "uid_type": "VKID"},
                    {"uid": "10004", "score": "4294967296", "dt": "2020-05-01", "uid_type": "VKID"},
                    {"uid": "10005", "score": "", "dt": "2020-05-01", "uid_type": "VKID"},
                ],
                "schema": (
                    ("uid", "string"),
                    ("score", "string"),
                    ("dt", "string"),
                    ("uid_type", "string")
                ),
                "partition_columns": ["dt", "uid_type"],
            },
            "expected_data": [
                "user;cat:score",
                "vk:10001;91",
                "vk:10002;92",
            ],
        },  # 4: uint as string
        {
            "source_partition_conf": {"audience_name": ["foo", "bar"], "category": "7", "uid_type": "VKID"},
            "export_columns": {
                "uid": "uid",
                "uid_type": "uid_type",
                "features": [
                    {
                        "name": "score",
                        "expr": "map_values_ordered(user_dmdesc.collect(audience_name, score), array('foo', 'bar'))"
                    }
                ]
            },
            "source": {
                "data": [
                    {"uid": "10001", "score": 0.91, "audience_name": "foo", "category": "7", "dt": "2020-05-01", "uid_type": "VKID"},
                    {"uid": "10001", "score": 0.20, "audience_name": "bar", "category": "7", "dt": "2020-05-01", "uid_type": "VKID"},
                    {"uid": "10002", "score": 0.30, "audience_name": "foo", "category": "7", "dt": "2020-05-01", "uid_type": "VKID"},
                    {"uid": "10003", "score": np.nan, "audience_name": "foo", "category": "7", "dt": "2020-05-01", "uid_type": "VKID"},
                    {"uid": "10004", "score": float("nan"), "audience_name": "foo", "category": "7", "dt": "2020-05-01", "uid_type": "VKID"},
                ],
                "schema": (
                    ("uid", "string"),
                    ("score", "float"),
                    ("audience_name", "string"),
                    ("category", "string"),
                    ("dt", "string"),
                    ("uid_type", "string"),
                ),
                "partition_columns": ["audience_name", "category", "dt", "uid_type"],
            },
            "expected_data": [
                "user;num:score",
                "vk:10001;0.91,0.2",
            ],
        },  # 5: group to map to array
        {
            "source_partition_conf": {"uid_type": "VKID"},
            "export_columns": {
                "uid": "uid",
                "uid_type": "uid_type",
                "bu_link_id": {
                    "expr": "cast(bu_link_id as string)",
                    "link_type": "BannerId"
                },
                "features": [{"name": "bu_score", "expr": "max(score)"}]
            },
            "source": {
                "data": [
                    {"uid": "10001", "bu_link_id": 11, "score": 0.91, "dt": "2020-05-01", "uid_type": "VKID"},
                    {"uid": "10001", "bu_link_id": 22, "score": 0.92, "dt": "2020-05-01", "uid_type": "VKID"},
                    {"uid": "10002", "bu_link_id": -1, "score": 0.92, "dt": "2020-05-01", "uid_type": "VKID"},
                ],
                "schema": (
                    ("uid", "string"),
                    ("bu_link_id", "int"),
                    ("score", "float"),
                    ("dt", "string"),
                    ("uid_type", "string")
                ),
                "partition_columns": ["dt", "uid_type"],
            },
            "expected_data": [
                "user;link_type:BannerId;num:bu_score",
                "vk:10001;11;0.91",
                "vk:10001;22;0.92",
            ],
        },  # 6: simple float
        {
            "source_partition_conf": {"uid_type": "VKID"},
            "export_columns": {
                "uid": "uid",
                "uid_type": "uid_type",
                "features": [
                    {"name": "score_cat", "expr": "cast(avg(score) as int)"},
                    {"name": "score", "expr": "max(score)"},
                    {"name": "score_arr", "expr": "gavg(array(score))"},
                    {"name": "score_map", "expr": "gavg(map_from_arrays(array('42'), array(score)))"},
                ]
            },
            "source": {
                "data": [
                    {"uid": "10001", "score": 1.0, "dt": "2020-05-01", "uid_type": "VKID"},
                    {"uid": "10002", "score": 2.0, "dt": "2020-05-01", "uid_type": "VKID"},
                ],
                "schema": (("uid", "string"), ("score", "float"), ("dt", "string"), ("uid_type", "string")),
                "partition_columns": ["dt", "uid_type"],
            },
            "expected_data": [
                "user;cat:score_cat;num:score;num:score_arr;num:score_map",
                "vk:10001;1;1.0;1.0;42:1.0",
                "vk:10002;2;2.0;2.0;42:2.0",
            ],
        },  # 7: multi features
    ]
    # fmt: on

    # test cases, list of (app_cfg, expected_cfg,)
    return [
        (
            {
                "target_dt": TARGET_DT,
                "target_hdfs_basedir": os.path.join(TMP_HDFS_DIR, "result", ""),
                "features_subdir": "test_{}".format(i),
                "source_db": TEST_DB,
                "source_table": "grinder_{}_source_v{}".format(NAME, i),
                "source_partition_conf": spec["source_partition_conf"],
                "max_dt_diff": spec.get("max_dt_diff", 60),
                "export_columns": spec["export_columns"],
                "min_target_rows": spec.get("min_target_rows", 1),
                "max_collection_size": spec.get("max_collection_size", 2),
                "output_max_rows_per_file": spec.get("output_max_rows_per_file", 2),
                "shuffle_partitions": spec.get("shuffle_partitions", 2),
                "ctid": "ctid_{}".format(i),
                "input_urls": [os.path.join(TMP_HDFS_DIR, "input_{}".format(i))],
                "output_urls": [os.path.join(TMP_HDFS_DIR, "output_{}".format(i))],
                "status_urls": [os.path.join(TMP_HDFS_DIR, "status_{}".format(i))],
            },
            spec,
        )
        for i, spec in enumerate(data_specs, 1)
    ]
