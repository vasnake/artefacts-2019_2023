import os

from pyspark.sql.types import MapType, ArrayType, FloatType, DoubleType, StringType

from prj.apps.utils.testing.defines import TEST_DB

# flake8: noqa
# @formatter:off
# fmt: off

NAME = "test_EUF_app"
TARGET_DT = "2020-06-01"


def generate_test_data(tmp_hdfs_dir):

    def abs_path(*dirs):
        return os.path.normpath(os.path.join(tmp_hdfs_dir, *dirs))

    data_specs_fails = [
        {
            "source_partition_conf": {"uid_type": "VKID"},
            "export_columns": {
                "uid": "uid",
                "uid_type": "uid_type",
                "features": {"bu_score": "gavg(score)"}
            },
            "output_max_rows_per_file": 1,
            "source": {
                "data": [
                    {"uid": "10001", "score": {1: 0.91}, "dt": "2020-05-01", "uid_type": "VKID"},
                    {"uid": "10002", "score": {92: 0.2, 93: 0.3}, "dt": "2020-05-01", "uid_type": "VKID"},
                    {"uid": "10003", "score": {92: 0.2, 94: None}, "dt": "2020-05-01", "uid_type": "VKID"},
                    {"uid": "10004", "score": None, "dt": "2020-05-01", "uid_type": "VKID"},
                    {"uid": "10005", "score": {-1: 0.91}, "dt": "2020-05-01", "uid_type": "VKID"},
                    {"uid": "10007", "score": {1: 0.1, 2: 0.2, 3: 0.3}, "dt": "2020-05-01", "uid_type": "VKID"},
                ],
                "schema": (("uid", "string"), ("score", "map<int,double>"), ("dt", "string"), ("uid_type", "string")),
                "partition_columns": ["dt", "uid_type"],
            },
            "expected_data": [
                "user;num:score",
                "vk:10001;1:0.91",
                "vk:10002;92:0.2,93:0.3",
            ],
        },  # 11 fail, map<int,double> failed agg stage, unsupported map key type
        {  # - combine maps
            "source_partition_conf": {"uid_type": "VKID"},
            "export_columns": {
                "uid": "uid",
                "uid_type": "uid_type",
                "feature": "udf_combine_maps('add_keys_stride', 1000, gavg(score_map1), gavg(score_map2))",
            },
            "source": {
                "data": [
                    {"uid": "10001", "score_map1": {"8": 0.81}, "score_map2": {"9": 0.91}, "dt": "2020-05-01", "uid_type": "VKID",},
                ],
                "schema": (("uid", "string"), ("score_map1", "map<string,float>"), ("score_map2", "map<string,float>"), ("dt", "string"), ("uid_type", "string"),),
                "partition_columns": ["dt", "uid_type"],
            },
            "expected_data": [
                ["user", "num:score"],
                ["vk:10001", "8:0.81,1009:0.91"],
            ],
        },  # 12 fail, combine maps, Undefined function: 'udf_combine_maps'
        {  # - values to map
            "source_partition_conf": {"uid_type": "VKID"},
            "export_columns": {
                "uid": "uid",
                "uid_type": "uid_type",
                "feature": "udf_list2map(array(avg(score1), avg(score2)), 0)",
            },
            "source": {
                "data": [
                    {"uid": "10001", "score1": 0.81, "score2": 0.91, "dt": "2020-05-01", "uid_type": "VKID"},
                ],
                "schema": (("uid", "string"), ("score1", "float"), ("score2", "float"), ("dt", "string"), ("uid_type", "string"),),
                "partition_columns": ["dt", "uid_type"],
            },
            "expected_data": [
                ["user", "num:score"],
                ["vk:10001", "0:0.81,1:0.91"],
            ],
        },  # 13 fail, values to map, Undefined function: 'udf_list2map'
    ]  # fail cases is not supported yet

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
                    {"uid": "10003", "score": {"92": 0.2, "foo": None}, "dt": "2020-05-01", "uid_type": "VKID"},
                    {"uid": "10004", "score": None, "dt": "2020-05-01", "uid_type": "VKID"},
                    {"uid": "10005", "score": {"-1": 0.91}, "dt": "2020-05-01", "uid_type": "VKID"},
                    {"uid": "10006", "score": {"4294967296": 0.91}, "dt": "2020-05-01", "uid_type": "VKID"},
                    {"uid": "10007", "score": {"1": 0.1, "2": 0.2, "3": 0.3}, "dt": "2020-05-01", "uid_type": "VKID"},
                    {"uid": "9223372036854775807", "score": {"2": 0.98}, "dt": "2020-05-01", "uid_type": "HID"},
                    {"uid": "-9223372036854775808", "score": {"3": 0.99}, "dt": "2020-05-01", "uid_type": "HID"},
                ],
                "schema": (("uid", "string"), ("score", "map<string,float>"), ("dt", "string"), ("uid_type", "string")),
                "partition_columns": ["dt", "uid_type"],
            },
            "expected_data": [
                "user;num:feature_foo",
                "vk:10001;1:0.91",
                "vk:10002;92:0.2,93:0.3",
                "hid:9223372036854775807;2:0.98",
                "hid:-9223372036854775808;3:0.99",
            ],
        },  # 1 map<string,float>
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
                    {"uid": "10003", "score": [0.92, None], "dt": "2020-05-01", "uid_type": "VKID"},
                    {"uid": "10004", "score": [0.92, float("nan")], "dt": "2020-05-01", "uid_type": "VKID"},
                    {"uid": "10005", "score": None, "dt": "2020-05-01", "uid_type": "VKID"},
                    {"uid": "10006", "score": [1.0, 2.0, 3.0], "dt": "2020-05-01", "uid_type": "VKID"},
                ],
                "schema": (("uid", "string"), ("score", "array<float>"), ("dt", "string"), ("uid_type", "string")),
                "partition_columns": ["dt", "uid_type"],
            },
            "expected_data": [
                "user;num:score",
                "vk:10001;1.0,0.91",
                "vk:10002;0.92,2.0",
            ],
        },  # 2 array<float>
        {
            "source_partition_conf": {"uid_type": "VKID"},
            "export_columns": {
                "uid": "uid",
                "uid_type": "uid_type",
                "features": [{"name": "score", "expr": "first(score)"}]
            },
            "source": {
                "data": [
                    {"uid": "10001", "score": ["1", "91"], "dt": "2020-05-01", "uid_type": "VKID"},
                    {"uid": "10002", "score": ["92", "2"], "dt": "2020-05-01", "uid_type": "VKID"},
                    {"uid": "10003", "score": ["92", None], "dt": "2020-05-01", "uid_type": "VKID"},
                    {"uid": "100041", "score": ["92", "nan"], "dt": "2020-05-01", "uid_type": "VKID"},
                    {"uid": "100042", "score": ["92", "NaN"], "dt": "2020-05-01", "uid_type": "VKID"},
                    {"uid": "10005", "score": None, "dt": "2020-05-01", "uid_type": "VKID"},
                    {"uid": "10006", "score": ["2", "-1"], "dt": "2020-05-01", "uid_type": "VKID"},
                    {"uid": "10007", "score": ["3", "4294967296"], "dt": "2020-05-01", "uid_type": "VKID"},
                    {"uid": "10008", "score": ["1", "2", "3"], "dt": "2020-05-01", "uid_type": "VKID"},
                ],
                "schema": (("uid", "string"), ("score", "array<string>"), ("dt", "string"), ("uid_type", "string")),
                "partition_columns": ["dt", "uid_type"],
            },
            "expected_data": [
                "user;cat:score",
                "vk:10001;1,91",
                "vk:10002;92,2",
            ],
        },  # 3 array<string>
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
                    {"uid": "10005", "score": None, "dt": "2020-05-01", "uid_type": "VKID"},
                ],
                "schema": (("uid", "string"), ("score", "string"), ("dt", "string"), ("uid_type", "string")),
                "partition_columns": ["dt", "uid_type"],
            },
            "expected_data": [
                "user;cat:score",
                "vk:10001;91",
                "vk:10002;92",
            ],
        },  # 4 uint as string
        {
            "source_partition_conf": {"audience_name": ["foo", "bar"], "category": "7", "uid_type": "VKID"},
            "export_columns": {
                "uid": "uid",
                "uid_type": "uid_type",
                "features": [{"name": "score", "expr": "map_values_ordered(user_dmdesc_collect(audience_name, score), array('foo', 'bar'))"}]
            },
            "feature_expr_native": "map_values_ordered("
                                   "   map_from_arrays(collect_list(audience_name), collect_list(cast(score as float))),"
                                   "   array('foo', 'bar'))",  # arrays of different sizes, won't work
            "source": {
                "data": [
                    {"uid": "10001", "score": 0.91, "audience_name": "foo", "category": "7", "dt": "2020-05-01", "uid_type": "VKID",},
                    {"uid": "10001", "score": 0.20, "audience_name": "bar", "category": "7", "dt": "2020-05-01", "uid_type": "VKID",},
                    {"uid": "10002", "score": 0.30, "audience_name": "foo", "category": "7", "dt": "2020-05-01", "uid_type": "VKID",},
                    {"uid": "10003", "score": None, "audience_name": "foo", "category": "7", "dt": "2020-05-01", "uid_type": "VKID",},
                    {"uid": "10004", "score": float("nan"), "audience_name": "foo", "category": "7", "dt": "2020-05-01", "uid_type": "VKID",},
                ],
                "schema": (("uid", "string"), ("score", "float"), ("audience_name", "string"), ("category", "string"), ("dt", "string"), ("uid_type", "string"),),
                "partition_columns": ["audience_name", "category", "dt", "uid_type"],
            },
            "expected_data": [
                "user;num:score",
                "vk:10001;0.91,0.2",
            ],
        },  # 5 group to map to array
        {
            "source_partition_conf": {"uid_type": "VKID"},
            "export_columns": {
                "uid": "uid",
                "uid_type": "uid_type",
                "features": [{"name": "score", "expr": "max(score)"}]
            },
            "source": {
                "data": [
                    {"uid": "10001", "score": 0.91, "dt": "2020-05-01", "uid_type": "VKID"},
                    {"uid": "10002", "score": 0.92, "dt": "2020-05-01", "uid_type": "VKID"},
                ],
                "schema": (("uid", "string"), ("score", "float"), ("dt", "string"), ("uid_type", "string")),
                "partition_columns": ["dt", "uid_type"],
            },
            "expected_data": [
                "user;num:score",
                "vk:10001;0.91",
                "vk:10002;0.92",
            ],
        },  # 6 simple float
        {
            "source_partition_conf": {"uid_type": "VKID"},
            "export_columns": {
                "uid": "uid",
                "uid_type": "uid_type",
                "features": [{"name": "score", "expr": "gavg(cast(score as map<string,float>))"}]
            },
            "output_max_rows_per_file": 1,
            "source": {
                "data": [
                    {"uid": "10001", "score": {1: 0.91}, "dt": "2020-05-01", "uid_type": "VKID"},
                    {"uid": "10002", "score": {92: 0.2, 93: 0.3}, "dt": "2020-05-01", "uid_type": "VKID"},
                    {"uid": "10003", "score": {92: 0.2, 94: None}, "dt": "2020-05-01", "uid_type": "VKID"},
                    {"uid": "10004", "score": None, "dt": "2020-05-01", "uid_type": "VKID"},
                    {"uid": "10005", "score": {-1: 0.91}, "dt": "2020-05-01", "uid_type": "VKID"},
                    {"uid": "10007", "score": {1: 0.1, 2: 0.2, 3: 0.3}, "dt": "2020-05-01", "uid_type": "VKID"},
                ],
                "schema": (("uid", "string"), ("score", "map<int,double>"), ("dt", "string"), ("uid_type", "string")),
                "partition_columns": ["dt", "uid_type"],
            },
            "expected_data": [
                "user;num:score",
                "vk:10001;1:0.91",
                "vk:10002;92:0.2,93:0.3",
            ],
        },  # 7 map<int,double>
        {
            "source_partition_conf": {"uid_type": "VKID"},
            "export_columns": {
                "uid": "uid",
                "uid_type": "uid_type",
                "features": [{"name": "score", "expr": "first(CASE WHEN score > 0.91 THEN NULL ELSE score END)"}]
            },
            "source": {
                "data": [
                    {"uid": "10001", "score": 0.90, "dt": "2020-05-01", "uid_type": "VKID"},
                    {"uid": "10002", "score": 0.92, "dt": "2020-05-01", "uid_type": "VKID"},
                    {"uid": "10003", "score": 0.93, "dt": "2020-05-01", "uid_type": "VKID"},
                    {"uid": "10004", "score": 0.94, "dt": "2020-05-01", "uid_type": "VKID"},
                    {"uid": "10005", "score": 0.95, "dt": "2020-05-01", "uid_type": "VKID"},
                ],
                "schema": (("uid", "string"), ("score", "float"), ("dt", "string"), ("uid_type", "string")),
                "partition_columns": ["dt", "uid_type"],
            },
            "expected_data": [
                "user;num:score",
                "vk:10001;0.9",
            ],
        },  # 8 score filters
        {
            "source_partition_conf": {"uid_type": "VKID"},
            "export_columns": {
                "uid": "uid",
                "uid_type": "uid_type",
                "features": [{"name": "score", "expr": "gavg(score_list)"}]
            },
            "source": {
                "data": [
                    {"uid": "10001", "score_list": [0.81, 0.91], "dt": "2020-05-01", "uid_type": "VKID"},
                    {"uid": "10002", "score_list": [0.82, 0.92], "dt": "2020-05-01", "uid_type": "VKID"},
                ],
                "schema": (("uid", "string"), ("score_list", "array<float>"), ("dt", "string"), ("uid_type", "string")),
                "partition_columns": ["dt", "uid_type"],
            },
            "expected_data": [
                "user;num:score",
                "vk:10001;0.81,0.91",
                "vk:10002;0.82,0.92",
            ],
        },  # 9 array<float>
        {
            "source_partition_conf": {"uid_type": ["VKID", "FOO_ID"]},
            "export_columns": {
                "uid": "uid",
                "uid_type": "uid_type",
                "features": [{"name": "score", "expr": "gavg(score_map)"}]
            },
            "source": {
                "data": [
                    {"uid": "10001", "score_map": {"8": 0.81, "9": 0.91}, "dt": "2020-05-01", "uid_type": "VKID"},
                    {"uid": "10002", "score_map": {"7": 0.71, "6": 0.61}, "dt": "2020-05-01", "uid_type": "FOO_ID"},
                ],
                "schema": (("uid", "string"), ("score_map", "map<string,float>"), ("dt", "string"), ("uid_type", "string"),),
                "partition_columns": ["dt", "uid_type"],
            },
            "expected_data": [
                "user;num:score",
                "vk:10001;8:0.81,9:0.91",
            ],
        },  # 10 map<string,float>
        {
            "source_partition_conf": {"uid_type": "VKID"},
            "export_columns": {
                "uid": "uid",
                "uid_type": "uid_type",
                "bu_link_id": {
                    "expr": "cast(bu_link_id as string)",
                    "link_type": "Unknown"
                },
                "features": [
                    {"name": "score_1", "expr": "max(score)"},
                    {"name": "score_2", "expr": "min(score)"}
                ]
            },
            "source": {
                "data": [
                    {"uid": "10001", "bu_link_id": 11, "score": 0.91, "dt": "2020-05-01", "uid_type": "VKID"},
                    {"uid": "10002", "bu_link_id": 22, "score": 0.92, "dt": "2020-05-01", "uid_type": "VKID"},
                    {"uid": "10002", "bu_link_id": -1, "score": 0.92, "dt": "2020-05-01", "uid_type": "VKID"},
                ],
                "schema": (("uid", "string"), ("bu_link_id", "int"), ("score", "float"), ("dt", "string"), ("uid_type", "string")),
                "partition_columns": ["dt", "uid_type"],
            },
            "expected_data": [
                "user;link_type:Unknown;num:score_1;num:score_2",
                "vk:10001;11;0.91;0.91",
                "vk:10002;22;0.92;0.92",
            ],
        },  # 11 two float features, export_columns.bu_link_id
        {
            # "feature_name": "bu_score",
            "source_partition_conf": {"uid_type": "VKID"},
            "export_columns": {
                "uid": "uid",
                "uid_type": "uid_type",
                "bu_link_id": {
                    "expr": "cast(bu_link_id as string)",
                    "link_type": "BannerId"
                },
                "features": [{"name": "bu_score", "expr": "gavg(cast(score as map<string,float>))"}]
            },
            "output_max_rows_per_file": 1,
            "source": {
                "data": [
                    {"uid": "10001", "bu_link_id": 11, "score": {1: 0.91}, "dt": "2020-05-01", "uid_type": "VKID"},
                    {"uid": "10001", "bu_link_id": -1, "score": {1: 0.91}, "dt": "2020-05-01", "uid_type": "VKID"},
                    {"uid": "10002", "bu_link_id": 22, "score": {92: 0.2, 93: 0.3}, "dt": "2020-05-01", "uid_type": "VKID"},
                    {"uid": "10003", "bu_link_id": 33, "score": {92: 0.2, 94: None}, "dt": "2020-05-01", "uid_type": "VKID"},
                    {"uid": "10004", "bu_link_id": 0, "score": None, "dt": "2020-05-01", "uid_type": "VKID"},
                    {"uid": "10005", "bu_link_id": 0, "score": {-1: 0.91}, "dt": "2020-05-01", "uid_type": "VKID"},
                    {"uid": "10007", "bu_link_id": 0, "score": {1: 0.1, 2: 0.2, 3: 0.3}, "dt": "2020-05-01", "uid_type": "VKID"},
                ],
                "schema": (("uid", "string"), ("bu_link_id", "long"), ("score", "map<int,double>"), ("dt", "string"), ("uid_type", "string")),
                "partition_columns": ["dt", "uid_type"],
            },
            "expected_data": [
                "user;link_type:BannerId;num:bu_score",
                "vk:10001;11;1:0.91",
                "vk:10002;22;92:0.2,93:0.3",
            ],
        },  # 12 map<int,double>, export_columns.bu_link_id
        {
            # "feature_name": "bu_score",
            "source_partition_conf": {"uid_type": "VKID"},
            "export_columns": {
                "uid": "uid",
                "uid_type": "uid_type",
                "bu_link_id": {
                    "expr": "bu_link_id",
                    "link_type": "DummyStrLink"
                },
                "features": [{"name": "bu_score", "expr": "first(score)"}]
            },
            "source": {
                "data": [
                    {"uid": "10001", "bu_link_id": "-1", "score": [1.0, 0.91], "dt": "2020-05-01", "uid_type": "VKID"},
                    {"uid": "10002", "bu_link_id": "4294967296", "score": [0.92, 2.0], "dt": "2020-05-01", "uid_type": "VKID"},
                    {"uid": "10002", "bu_link_id": "foo", "score": [0.92, 2.0], "dt": "2020-05-01", "uid_type": "VKID"},
                    {"uid": "10003", "bu_link_id": "0", "score": [0.92, None], "dt": "2020-05-01", "uid_type": "VKID"},
                    {"uid": "10004", "bu_link_id": "0", "score": [0.92, float("nan")], "dt": "2020-05-01", "uid_type": "VKID"},
                    {"uid": "10005", "bu_link_id": "0", "score": None, "dt": "2020-05-01", "uid_type": "VKID"},
                    {"uid": "10006", "bu_link_id": "0", "score": [1.0, 2.0, 3.0], "dt": "2020-05-01", "uid_type": "VKID"},
                ],
                "schema": (("uid", "string"), ("bu_link_id", "string"), ("score", "array<float>"), ("dt", "string"), ("uid_type", "string")),
                "partition_columns": ["dt", "uid_type"],
            },
            "expected_data": [
                "user;link_type:DummyStrLink;num:bu_score",
                "vk:10001;-1;1.0,0.91",
                "vk:10002;4294967296;0.92,2.0",
                "vk:10002;foo;0.92,2.0",
            ],
        },  # 13 array<float>, export_columns.bu_link_id
        {
            # "feature_name": "bu_score",
            "source_partition_conf": {"uid_type": "VKID"},
            "export_columns": {
                "uid": "uid",
                "uid_type": "uid_type",
                "bu_link_id": {
                    "expr": "bu_link_id",
                    "link_type": "AggBannerTopic"
                },
                "features": [{"name": "bu_score", "expr": "first(score)"}]
            },
            "source": {
                "data": [
                    {"uid": "10001", "bu_link_id": 11, "score": ["1", "91"], "dt": "2020-05-01", "uid_type": "VKID"},
                    {"uid": "10002", "bu_link_id": 22, "score": ["92", "2"], "dt": "2020-05-01", "uid_type": "VKID"},
                    {"uid": "10002", "bu_link_id": -1, "score": ["92", "2"], "dt": "2020-05-01", "uid_type": "VKID"},
                    {"uid": "10003", "bu_link_id": 33, "score": ["92", None], "dt": "2020-05-01", "uid_type": "VKID"},
                    {"uid": "100041", "bu_link_id": 0, "score": ["92", "nan"], "dt": "2020-05-01", "uid_type": "VKID"},
                    {"uid": "100042", "bu_link_id": 0, "score": ["92", "NaN"], "dt": "2020-05-01", "uid_type": "VKID"},
                    {"uid": "10005", "bu_link_id": 0, "score": None, "dt": "2020-05-01", "uid_type": "VKID"},
                    {"uid": "10006", "bu_link_id": 0, "score": ["2", "-1"], "dt": "2020-05-01", "uid_type": "VKID"},
                    {"uid": "10007", "bu_link_id": 0, "score": ["3", "4294967296"], "dt": "2020-05-01", "uid_type": "VKID"},
                    {"uid": "10008", "bu_link_id": 0, "score": ["1", "2", "3"], "dt": "2020-05-01", "uid_type": "VKID"},
                ],
                "schema": (("uid", "string"), ("bu_link_id", "int"), ("score", "array<string>"), ("dt", "string"), ("uid_type", "string")),
                "partition_columns": ["dt", "uid_type"],
            },
            "expected_data": [
                "user;link_type:AggBannerTopic;cat:bu_score",
                "vk:10001;11;1,91",
                "vk:10002;22;92,2",
            ],
        },  # 14 array<string>, export_columns.bu_link_id
        {
            "source_partition_conf": {"uid_type": "VKID"},
            "export_columns": {
                "uid": "uid",
                "uid_type": "uid_type",
                "features": [
                    {"name": "score_cat", "expr": "cast(avg(score) as int)"},
                    {"name": "score", "expr": "max(score)"},
                    {"name": "score_arr", "expr": "gavg(array(score))"},
                    {"name": "score_map", "expr": "gavg(map_from_arrays(array('42'), array(score)))"}
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
                # ['user', 'cat:score_cat', 'num:score', 'num:score_arr', 'num:score_map']
                # ['user', 'cat:score_cat', 'num:score_arr', 'num:score_map', 'num:score']
                "user;cat:score_cat;num:score;num:score_arr;num:score_map",
                "vk:10001;1;1.0;1.0;42:1.0",
                "vk:10002;2;2.0;2.0;42:2.0",
            ],
        },  # 15 multi features
    ]

    # test cases, list of (app_cfg, expected_cfg, i,)
    return [
        (
            {
                "target_dt": TARGET_DT,
                "target_hdfs_basedir": abs_path("result_{}".format(i), ""),
                "features_subdir": spec.get("features_subdir", "some_feature"),

                "source_db": TEST_DB,
                "source_table": "grinder_{}_source_v{}".format(NAME, i),
                "source_partition_conf": spec["source_partition_conf"],
                "max_dt_diff": spec.get("max_dt_diff", 60),

                "export_columns": spec["export_columns"],

                "min_target_rows": spec.get("min_target_rows", 1),
                "shuffle_partitions": spec.get("shuffle_partitions", 2),
                "max_collection_size": spec.get("max_collection_size", 2),
                "output_max_rows_per_file": spec.get("output_max_rows_per_file", 2),

                "csv_params": {
                    "header": True,
                    "delimiter": ";",
                    "quote": "",
                    "collection_items_sep": ",",
                    "map_kv_sep": ":",
                },

                "ctid": "ctid_{}".format(i),
                "input_urls": [abs_path("input_{}".format(i))],
                "output_urls": [abs_path("output_{}".format(i))],
                "status_urls": [abs_path("status_{}".format(i))],
            },
            spec,
            i
        )
        for i, spec in enumerate(data_specs, 1)
    ]
# @formatter:on
# fmt: on
