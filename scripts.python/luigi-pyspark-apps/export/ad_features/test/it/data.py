# flake8: noqa

import os

from pyspark.sql.types import MapType, ArrayType, FloatType, DoubleType, StringType

from prj.apps.utils.testing.defines import TEST_DB, TEST_HDFS_DIR
from prj.apps.utils.control.client.status import FatalStatusException

NAME = "test_export_ad_features_app"
TARGET_DT = "2020-09-27"
TMP_HDFS_DIR = os.path.join(TEST_HDFS_DIR, NAME)

# fmt: off
# @formatter:off

SOURCE_TABLE = "grinder_{}_source".format(NAME)
SOURCE_TABLE_SPEC = (
    ("uid", "string", None),
    ("score", "float", float("nan")),
    ("score_list", "array<float>", None),
    ("score_map", "map<string,float>", None),
    ("cat_list", "array<string>", None),
    ("part", "string", "P0"),
    ("feature_name", "string", "some_feature"),
    ("dt", "string", TARGET_DT),
    ("uid_type", "string", "BANNER"),
)
SOURCE_TABLE_SCHEMA = tuple((n, t) for n, t, _ in SOURCE_TABLE_SPEC)
SOURCE_PARTITION_COLUMNS = ["part", "feature_name", "dt", "uid_type"]
SOURCE_DATA_DEFAULTS = {n: d for n, _, d in SOURCE_TABLE_SPEC}

SOURCE_DATA = [
    # P1, simple float
    {"uid": "10001", "score": 0.91, "part": "P1"},
    {"uid": "10002", "score": 0.91, "part": "P1"},
    {"uid": "10002", "score": 0.93, "part": "P1"},
    {"uid": "10002", "score": None, "part": "P1"},
    {"uid": "10002", "score": float("nan"), "part": "P1"},
    {"uid": "10003", "score": None, "part": "P1"},
    {"uid": "10004", "score": float("nan"), "part": "P1"},
    {"uid": None, "part": "P1"},
    {"uid": "4294967296", "score": 0.91, "part": "P1"},
    {"uid": "-1", "score": 0.91, "part": "P1"},
    {"uid": "foo", "score": 0.99, "part": "P1"},
    # P2, float, too much rows
    {"uid": "10001", "score": 0.91, "part": "P2"},
    {"uid": "10002", "score": 0.92, "part": "P2"},
    {"uid": "10003", "score": 0.93, "part": "P2"},
    {"uid": "10004", "score": 0.94, "part": "P2"},
    {"uid": "4294967295", "score": 0.95, "part": "P2"},
    # P6 more than one feature in file
    {"uid": "10001", "score": 0.91, "score_list": [0.11, 0.12], "score_map": {"13": 0.13, "14": 0.14}, "cat_list": ["15", "16"], "part": "P6"},
    {"uid": "10002", "score": 0.92, "score_list": [0.21, 0.22], "score_map": {"23": 0.23, "24": 0.24}, "cat_list": ["25", "26"], "part": "P6"},
    # invalid rows, score
    {"uid": "10003", "score": None, "score_list": [0.32, 0.33], "score_map": {"34": 0.34, "35": 0.35}, "cat_list": ["36", "37"], "part": "P6"},
    {"uid": "10004", "score": float("nan"), "score_list": [0.32, 0.33], "score_map": {"34": 0.34, "35": 0.35}, "cat_list": ["36", "37"], "part": "P6"},
    # invalid rows, score_list
    {"uid": "10005", "score": 0.31, "score_list": [None, 0.33], "score_map": {"34": 0.34, "35": 0.35}, "cat_list": ["36", "37"], "part": "P6"},
    {"uid": "10006", "score": 0.31, "score_list": [0.32, float("nan")], "score_map": {"34": 0.34, "35": 0.35}, "cat_list": ["36", "37"], "part": "P6"},
    {"uid": "10007", "score": 0.31, "score_list": [0.32, 0.33, 0.34], "score_map": {"34": 0.34, "35": 0.35}, "cat_list": ["36", "37"], "part": "P6"},
    {"uid": "10027", "score": 0.31, "score_list": [], "score_map": {"34": 0.34, "35": 0.35}, "cat_list": ["36", "37"], "part": "P6"},
    # invalid rows, score_map
    {"uid": "10008", "score": 0.31, "score_list": [0.32, 0.33], "score_map": {"34": None, "35": 0.35}, "cat_list": ["36", "37"], "part": "P6"},
    {"uid": "10009", "score": 0.31, "score_list": [0.32, 0.33], "score_map": {"34": 0.34, "35": float("nan")}, "cat_list": ["36", "37"], "part": "P6"},
    {"uid": "10011", "score": 0.31, "score_list": [0.32, 0.33], "score_map": {"34": 0.34, "35": 0.35, "36": 0.36}, "cat_list": ["36", "37"], "part": "P6"},
    {"uid": "10111", "score": 0.31, "score_list": [0.32, 0.33], "score_map": {"34": 0.34, "0.35": 0.35}, "cat_list": ["36", "37"], "part": "P6"},
    {"uid": "10021", "score": 0.31, "score_list": [0.32, 0.33], "score_map": {}, "cat_list": ["36", "37"], "part": "P6"},
    # invalid rows, cat_list
    {"uid": "10012", "score": 0.31, "score_list": [0.32, 0.33], "score_map": {"34": 0.34, "35": 0.35}, "cat_list": ["36", None], "part": "P6"},
    {"uid": "10022", "score": 0.31, "score_list": [0.32, 0.33], "score_map": {"34": 0.34, "35": 0.35}, "cat_list": ["36", "4294967296"], "part": "P6"},
    {"uid": "10013", "score": 0.31, "score_list": [0.32, 0.33], "score_map": {"34": 0.34, "35": 0.35}, "cat_list": ["36", "37", "38"], "part": "P6"},
    {"uid": "10014", "score": 0.31, "score_list": [0.32, 0.33], "score_map": {"34": 0.34, "35": 0.35}, "cat_list": [], "part": "P6"},
    # P7 string, array<string>, map<string,float>
    {"uid": "10001", "score": 11.0, "score_list": [12.0, 13.0], "score_map": {"14": 14.0}, "part": "P7"},
    {"uid": "10002", "score": 21.0, "score_list": [22.0, 23.0], "score_map": {"24": 24.0}, "part": "P7"},
    # invalid rows, score
    {"uid": "20001", "score": -2.1, "score_list": [22.0, 23.0], "score_map": {"24": 24.0}, "part": "P7"},
    {"uid": "20002", "score": None, "score_list": [22.0, 23.0], "score_map": {"24": 24.0}, "part": "P7"},
    {"uid": "20003", "score": 4294967296.0, "score_list": [22.0, 23.0], "score_map": {"24": 24.0}, "part": "P7"},
]

SOURCE_DATA = [
    {n: row.get(n, SOURCE_DATA_DEFAULTS[n]) for n, _ in SOURCE_TABLE_SCHEMA}
    for row in SOURCE_DATA
]


def generate_test_data(tmp_dir=TMP_HDFS_DIR):
    def abs_path(*dirs):
        return os.path.join(tmp_dir, *dirs)

    skipped = [
        {
            "source_partition_conf": {"part": "P1"},
            "export_columns": {
                "uid": {
                    "name": "banner",
                    "expr": "array(uid)"
                },
                "features": {"app": "gavg(score)"},
            },
            "expect_fatal": True,
        },  # 10 FATAL, wrong feature type // validation removed, skip
        {
            "source_partition_conf": {"part": "P1"},
            "export_columns": {
                "uid": {
                    "name": "num_banner_feature_0",
                    "expr": "array(uid)"
                },
                "features": {"num_banner_feature_1": "gavg(score)"},
            },
            "exception": FatalStatusException,
        },  # 11 exception, key not a `categorical` // validation removed, skip
        {
            "source_partition_conf": {"part": "P1"},
            "export_columns": {
                "uid": {
                    "name": "foo",
                    "expr": "array(uid)"
                },
                "features": {"num_banner_feature_1": "gavg(score)"},
            },
            "exception": FatalStatusException,
        },  # 12 exception, no such key // validation removed, skip
        {
            "source_partition_conf": {"part": "P1"},
            "export_columns": {
                "uid": {
                    "name": "banner",
                    "expr": "uid"
                },
                "features": {"foo": "gavg(score)"},
            },
            "exception": FatalStatusException,
        },  # 13 exception, no such feature // validation removed, skip
        {
            "source_partition_conf": {"part": "P1"},
            "export_columns": {
                "uid": {
                    "name": "banner",
                    "expr": "uid"
                },
                "features": {"banner": "cast(gavg(score) as string)"},
            },
            "exception": FatalStatusException,
        },  # 14 exception, key name = feature name // validation removed, skip
    ]

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
        },  # 1 simple float
        {
            "max_target_rows": 5,
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
        },  # 2 produced_rows = max_target_rows
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
        },  # 3 extra files present # other files may be present in output dir, should not be touched
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
        },  # 4 old file present # old file may be present in output dir, should be overridden
        {
            "source_partition_conf": {"part": "P6"},
            "export_columns": {
                "uid": {
                    "name": "banner",
                    "expr": "uid"
                },
                "features": [
                    {"name": "banner_feature_0", "expr": "gavg(cast(cat_list as array<long>))"},
                    {"name": "num_banner_feature_0", "expr": "most_freq(score)"},
                    {"name": "num_banner_feature_1", "expr": "gsum(score_list)"},
                    {"name": "num_banner_feature_2", "expr": "gmax(score_map)"},
                ]
            },
            "expected_data": [
                "banner;banner_feature_0;num_banner_feature_0;num_banner_feature_1;num_banner_feature_2",
                "10001;15,16;0.91;0.11,0.12;14:0.14,13:0.13",
                "10002;25,26;0.92;0.21,0.22;24:0.24,23:0.23"
            ],
        },  # 5 more than one feature in file; allowed feature type: float, array<float>, map<string,float>, array<long>
        {
            "source_partition_conf": {"part": "P7"},
            "export_columns": {
                "uid": {
                    "name": "banner",
                    "expr": "uid"
                },
                "features": [
                    {"name": "app", "expr": "cast(cast(gmax(score_list) as array<int>) as array<string>)"},
                    {"name": "contents", "expr": "cast(cast(gmax(score) as long) as string)"},
                    {
                        "name": "num_banner_feature_2", "expr": """
                        cast(map_from_entries(
                            transform(
                                user_dmdesc.map_key_values( gmax(score_map) ),
                                _x -> ( cast(_x['key'] as int), _x['value'] )
                            )
                        ) as map<string,float>)
                    """
                    }
                ]
            },
            "expected_data": [
                "banner;app;contents;num_banner_feature_2",
                "10001;12,13;11;14:14.0",
                "10002;22,23;21;24:24.0",
            ],
        },  # 6 allowed feature type: string, array<string>, map<string,float>
        {
            "source_partition_conf": {"part": "P7"},
            "export_columns": {
                "uid": {
                    "name": "banner",
                    "expr": "uid"
                },
                "features": [
                    {"name": "app", "expr": "cast(gmax(score_list) as array<int>)"},
                    {"name": "contents", "expr": "cast(gmax(score) as long)"},
                    {
                        "name": "num_banner_feature_2", "expr": """
                        map_from_entries(
                            transform(
                                user_dmdesc.map_key_values( gmax(score_map) ),
                                _x -> ( cast(_x['key'] as int), _x['value'] )
                            )
                        )
                    """
                    }
                ]
            },
            "expected_data": [
                "banner;app;contents;num_banner_feature_2",
                "10001;12,13;11;14:14.0",
                "10002;22,23;21;24:24.0",
            ],
        },  # 7 allowed feature type: int, array<int>, map<int,float>
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
            "expect_fatal": True,
        },  # 8 FATAL, too many rows
        {
            "min_target_rows": 3,
            "source_partition_conf": {"part": "P1"},
            "export_columns": {
                "uid": {
                    "name": "banner",
                    "expr": "uid"
                },
                "features": [{"name": "num_banner_feature_0", "expr": "gavg(score)"}]
            },
            "expect_fatal": True,
        },  # 9 FATAL, not enough rows
        {
            "source_partition_conf": {"part": "P1"},
            "export_columns": {
                "uid": {
                    "name": "banner",
                    "expr": "uid"
                },
                "features": [
                    {"name": "num_banner_feature_0", "expr": "gavg(score)"},
                    {"name": "num_banner_feature_1", "expr": "gmin(score)"},
                    {"name": "num_banner_feature_0", "expr": "gmax(score)"}
                ]
            },
            "expect_fatal": True,
            "exception": FatalStatusException
        },  # 10 FATAL, repeated features
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
        },  # 11 output_max_rows_per_file, two files
    ]

    data = [
        (
            {
                # src-trg
                "target_dt": TARGET_DT,
                "target_hdfs_basedir": abs_path("result_{}".format(i)),
                "features_subdir": spec.get("features_subdir", "some_features"),
                "source_db": TEST_DB,
                "source_table": SOURCE_TABLE,
                "source_partition_conf": spec["source_partition_conf"],
                "export_columns": spec["export_columns"],

                # limits
                "max_dt_diff": spec.get("max_dt_diff"),
                "shuffle_partitions": spec.get("shuffle_partitions", 4),
                "min_target_rows": spec.get("min_target_rows", 1),

                # current work
                "max_target_rows": spec.get("max_target_rows", 10),
                "output_max_rows_per_file": spec.get("output_max_rows_per_file", int(1e6)),
                "max_collection_size": spec.get("max_collection_size", 2),

                # control
                "ctid": "ctid_{}".format(i),
                "input_urls": [abs_path("input_{}".format(i))],
                "output_urls": [abs_path("output_{}".format(i))],
                "status_urls": [abs_path("status_{}".format(i))],

                # CSV
                "csv_params": {
                    "header": True,
                    "delimiter": ";",
                    "quote": "",
                    "collection_items_sep": ",",
                    "map_kv_sep": ":",
                },
            },
            spec,
            i,
        )
        for i, spec in enumerate(data_specs, 1)
    ]
    print("\n\nAd features test data generated.\n")
    return data
