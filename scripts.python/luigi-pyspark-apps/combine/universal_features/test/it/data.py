import os

from pyspark.sql.types import MapType, ArrayType, FloatType, DoubleType, StringType

from prj.apps.utils.testing.defines import TEST_DB, TEST_HDFS_DIR

NAME = "test_combine_universal_features_app"
TARGET_DT = "2020-06-01"
TMP_HDFS_DIR = os.path.join(TEST_HDFS_DIR, NAME)

# flake8: noqa
# fmt: off
# @formatter:off

TABLES_DATA = {
    "local_db.features_e2e": {
        "schema": [
            ("uid", StringType()),
            ("fg_0", ArrayType(DoubleType())),
            ("fg_1", MapType(StringType(), DoubleType())),
            ("dt", StringType()),
            ("uid_type", StringType()),
        ],
        "partitions": ["dt", "uid_type"],
        "rows": [("a", [float(x) for x in range(20)], {"0": 3.14, "2": 0.9, "9": 99.9}, "2020-10-15", "VID")],
    },
    "local_db.features_table": {
        "schema": [
            ("uid", StringType()),
            ("0", MapType(StringType(), DoubleType())),
            ("1", ArrayType(DoubleType())),
            ("dt", StringType()),
            ("uid_type", StringType()),
        ],
        "partitions": ["dt", "uid_type"],
        "rows": [("a", {"0": 3.14, "2": 0.9, "9": 99.9}, [float(x) for x in range(20)], "2020-10-15", "VID")],
    },
    "local_db.audience_table": {
        "schema": [
            ("audience_name", StringType()),
            ("category", StringType()),
            ("dt", StringType()),
            ("uid_type", StringType()),
            ("uid", StringType()),
            ("score", DoubleType()),
            ("scores_raw", ArrayType(DoubleType())),
            ("scores_trf", ArrayType(DoubleType())),
        ],
        "partitions": ["audience_name", "category", "dt", "uid_type"],
        "rows": [],
    },
    "snb_cdm_activity.activity_agg": {
        "schema": [
            ("uid", StringType()),
            ("score", MapType(StringType(), FloatType())),
            ("source_name", StringType()),
            ("activity", StringType()),
            ("aggregation_period", StringType()),
            ("dt", StringType()),
            ("uid_type", StringType()),
        ],
        "partitions": ["source_name", "activity", "aggregation_period", "dt", "uid_type"],
        "rows": [
            # @formatter:off
            ("d", {"2020-07-07": 23.0, "2020-07-08": 50.0}, "FOO", "BAR", "3_days", "2020-07-09", "VID"),
            (
                "a",
                {"2020-07-07": 43.0, "2020-07-08": 50.0, "2020-07-09": 19.0},
                "ANTIFRAUD",
                "SHOW",
                "3_days",
                "2020-07-09",
                "OKID",
            ),
            (
                "b",
                {"2020-07-07": 40.0, "2020-07-08": 20.0, "2020-07-09": 15.0},
                "ANTIFRAUD",
                "SHOW",
                "3_days",
                "2020-07-09",
                "VKID",
            ),
            (
                "c",
                {"2020-07-07": 49.0, "2020-07-08": 30.0, "2020-07-09": 10.0},
                "ANTIFRAUD",
                "SHOW",
                "3_days",
                "2020-07-09",
                "VID",
            )
            # @formatter:on
        ],
    },
    "snb_ds_auditories.active_audience": {
        "schema": [
            ("uid", StringType()),
            ("score", DoubleType()),
            ("audience_name", StringType()),
            ("category", StringType()),
            ("dt", StringType()),
            ("uid_type", StringType()),
        ],
        "partitions": ["audience_name", "category", "dt", "uid_type"],
        "rows": [],
    },
}

TARGET_TABLE = "grinder_{}_target".format(NAME)
TARGET_SCHEMA = (
    ("uid", "string"),
    ("bu_link_id", "string"),
    ("score", "float"),
    ("score_list", "array<float>"),
    ("score_map", "map<string,float>"),
    ("cat_list", "array<string>"),
    ("feature_name", "string"),
    ("dt", "string"),
    ("uid_type", "string"),
)
TARGET_PARTITION_COLUMNS = ["feature_name", "dt", "uid_type"]

SOURCE_TABLE = "grinder_{}_source".format(NAME)
SOURCE_TABLE_SCHEMA = (
    ("uid", "string"),
    ("score", "float"),
    ("score_map", "map<string,float>"),
    ("score_list", "array<float>"),
    ("cat_list", "array<string>"),
    ("category", "int"),
    ("audience_name", "string"),
    ("bu_link_id", "string"),
    ("part", "string"),
    ("dt", "string"),
    ("uid_type", "string"),
)
SOURCE_PARTITION_COLUMNS = ["part", "dt", "uid_type"]
SOURCE_DATA = [
    # P1
    {"uid": "10001", "score": 0.91, "bu_link_id": "11", "part": "P1", "dt": "2020-05-01", "uid_type": "VKID"},
    {"uid": "10001", "score": None, "bu_link_id": "11", "part": "P1", "dt": "2020-05-01", "uid_type": "VKID"},
    # {"uid": "10001", "score": float("nan"), "bu_link_id": "11", "part": "P1", "dt": "2020-05-01", "uid_type": "VKID"},
    # {"uid": "10001", "score": float("+inf"), "bu_link_id": "11", "part": "P1", "dt": "2020-05-01", "uid_type": "VKID"},
    # {"uid": "10001", "score": float("-inf"), "bu_link_id": "11", "part": "P1", "dt": "2020-05-01", "uid_type": "VKID"},

    {"uid": "10002", "score": 0.92, "bu_link_id": "22", "part": "P1", "dt": "2020-05-01", "uid_type": "VKID"},
    {"uid": "10002", "score": 0.92, "bu_link_id": "222", "part": "P1", "dt": "2020-05-01", "uid_type": "VKID"},
    {"uid": "10003", "score": None, "bu_link_id": "foo", "part": "P1", "dt": "2020-05-01", "uid_type": "VKID"},
    {"uid": "10004", "score": float("nan"), "bu_link_id": "foo", "part": "P1", "dt": "2020-05-01", "uid_type": "VKID"},
    {"uid": "12312", "score": 0.99, "bu_link_id": "foo", "part": "P1", "dt": "2020-05-01", "uid_type": "VKID"},
    # P2
    {"uid": "10001", "score_map": {"1": 0.91}, "bu_link_id": "11", "part": "P2", "dt": "2020-05-01", "uid_type": "VKID"},
    {"uid": "10002", "score_map": {"92": 0.2, "-93": 0.3}, "bu_link_id": "22", "part": "P2", "dt": "2020-05-01", "uid_type": "VKID"},
    {"uid": "10003", "score_map": {"1": 0.2, "2": None}, "part": "P2", "dt": "2020-05-01", "uid_type": "VKID"},
    {"uid": "10004", "score_map": {"1": 0.2, "2": float("nan")}, "part": "P2", "dt": "2020-05-01", "uid_type": "VKID"},
    {"uid": "10009", "score_map": {"1": 0.1, "2": 0.2, "3": 0.3}, "part": "P2", "dt": "2020-05-01", "uid_type": "VKID"},
    # invalid rows
    {"uid": "12345", "score_map": {"1": 0.99, "2": 0.99}, "bu_link_id": "foo", "part": "P2", "dt": "2020-05-01", "uid_type": "VKID"},
    {"uid": "10005", "score_map": None, "part": "P2", "dt": "2020-05-01", "uid_type": "VKID"},
    {"uid": "10006", "score_map": {"-1": 0.91}, "part": "P2", "dt": "2020-05-01", "uid_type": "VKID"},
    {"uid": "10007", "score_map": {"foo": 0.91}, "part": "P2", "dt": "2020-05-01", "uid_type": "VKID"},
    {"uid": "10008", "score_map": {"4294967296": 0.91}, "part": "P2", "dt": "2020-05-01", "uid_type": "VKID"},
    # P3
    {"uid": "10001", "score_list": [1.0, 0.91], "bu_link_id": "11", "part": "P3", "dt": "2020-05-01", "uid_type": "VKID"},
    {"uid": "10002", "score_list": [0.92, 2.0], "bu_link_id": "22", "part": "P3", "dt": "2020-05-01", "uid_type": "VKID"},
    {"uid": "12345", "score_list": [0.99, 0.9], "part": "P3", "dt": "2020-05-01", "uid_type": "VKID"},
    {"uid": "10003", "score_list": [0.92, None], "part": "P3", "dt": "2020-05-01", "uid_type": "VKID"},
    {"uid": "10004", "score_list": [0.92, float("nan")], "part": "P3", "dt": "2020-05-01", "uid_type": "VKID"},
    {"uid": "10005", "score_list": None, "part": "P3", "dt": "2020-05-01", "uid_type": "VKID"},
    {"uid": "10006", "score_list": [1.0, 2.0, 3.0], "bu_link_id": "baz", "part": "P3", "dt": "2020-05-01", "uid_type": "VKID"},
    # P4
    {"uid": "10001", "cat_list": ["1", "91"], "part": "P4", "dt": "2020-05-01", "uid_type": "VKID"},
    {"uid": "10002", "cat_list": ["92", "2"], "part": "P4", "dt": "2020-05-01", "uid_type": "VKID"},
    {"uid": "10003", "cat_list": None, "part": "P4", "dt": "2020-05-01", "uid_type": "VKID"},
    {"uid": "10004", "cat_list": [], "part": "P4", "dt": "2020-05-01", "uid_type": "VKID"},
    {"uid": "10005", "cat_list": [None], "part": "P4", "dt": "2020-05-01", "uid_type": "VKID"},
    {"uid": "10006", "cat_list": ["2", "-1"], "part": "P4", "dt": "2020-05-01", "uid_type": "VKID"},
    {"uid": "10007", "cat_list": ["3", "4294967296"], "part": "P4", "dt": "2020-05-01", "uid_type": "VKID"},
    {"uid": "10008", "cat_list": ["1", "2", "3"], "part": "P4", "dt": "2020-05-01", "uid_type": "VKID"},
    {"uid": "9223372036854775807", "cat_list": ["3", "4"], "part": "P4", "dt": "2020-05-01", "uid_type": "HID"},
    {"uid": "-9223372036854775808", "cat_list": ["5", "6"], "part": "P4", "dt": "2020-05-01", "uid_type": "HID"},
    # P5
    {"uid": "10001", "score": 0.91, "audience_name": "foo", "part": "P5", "dt": "2020-05-01", "uid_type": "VKID",},
    {"uid": "10001", "score": 0.20, "audience_name": "bar", "part": "P5", "dt": "2020-05-01", "uid_type": "VKID",},
    {"uid": "10002", "score": None, "audience_name": "foo", "part": "P5", "dt": "2020-05-01", "uid_type": "VKID",},
    {"uid": "10003", "score": float("nan"), "audience_name": "bar", "part": "P5", "dt": "2020-05-01", "uid_type": "VKID",},
    # P6
    {"uid": "10001", "score_map": {"1": 0.1}, "part": "P6", "dt": "2020-05-01", "uid_type": "VKID"},
    {"uid": "10001", "score_map": {"4294967296": 0.2}, "part": "P6", "dt": "2020-05-01", "uid_type": "OKID"},
    {"uid": "10001", "score_map": {"-1": 0.3, "foo": 0.4}, "part": "P6", "dt": "2020-05-01", "uid_type": "HID"},
    # invalid rows
    {"uid": "10004", "score_map": {"2": None}, "part": "P6", "dt": "2020-05-01", "uid_type": "VKID"},
    {"uid": "10005", "score_map": {"2": float("nan")}, "part": "P6", "dt": "2020-05-01", "uid_type": "VKID"},
    {"uid": "10006", "score_map": None, "part": "P6", "dt": "2020-05-01", "uid_type": "VKID"},
    # P7
    {"uid": "10001", "cat_list": ["-1", "91"], "bu_link_id": "11", "part": "P7", "dt": "2020-05-01", "uid_type": "VKID"},
    {"uid": "10002", "cat_list": ["-92", "2"], "bu_link_id": "22", "part": "P7", "dt": "2020-05-01", "uid_type": "VKID"},
    {"uid": "10008", "cat_list": ["1", "2", "3"], "part": "P7", "dt": "2020-05-01", "uid_type": "VKID"},
    # P8
    {"uid": "10001", "category": 0, "part": "P8", "dt": "2020-05-01", "uid_type": "VKID"},
    {"uid": "10002", "category": 1, "part": "P8", "dt": "2020-05-01", "uid_type": "VKID"},
    {"uid": "10003", "category": -1, "part": "P8", "dt": "2020-05-01", "uid_type": "VKID"},
    {"uid": "10004", "category": 2147483647, "part": "P8", "dt": "2020-05-01", "uid_type": "VKID"},
    {"uid": "10005", "category": -2147483648, "part": "P8", "dt": "2020-05-01", "uid_type": "VKID"},
    {"uid": "10006", "category": None, "part": "P8", "dt": "2020-05-01", "uid_type": "VKID"},
    # P9
    {"uid": "10001", "score": 0.01, "category": 0, "part": "P9", "dt": "2020-05-01", "uid_type": "VKID"},
    {"uid": "10002", "score": 0.1, "category": 1, "part": "P9", "dt": "2020-05-01", "uid_type": "VKID"},
    {"uid": "10003", "score": -0.1, "category": -1, "part": "P9", "dt": "2020-05-01", "uid_type": "VKID"},
    {"uid": "10004", "score": 0.2, "category": 2147483647, "part": "P9", "dt": "2020-05-01", "uid_type": "VKID"},
    {"uid": "10005", "score": -0.2, "category": -2147483648, "part": "P9", "dt": "2020-05-01", "uid_type": "VKID"},
    # P19
    {"uid": "9223372036854775807", "score": 0.91, "part": "P19", "dt": "2020-05-01", "uid_type": "HID"},
    {"uid": "-9223372036854775808", "score": 0.92, "part": "P19", "dt": "2020-05-01", "uid_type": "HID"},
    # P20
    {"uid": "10001", "score": 0.10, "part": "P20", "dt": "2020-06-01", "uid_type": "HID"},
    {"uid": "10002", "score": 0.11, "part": "P20", "dt": "2020-06-01", "uid_type": "HID"},
    {"uid": "10001", "score": 0.12, "part": "P20", "dt": "2020-05-31", "uid_type": "HID"},
    {"uid": "10002", "score": 0.13, "part": "P20", "dt": "2020-05-31", "uid_type": "HID"},
]

FILTER_TABLE = "grinder_{}_filter".format(NAME)
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

MATCHING_TABLE = "grinder_{}_matching".format(NAME)
MATCHING_SCHEMA = (
    ("internal_link_id", "string"),
    ("external_link_id", "string"),
    ("dt", "string"),
)
MATCHING_PARTITION_COLUMNS = ["dt"]
MATCHING_DATA = [
    {"internal_link_id": "11", "external_link_id": "eleven", "dt": "2020-05-01"},
    {"internal_link_id": "22", "external_link_id": "twentytwo", "dt": "2020-05-01"},
    {"internal_link_id": "222", "external_link_id": "twentytwo", "dt": "2020-05-01"},
    {"internal_link_id": "foo", "external_link_id": "bar", "dt": "2020-05-01"},
]


def generate_test_data(tmp_dir=TMP_HDFS_DIR):
    def abs_tmp_path(*dirs):
        return os.path.join(tmp_dir, *dirs)

    data_specs = [
        {
            "source_where": "uid = '10001'",
            "feature_name": "1_sf",
            "source_partition_conf": {"uid_type": "VKID", "part": "P1"},
            "combine_columns": {
                "uid": "uid",
                "uid_type": "uid_type",
                "feature": "max(score)",
            },
            "filter_config": {
                "db": TEST_DB,
                "table": FILTER_TABLE,
                "partition_conf": {"id_type": "VKID"},
                "columns": {"uid": "id", "uid_type": "id_type"},
            },
            "expected_data": [
                {"uid": "10001", "score": 0.91, "feature_name": "1_sf", "dt": TARGET_DT, "uid_type": "VKID"},
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
            "filter_config": {
                "db": TEST_DB,
                "table": FILTER_TABLE,
                "partition_conf": {"id_type": "VKID"},
                "columns": {"uid": "id", "uid_type": "id_type"},
            },
            "expected_data": [
                {"uid": "10001", "score": None, "score_list": None, "score_map": {"1": 0.91}, "cat_list": None, "feature_name": "2_map", "dt": TARGET_DT, "uid_type": "VKID"},
                {"uid": "10002", "score": None, "score_list": None, "score_map": {"92": 0.2}, "cat_list": None, "feature_name": "2_map", "dt": TARGET_DT, "uid_type": "VKID"},
                {"uid": "10003", "score": None, "score_list": None, "score_map": {"1": 0.2}, "cat_list": None, "feature_name": "2_map", "dt": TARGET_DT, "uid_type": "VKID"},
                {"uid": "10004", "score": None, "score_list": None, "score_map": {"1": 0.2}, "cat_list": None, "feature_name": "2_map", "dt": TARGET_DT, "uid_type": "VKID"},
                {"uid": "10009", "score": None, "score_list": None, "score_map": {"2": 0.2, "3": 0.3}, "cat_list": None, "feature_name": "2_map", "dt": TARGET_DT, "uid_type": "VKID"},
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
            "filter_config": {
                "db": TEST_DB,
                "table": FILTER_TABLE,
                "partition_conf": {"id_type": "VKID"},
                "columns": {"uid": "id", "uid_type": "id_type"},
            },
            "expected_data": [
                {"uid": "10001", "score": None, "score_list": [1.0, 0.91], "score_map": None, "cat_list": None, "feature_name": "3_arr", "dt": TARGET_DT, "uid_type": "VKID"},
                {"uid": "10002", "score": None, "score_list": [0.92, 2.0], "score_map": None, "cat_list": None, "feature_name": "3_arr", "dt": TARGET_DT, "uid_type": "VKID"},
                {"uid": "10006", "score": None, "score_list": [1.0, 2.0], "score_map": None, "cat_list": None, "feature_name": "3_arr", "dt": TARGET_DT, "uid_type": "VKID"},
            ],
        },  # 3 array<float>
        {
            "feature_name": "4_arr",
            "source_partition_conf": {"uid_type": ["VKID", "HID"], "part": "P4"},
            "combine_columns": {
                "uid": "uid",
                "uid_type": "uid_type",
                "feature": "first(cat_list)",
            },
            "expected_data": [
                {"uid": "10001", "score": None, "score_list": None, "score_map": None, "cat_list": ["1", "91"], "feature_name": "4_arr", "dt": TARGET_DT, "uid_type": "VKID"},
                {"uid": "10002", "score": None, "score_list": None, "score_map": None, "cat_list": ["92", "2"], "feature_name": "4_arr", "dt": TARGET_DT, "uid_type": "VKID"},
                {"uid": "10008", "score": None, "score_list": None, "score_map": None, "cat_list": ["1", "2"], "feature_name": "4_arr", "dt": TARGET_DT, "uid_type": "VKID"},
                {"uid": "9223372036854775807", "score": None, "score_list": None, "score_map": None, "cat_list": ["3", "4"], "feature_name": "4_arr", "dt": TARGET_DT, "uid_type": "HID"},
                {"uid": "-9223372036854775808", "score": None, "score_list": None, "score_map": None, "cat_list": ["5", "6"], "feature_name": "4_arr", "dt": TARGET_DT, "uid_type": "HID"},
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
                {"uid": "10001", "score": None, "score_list": [0.91, 0.2], "score_map": None, "cat_list": None, "feature_name": "5_arr", "dt": TARGET_DT, "uid_type": "VKID"},
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
                {"uid": "10001", "score": None, "score_list": None, "score_map": {"2622836501": 0.1}, "cat_list": None, "feature_name": "6_map", "dt": TARGET_DT, "uid_type": "VKID"},
                {"uid": "10001", "score": None, "score_list": None, "score_map": {"1633052041": 0.2}, "cat_list": None, "feature_name": "6_map", "dt": TARGET_DT, "uid_type": "OKID"},
                {"uid": "10001", "score": None, "score_list": None, "score_map": {"1771093052": 0.3, "176538449": 0.4}, "cat_list": None, "feature_name": "6_map", "dt": TARGET_DT, "uid_type": "HID"},
            ],
        },  # 6 hash map keys
        {
            "feature_hashing": True,
            "feature_name": "7_arr",
            "source_partition_conf": {"part": "P7"},
            "combine_columns": {
                "uid": "uid",
                "uid_type": "uid_type",
                "feature": "first(cat_list)",
            },
            "expected_data": [
                {"uid": "10001", "cat_list": ["1771093052", "1075781878"], "feature_name": "7_arr", "dt": TARGET_DT, "uid_type": "VKID"},
                {"uid": "10002", "cat_list": ["549400414", "382493853"], "feature_name": "7_arr", "dt": TARGET_DT, "uid_type": "VKID"},
                {"uid": "10008", "cat_list": ["2622836501", "382493853"], "feature_name": "7_arr", "dt": TARGET_DT, "uid_type": "VKID"},
            ],
        },  # 7 hash array values
        {
            "feature_name": "8_arr",
            "source_partition_conf": {"part": "P8"},
            "combine_columns": {
                "uid": "uid",
                "uid_type": "uid_type",
                "feature": "collect_list(category)",
            },
            "expected_data": [
                {"uid": "10001", "cat_list": ["0"], "feature_name": "8_arr", "dt": TARGET_DT, "uid_type": "VKID"},
                {"uid": "10002", "cat_list": ["1"], "feature_name": "8_arr", "dt": TARGET_DT, "uid_type": "VKID"},
                {"uid": "10004", "cat_list": ["2147483647"], "feature_name": "8_arr", "dt": TARGET_DT, "uid_type": "VKID"},
            ],
        },  # 8 int input, no hash
        {
            "feature_hashing": True,
            "feature_name": "9_arr",
            "source_partition_conf": {"part": "P8"},
            "combine_columns": {
                "uid": "uid",
                "uid_type": "uid_type",
                "feature": "collect_list(category)",
            },
            "expected_data": [
                {"uid": "10001", "cat_list": ["3222849387"], "feature_name": "9_arr", "dt": TARGET_DT, "uid_type": "VKID"},
                {"uid": "10002", "cat_list": ["2622836501"], "feature_name": "9_arr", "dt": TARGET_DT, "uid_type": "VKID"},
                {"uid": "10003", "cat_list": ["1771093052"], "feature_name": "9_arr", "dt": TARGET_DT, "uid_type": "VKID"},
                {"uid": "10004", "cat_list": ["2460312044"], "feature_name": "9_arr", "dt": TARGET_DT, "uid_type": "VKID"},
                {"uid": "10005", "cat_list": ["3194330916"], "feature_name": "9_arr", "dt": TARGET_DT, "uid_type": "VKID"},
            ],
        },  # 9 int input, hash
        {
         "feature_name": "10_map",
         "source_partition_conf": {"part": "P9"},
         "max_collection_size": 9,
         "combine_columns": {
             "uid": "uid",
             "uid_type": "uid_type",
             "feature": "user_dmdesc.collect(category, score)",
         },
         "expected_data": [
             {"uid": "10001", "score_map": {"0": 0.01}, "feature_name": "10_map", "dt": TARGET_DT, "uid_type": "VKID"},
             {"uid": "10002", "score_map": {"1": 0.1}, "feature_name": "10_map", "dt": TARGET_DT, "uid_type": "VKID"},
             {"uid": "10004", "score_map": {"2147483647": 0.2}, "feature_name": "10_map", "dt": TARGET_DT, "uid_type": "VKID"},
         ],
        },  # 10 int group to map keys
        {
            "feature_hashing": True,
            "feature_name": "11_map",
            "source_partition_conf": {"part": "P9"},
            "max_collection_size": 9,
            "combine_columns": {
                "uid": "uid",
                "uid_type": "uid_type",
                "feature": "user_dmdesc.collect(category, score)",
            },
            "expected_data": [
                {"uid": "10001", "score_map": {"3222849387": 0.01}, "feature_name": "11_map", "dt": TARGET_DT, "uid_type": "VKID"},
                {"uid": "10002", "score_map": {"2622836501": 0.1}, "feature_name": "11_map", "dt": TARGET_DT, "uid_type": "VKID"},
                {"uid": "10003", "score_map": {"1771093052": -0.1}, "feature_name": "11_map", "dt": TARGET_DT, "uid_type": "VKID"},
                {"uid": "10004", "score_map": {"2460312044": 0.2}, "feature_name": "11_map", "dt": TARGET_DT, "uid_type": "VKID"},
                {"uid": "10005", "score_map": {"3194330916": -0.2}, "feature_name": "11_map", "dt": TARGET_DT, "uid_type": "VKID"},
            ],
        },  # 11 int group to map keys, hash
        {
            "feature_name": "12_sf",
            "source_partition_conf": {"uid_type": "VKID", "part": "P1"},
            "combine_columns": {
                "uid": "uid",
                "uid_type": "uid_type",
                "feature": "max(score)",
            },
            "expected_data": [
                {"uid": "10001", "score": 0.91, "feature_name": "12_sf", "dt": TARGET_DT, "uid_type": "VKID"},
                {"uid": "10002", "score": 0.92, "feature_name": "12_sf", "dt": TARGET_DT, "uid_type": "VKID"},
                {"uid": "12312", "score": 0.99, "feature_name": "12_sf", "dt": TARGET_DT, "uid_type": "VKID"},
            ],
        },  # 12 simple float, no filter
        {
         "feature_name": "13_sf",
         "source_partition_conf": {"uid_type": "VKID", "part": "P1"},
         "combine_columns": {
             "uid": "uid",
             "uid_type": "uid_type",
             "bu_link_id": {
                 "expr": "cast(bu_link_id as string)"
             },
             "feature": "max(score)",
         },
         "filter_config": {
             "db": TEST_DB,
             "table": FILTER_TABLE,
             "partition_conf": {"id_type": "VKID"},
             "columns": {"uid": "id", "uid_type": "id_type"},
         },
         "expected_data": [
             {"uid": "10001", "bu_link_id": "11", "score": 0.91, "feature_name": "13_sf", "dt": TARGET_DT, "uid_type": "VKID"},
             {"uid": "10002", "bu_link_id": "22", "score": 0.92, "feature_name": "13_sf", "dt": TARGET_DT, "uid_type": "VKID"},
             {"uid": "10002", "bu_link_id": "222", "score": 0.92, "feature_name": "13_sf", "dt": TARGET_DT, "uid_type": "VKID"},
         ],
        },  # 13 simple float, combine_columns.bu_link_id w/o matching
        {
         "feature_name": "14_sf",
         "source_partition_conf": {"uid_type": "VKID", "part": "P1"},
         "combine_columns": {
             "uid": "uid",
             "uid_type": "uid_type",
             "bu_link_id": {
                 "expr": "cast(bu_link_id as string)",
                 "matching_config": {
                     "db": TEST_DB,
                     "table": MATCHING_TABLE,
                     "partition_conf": {},
                     "join_conf": {"type": "regular"},
                     "bu_link_id_from": "cast(internal_link_id as string)",
                     "bu_link_id_to": "cast(external_link_id as string)",
                 }
             },
             "feature": "sum(score)",
         },
         "filter_config": {
             "db": TEST_DB,
             "table": FILTER_TABLE,
             "where": "id = '10001'",
             "partition_conf": {"id_type": "VKID"},
             "columns": {"uid": "id", "uid_type": "id_type"},
         },
         "expected_data": [
             {"uid": "10001", "bu_link_id": "eleven", "score": 0.91, "feature_name": "14_sf", "dt": TARGET_DT, "uid_type": "VKID"},
         ],
        },  # 14 simple float, combine_columns.bu_link_id with matching
        {
         "feature_name": "15_sf",
         "source_partition_conf": {"uid_type": "VKID", "part": "P1"},
         "combine_columns": {
             "uid": "uid",
             "uid_type": "uid_type",
             "bu_link_id": {
                 "expr": "cast(bu_link_id as string)",
                 "matching_config": {
                     "db": TEST_DB,
                     "table": MATCHING_TABLE,
                     "where": "external_link_id in ('eleven', 'twentytwo')",
                     "partition_conf": {},
                     "join_conf": {"type": "broadcast"},
                     "bu_link_id_from": "cast(internal_link_id as string)",
                     "bu_link_id_to": "cast(external_link_id as string)"
                 }
             },
             "feature": "max(score)",
         },
         "filter_config": None,
         "expected_data": [
             {"uid": "10001", "bu_link_id": "eleven", "score": 0.91, "feature_name": "15_sf", "dt": TARGET_DT, "uid_type": "VKID"},
             {"uid": "10002", "bu_link_id": "twentytwo", "score": 0.92, "feature_name": "15_sf", "dt": TARGET_DT, "uid_type": "VKID"},
         ],
        },  # 15 simple float, combine_columns.bu_link_id with matching, w/o filter-by-join
        {
            "feature_name": "16_map",
            "source_partition_conf": {"uid_type": "VKID", "part": "P2"},
            "combine_columns": {
                "uid": "uid",
                "uid_type": "uid_type",
                "bu_link_id": {
                    "expr": "cast(bu_link_id as string)",
                    "matching_config": {
                        "db": TEST_DB,
                        "table": MATCHING_TABLE,
                        "partition_conf": {},
                        "join_conf": {"type": "skewed"},
                        "bu_link_id_from": "cast(internal_link_id as string)",
                        "bu_link_id_to": "cast(external_link_id as string)",
                    }
                },
                "feature": "gavg(score_map)",
            },
            "filter_config": {
                "db": TEST_DB,
                "table": FILTER_TABLE,
                "partition_conf": {"id_type": "VKID"},
                "columns": {"uid": "id", "uid_type": "id_type"},
            },
            "expected_data": [
                {"uid": "10001", "bu_link_id": "eleven", "score_map": {"1": 0.91}, "feature_name": "16_map", "dt": TARGET_DT, "uid_type": "VKID"},
                {"uid": "10002", "bu_link_id": "twentytwo", "score_map": {"92": 0.2}, "feature_name": "16_map", "dt": TARGET_DT, "uid_type": "VKID"},
            ],
        },  # 16 map<string,float>, combine_columns.bu_link_id with matching
        {
            "feature_name": "17_arr",
            "source_partition_conf": {"uid_type": "VKID", "part": "P3"},
            "combine_columns": {
                "uid": "uid",
                "uid_type": "uid_type",
                "bu_link_id": {
                    "expr": "cast(bu_link_id as string)",
                    "matching_config": {
                        "db": TEST_DB,
                        "table": MATCHING_TABLE,
                        "partition_conf": {},
                        "join_conf": {"type": "regular"},
                        "bu_link_id_from": "cast(internal_link_id as string)",
                        "bu_link_id_to": "cast(external_link_id as string)",
                    }
                },
                "feature": "first(score_list)",
            },
            "filter_config": {
                "db": TEST_DB,
                "table": FILTER_TABLE,
                "partition_conf": {"id_type": "VKID"},
                "columns": {"uid": "id", "uid_type": "id_type"},
            },
            "expected_data": [
                {"uid": "10001", "bu_link_id": "eleven", "score_list": [1.0, 0.91], "feature_name": "17_arr", "dt": TARGET_DT, "uid_type": "VKID"},
                {"uid": "10002", "bu_link_id": "twentytwo", "score_list": [0.92, 2.0], "feature_name": "17_arr", "dt": TARGET_DT, "uid_type": "VKID"},
            ],
        },  # 17 array<float>, combine_columns.bu_link_id with matching
        {
            "feature_name": "18_arr",
            "feature_hashing": True,
            "source_partition_conf": {"part": "P7"},
            "combine_columns": {
                "uid": "uid",
                "uid_type": "uid_type",
                "bu_link_id": {
                    "expr": "cast(bu_link_id as string)",
                    "matching_config": {
                        "db": TEST_DB,
                        "table": MATCHING_TABLE,
                        "partition_conf": {},
                        "join_conf": {"type": "regular"},
                        "bu_link_id_from": "cast(internal_link_id as string)",
                        "bu_link_id_to": "cast(external_link_id as string)",
                    }
                },
                "feature": "first(cat_list)",
            },
            "expected_data": [
                {"uid": "10001", "bu_link_id": "eleven", "cat_list": ["1771093052", "1075781878"], "feature_name": "18_arr", "dt": TARGET_DT, "uid_type": "VKID"},
                {"uid": "10002", "bu_link_id": "twentytwo", "cat_list": ["549400414", "382493853"], "feature_name": "18_arr", "dt": TARGET_DT, "uid_type": "VKID"},
            ],
        },  # 18 hash array values, combine_columns.bu_link_id with matching
        {
            "feature_name": "19_sf",
            "source_partition_conf": {"uid_type": "HID", "part": "P19"},
            "combine_columns": {
                "uid": "uid",
                "uid_type": "uid_type",
                "feature": "max(score)",
            },
            "expected_data": [
                {"uid": "9223372036854775807", "score": 0.91, "feature_name": "19_sf", "dt": TARGET_DT, "uid_type": "HID"},
                {"uid": "-9223372036854775808", "score": 0.92, "feature_name": "19_sf", "dt": TARGET_DT, "uid_type": "HID"},
            ],
        },  # 19 simple float, HID
        {
            "feature_name": "20_sf",
            "source_partition_conf": {"part": "P20"},
            "period": 2,
            "dt_selection_mode": "multiple_any",
            "combine_columns": {
                "uid": "uid",
                "uid_type": "uid_type",
                "feature": "avg(score)",
            },
            "expected_data": [
                {"uid": "10001", "score": 0.11, "feature_name": "20_sf", "dt": TARGET_DT, "uid_type": "HID"},
                {"uid": "10002", "score": 0.12, "feature_name": "20_sf", "dt": TARGET_DT, "uid_type": "HID"},
            ],
        },  # 20 aggregate period=2
        {
            "feature_name": "21_sf",
            "source_partition_conf": {"part": "P20"},
            "period": 1,
            "dt_selection_mode": "multiple_any",
            "combine_columns": {
                "uid": "uid",
                "uid_type": "uid_type",
                "feature": "avg(score)",
            },
            "expected_data": [
                {"uid": "10001", "score": 0.10, "feature_name": "21_sf", "dt": TARGET_DT, "uid_type": "HID"},
                {"uid": "10002", "score": 0.11, "feature_name": "21_sf", "dt": TARGET_DT, "uid_type": "HID"},
            ],
        },  # 21 aggregate period=1
    ]

    # list of (app_cfg, expected_cfg, idx)
    data = [
        (
            {
                "feature_name": spec["feature_name"],
                "feature_hashing": spec.get("feature_hashing", False),

                "target_dt": TARGET_DT,
                "target_db": TEST_DB,
                "target_table": TARGET_TABLE,

                "source_db": TEST_DB,
                "source_table": SOURCE_TABLE,
                "source_where": spec.get("source_where"),
                "source_partition_conf": spec["source_partition_conf"],
                "period": spec.get("period", 365),
                "dt_selection_mode": spec.get("dt_selection_mode", "single_last"),

                "combine_columns": spec["combine_columns"],

                "filter_config": spec.get("filter_config", {}),

                "min_target_rows": spec.get("min_target_rows", 1),
                "max_collection_size": spec.get("max_collection_size", 2),
                "shuffle_partitions": spec.get("shuffle_partitions", 2),

                "ctid": "ctid_{}".format(i),
                "input_urls": [abs_tmp_path("input_{}".format(i))],
                "output_urls": [abs_tmp_path("output_{}".format(i))],
                "status_urls": [abs_tmp_path("status_{}".format(i))],
            },
            spec,
            i,
        )
        for i, spec in enumerate(data_specs, 1)
    ]
    print("\n\nUniversal features test data generated.\n")
    return data
