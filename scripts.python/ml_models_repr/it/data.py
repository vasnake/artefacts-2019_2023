from pyspark.sql.types import StringType, MapType, FloatType, DoubleType, ArrayType

TABLES = {
    "local_db.features_table": {
        "schema": [
            ("uid", StringType()),
            ("0", MapType(StringType(), DoubleType())),
            ("1", ArrayType(DoubleType())),
            ("dt", StringType()),
            ("uid_type", StringType())
        ],
        "partitions": ["dt", "uid_type"],
        "rows": [
            ("a", {"0": 3.14, "2": 0.9, "9": 99.9}, [float(x) for x in range(20)], "2020-10-15", "VID")
        ]
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
            ("scores_trf", ArrayType(DoubleType()))
        ],
        "partitions": ["audience_name", "category", "dt", "uid_type"],
        "rows": []
    },
    "snb_cdm_activity.activity_agg": {
        "schema": [
            ("uid", StringType()),
            ("score", MapType(StringType(), FloatType())),
            ("source_name", StringType()),
            ("activity", StringType()),
            ("aggregation_period", StringType()),
            ("dt", StringType()),
            ("uid_type", StringType())
        ],
        "partitions": ["source_name", "activity", "aggregation_period", "dt", "uid_type"],
        "rows": [
            # @formatter:off
            ("d", {"2020-07-07": 23.0, "2020-07-08": 50.0}, "FOO", "BAR", "3_days", "2020-07-09", "VID"),
            ("a", {"2020-07-07": 43.0, "2020-07-08": 50.0, "2020-07-09": 19.0}, "ANTIFRAUD", "SHOW", "3_days", "2020-07-09", "OKID"),
            ("b", {"2020-07-07": 40.0, "2020-07-08": 20.0, "2020-07-09": 15.0}, "ANTIFRAUD", "SHOW", "3_days", "2020-07-09", "VKID"),
            ("c", {"2020-07-07": 49.0, "2020-07-08": 30.0, "2020-07-09": 10.0}, "ANTIFRAUD", "SHOW", "3_days", "2020-07-09", "VID")
            # @formatter:on
        ]
    },
    "snb_ds_auditories.active_audience": {
        "schema": [
            ("uid", StringType()),
            ("score", DoubleType()),
            ("audience_name", StringType()),
            ("category", StringType()),
            ("dt", StringType()),
            ("uid_type", StringType())
        ],
        "partitions": ["audience_name", "category", "dt", "uid_type"],
        "rows": []
    }
}
