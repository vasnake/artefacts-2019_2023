import os

from pyspark.sql.types import MapType, ArrayType, FloatType, DoubleType, StringType

from prj.apps.utils.control.client.status import FatalStatusException, MissingDepsStatusException

from prj.apps.utils.testing.defines import TEST_DB

NAME = "test_AUD_RB_app"
TARGET_DT = "2020-06-01"

# flake8: noqa
# @formatter:off
# fmt: off


def generate_test_data(tmp_dir):
    def temp_path(*dirs):
        return os.path.join(tmp_dir, *dirs)

    source_schema = (
        ("uid", "string"),
        ("score", "double"),
        ("audience_name", "string"),
        ("category", "string"),
        ("dt", "string"),
        ("uid_type", "string"),
    )
    source_partition_columns = ["audience_name", "category", "dt", "uid_type"]

    expected_success_file = "{success_hdfs_basedir}/{target_dt}/ExportAudienceRbTask_{audience_id}-{uid_type}-{target_dt}.success"
    expected_data_files = "task.*-{ctid}-RB-{audience_id}-{uid_type}-{target_dt}-*"
    extra_file = "task.20210625090719-RB-999-{uid_type}-{target_dt}-90909"
    previous_attempt_files = [
        "task.20210625090719-RB-{audience_id}-{uid_type}-{target_dt}-12345",
        "task.20210625090719-RB-{audience_id}-{uid_type}-{target_dt}-12345.success"
    ]

    data_specs = [
        {
            "audience_id": 123123,
            "life_time": 333,
            "exclude_audience_ids": [122, 124],
            "uid_type": "VKID",
            "source_partition_conf": {"audience_name": "A1"},
            "min_score": 0.0,
            "max_score": 1.0,
            "min_target_rows": 1,
            "max_target_rows": 2,
            "output_max_rows_per_file": 1,
            "source": {
                "data": [
                    {"uid": "10001", "score": 0.91, "audience_name": "A1", "category": "positive", "dt": "2020-05-01", "uid_type": "VKID",},
                    {"uid": "10002", "score": 0.91, "audience_name": "A1", "category": "positive", "dt": "2020-05-01", "uid_type": "OKID",},
                ],
            },
            "expected_data": [
                "vk\t10001\t123123\t333",
                "vk\t10001\t122\t-1",
                "vk\t10001\t124\t-1",
            ],
        },  # 1, VKID, delete list
        {
            "audience_id": 1234,
            "uid_type": "VID",
            "source_partition_conf": {"audience_name": "A1"},
            "min_score": -99.9,
            "max_score": 99.9,
            "min_target_rows": 1,
            "max_target_rows": 2,
            "output_max_rows_per_file": 2,
            "source": {
                "data": [
                    {"uid": "10001", "score": -9.1, "audience_name": "A1", "category": "positive", "dt": "2020-05-01", "uid_type": "VID"},
                    {"uid": "-10001", "score": 9.1, "audience_name": "A1", "category": "positive", "dt": "2020-05-01", "uid_type": "VID"},
                    {"uid": "10002", "score": 0.92, "audience_name": "A1", "category": "positive", "dt": "2020-05-01", "uid_type": "OKID"},
                ],
            },
            "expected_data": [
                "vid\t-10001\t1234\t33",
                "vid\t10001\t1234\t33",
            ],
        },  # 2, VID
        {
            "uid_type": "VID",
            "source_partition_conf": {"audience_name": "A1"},
            "source": {
                "data": [
                    {"uid": "10001", "score": 0.910, "audience_name": "A1", "category": "positive", "dt": "2020-05-01", "uid_type": "VID"},
                    {"uid": "-10002", "score": None, "audience_name": "A1", "category": "positive", "dt": "2020-05-01", "uid_type": "VID"},
                    {"uid": "10003", "score": -0.01, "audience_name": "A1", "category": "positive", "dt": "2020-05-01", "uid_type": "VID"},
                    {"uid": "10004", "score": 1.001, "audience_name": "A1", "category": "positive", "dt": "2020-05-01", "uid_type": "VID"},
                    {"uid": "10005", "score": float("nan"), "audience_name": "A1", "category": "positive", "dt": "2020-05-01", "uid_type": "VID"},
                ],
            },
            "expected_data": [
                "vid\t10001\t123\t33",
            ],
        },  # 3, invalid score
        {
            "max_target_rows": 2,
            "shuffle_partitions": 1,
            "uid_type": "VID",
            "source_partition_conf": {"audience_name": "A1"},
            "source": {
                "data": [
                    {"uid": "10001", "score": 0.910, "audience_name": "A1", "category": "positive", "dt": "2020-05-01", "uid_type": "VID"},
                    {"uid": "10002", "score": 0.010, "audience_name": "A1", "category": "positive", "dt": "2020-05-01", "uid_type": "VID"},
                    {"uid": "10003", "score": 0.011, "audience_name": "A1", "category": "positive", "dt": "2020-05-01", "uid_type": "VID"},
                    {"uid": "10004", "score": 0.009, "audience_name": "A1", "category": "positive", "dt": "2020-05-01", "uid_type": "VID"},
                ],
            },
            "expected_data": [
                "vid\t10001\t123\t33",
                "vid\t10003\t123\t33",
            ],
        },  # 4, top M records
        {
            "max_target_rows": 5,
            "uid_type": "VID",
            "source_partition_conf": {"category": "positive"},
            "source": {
                "data": [
                    {"uid": "10001", "score": 0.68, "audience_name": "A1", "category": "positive", "dt": "2020-05-01", "uid_type": "VID"},
                    {"uid": "10001", "score": 0.81, "audience_name": "A2", "category": "positive", "dt": "2020-05-01", "uid_type": "VID"},
                    {"uid": "10001", "score": 0.91, "audience_name": "A3", "category": "positive", "dt": "2020-05-01", "uid_type": "VID"},
                    {"uid": "10002", "score": 0.01, "audience_name": "A1", "category": "positive", "dt": "2020-05-01", "uid_type": "VID"},
                    {"uid": "10003", "score": 0.4, "audience_name": "A1", "category": "positive", "dt": "2020-05-01", "uid_type": "VID"},
                ],
            },
            "expected_data": [
                "vid\t10001\t123\t33",
                "vid\t10002\t123\t33",
                "vid\t10003\t123\t33",
            ],
        },  # 5, agg by user
        {
            "uid_type": "EMAIL",
            "source_partition_conf": {"category": "positive"},
            "source": {
                "data": [
                    {"uid": "foo@bar.baz", "score": 0.68, "audience_name": "A1", "category": "positive", "dt": "2020-05-01", "uid_type": "EMAIL"},
                    {"uid": "00D70A01E57B4A6BBDA12AA04CBEF113", "score": 0.81, "audience_name": "A2", "category": "positive", "dt": "2020-05-01", "uid_type": "IDFA"},
                    {"uid": "0039548579FC479BA594AB673BD3C255", "score": 0.91, "audience_name": "A3", "category": "positive", "dt": "2020-05-01", "uid_type": "GAID"},
                ],
            },
            "expected_data": [
                "email\tfoo@bar.baz\t123\t33",
            ],
        },  # 6, EMAIL
        {
            "uid_type": "IDFA",
            "source_partition_conf": {"category": "positive"},
            "source": {
                "data": [
                    {"uid": "foo@bar.baz", "score": 0.68, "audience_name": "A1", "category": "positive", "dt": "2020-05-01", "uid_type": "EMAIL"},
                    {"uid": "aabbccddeeff00112233445566778899", "score": 0.81, "audience_name": "A2", "category": "positive", "dt": "2020-05-01", "uid_type": "IDFA"},
                    {"uid": "0039548579FC479BA594AB673BD3C255", "score": 0.91, "audience_name": "A3", "category": "positive", "dt": "2020-05-01", "uid_type": "GAID"},
                ],
            },
            "expected_data": [
                "idfa\tAABBCCDD-EEFF-0011-2233-445566778899\t123\t33",
            ],
        },  # 7, IDFA
        {
            "uid_type": "GAID",
            "source_partition_conf": {"category": "positive"},
            "source": {
                "data": [
                    {"uid": "foo@bar.baz", "score": 0.68, "audience_name": "A1", "category": "positive", "dt": "2020-05-01", "uid_type": "EMAIL"},
                    {"uid": "00D70A01E57B4A6BBDA12AA04CBEF113", "score": 0.81, "audience_name": "A2", "category": "positive", "dt": "2020-05-01", "uid_type": "IDFA"},
                    {"uid": "0039548579FC479BA594AB673BD3C255", "score": 0.91, "audience_name": "A3", "category": "positive", "dt": "2020-05-01", "uid_type": "GAID"},
                ],
            },
            "expected_data": [
                "gaid\t00395485-79fc-479b-a594-ab673bd3c255\t123\t33",
            ],
        },  # 8, GAID
        {
            "uid_type": "VKID",
            "audience_id": 99,
            "life_time": 11,
            "source_partition_conf": {"audience_name": "A1"},
            "min_target_rows": 1,
            "max_target_rows": 5,
            "output_max_rows_per_file": 1,
            "source": {
                "data": [
                    {"uid": "10001", "score": 0.91, "audience_name": "A1", "category": "positive", "dt": "2020-05-01", "uid_type": "VKID",},
                    {"uid": "10002", "score": 0.91, "audience_name": "A1", "category": "positive", "dt": "2020-05-01", "uid_type": "VKID",},
                    {"uid": "10003", "score": 0.91, "audience_name": "A1", "category": "positive", "dt": "2020-05-01", "uid_type": "VKID",},
                    {"uid": "10004", "score": 0.91, "audience_name": "A1", "category": "positive", "dt": "2020-05-01", "uid_type": "VKID",},
                    {"uid": "10005", "score": 0.91, "audience_name": "A1", "category": "positive", "dt": "2020-05-01", "uid_type": "VKID",},
                ],
                "schema": source_schema,
                "partition_columns": source_partition_columns,
            },
            "expected_data": [
                "vk\t10001\t99\t11",
                "vk\t10002\t99\t11",
                "vk\t10003\t99\t11",
                "vk\t10004\t99\t11",
                "vk\t10005\t99\t11",
            ],
        },  # 9, VKID multiple files
        {
            "uid_type": "VID",
            "source_partition_conf": {"audience_name": "A1"},
            "source": {
                "data": [
                    {"uid": "10001", "score": 0.91, "audience_name": "A1", "category": "positive", "dt": "2020-05-01", "uid_type": "OKID"},
                    {"uid": "-10001", "score": 0.92, "audience_name": "A1", "category": "positive", "dt": "2020-05-01", "uid_type": "VKID"},
                    {"uid": "10002", "score": 0.93, "audience_name": "A1", "category": "positive", "dt": "2020-05-01", "uid_type": "HID"},
                ],
            },
            "expect_fatal": True,
            "exception": MissingDepsStatusException,
        },  # 10, empty source, fatal
        {
            "uid_type": "HID",
            "source_partition_conf": {"audience_name": "A1"},
            "source": {
                "data": [
                    {"uid": "10001", "score": 0.91, "audience_name": "A1", "category": "positive", "dt": "2020-05-01", "uid_type": "HID"},
                    {"uid": "10002", "score": 0.92, "audience_name": "A1", "category": "positive", "dt": "2020-05-01", "uid_type": "VID"},
                    {"uid": "10003", "score": 0.93, "audience_name": "A1", "category": "positive", "dt": "2020-05-01", "uid_type": "OKID"},
                ],
            },
            "expect_fatal": True,
            "exception": FatalStatusException,
        },  # 11, invalid uid_type, fatal
        {
            "min_target_rows": 3,
            "uid_type": "VID",
            "source_partition_conf": {"audience_name": "A1"},
            "source": {
                "data": [
                    {"uid": "10001", "score": 0.910, "audience_name": "A1", "category": "positive", "dt": "2020-05-01", "uid_type": "VID"},
                    {"uid": "-10002", "score": 0.01, "audience_name": "A1", "category": "positive", "dt": "2020-05-01", "uid_type": "VID"},
                ],
            },
            "expect_fatal": True,
            "exception": FatalStatusException,
        },  # 12, min records limit, fatal
    ]

    test_data = []  # (task_config, test_spec, i, )
    for i, spec in enumerate(data_specs, 1):
        spec["source"]["schema"] = spec["source"].get("schema", source_schema)
        spec["source"]["partition_columns"] = spec["source"].get("partition_columns", source_partition_columns)
        spec["expected_success_file"] = spec.get("expected_success_file", expected_success_file)
        spec["expected_data_files"] = spec.get("expected_data_files", expected_data_files)
        spec["previous_attempt_files"] = spec.get("previous_attempt_files", previous_attempt_files)
        spec["extra_file"] = spec.get("extra_file", extra_file)

        task_config = {
            "audience_id": spec.get("audience_id", 123),
            "life_time": spec.get("life_time", 33),
            "exclude_audience_ids": spec.get("exclude_audience_ids", []),
            "uid_type": spec.get("uid_type", "EMAIL"),
            "target_dt": spec.get("target_dt", TARGET_DT),
            "target_hdfs_basedir": temp_path("result_{}".format(i), ""),
            "success_hdfs_basedir": temp_path("success_{}".format(i), ""),
            "source_db": TEST_DB,
            "source_table": "grinder_{}_source_v{}".format(NAME, i),
            "source_partition_conf": spec["source_partition_conf"],
            "min_score": spec.get("min_score", 0.0),
            "max_score": spec.get("max_score", 1.0),
            "max_target_rows": spec.get("max_target_rows", 2),
            "min_target_rows": spec.get("min_target_rows", 1),
            "max_dt_diff": spec.get("max_dt_diff", 60),
            "shuffle_partitions": spec.get("shuffle_partitions", 1),
            "output_max_rows_per_file": spec.get("output_max_rows_per_file", 1),
            "ctid": "ctid_{}".format(i),
            "input_urls": [temp_path("input_{}".format(i))],
            "output_urls": [temp_path("output_{}".format(i))],
            "status_urls": [temp_path("status_{}".format(i))],
        }
        for n in [
            "expected_data_files",
            "expected_success_file",
            "previous_attempt_files",
            "extra_file",
        ]:
            if isinstance(spec[n], list):
                spec[n] = [x.format(**task_config) for x in spec[n]]
            else:
                spec[n] = spec[n].format(**task_config)

        test_data.append((task_config, spec, i, ))  # fmt: skip

    return test_data

# @formatter:on
# fmt: on
