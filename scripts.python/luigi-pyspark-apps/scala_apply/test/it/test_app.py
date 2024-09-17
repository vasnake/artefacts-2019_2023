import os
import six
import uuid
import pytest
import pandas as pd

from backports import tempfile
from collections import OrderedDict

from prj.adapters.utils import construct
from prj.apps.apply.app import ApplyTask
from prj.apps.learn.test import make_lal_learnset
from prj.adapters.dataset import LookalikeLearnsetAdapter

from .data import TABLES
from .service import TableWrapper, HadoopEnvironment, HdfsClient, parse_df, show_pandas_df


class TestApplyTask(object):
    _verbose = True

    NAME = "e2e_apply_app"
    DT = "2020-10-15"
    UID_TYPE = "HID"
    TEST_DB = "local_db"
    TARGET_TABLE = "audience_table"
    SOURCE_TABLE = "features_e2e"

    MODEL_NAMES = ["mnb", "sgd"]
    MODEL_CONFIGS = [
        {
            "py_class": "prj.adapters.model.lal.LookalikeMultinomialNB",
            "kwargs": {
                "estimator": {
                    "postprocessors": [
                        "__default__",
                        ["equalizer", {"noise": 0}]
                    ]
                }
            }
        },
        {
            "py_class": "prj.adapters.model.lal.LookalikeSGDClassifier",
            "kwargs": {
                "estimator": {
                    "postprocessors": [
                        "__default__",
                        ["equalizer", {"noise": 0}]
                    ]
                }
            }
        }
    ]

    TMP_HDFS_DIR_ROOT = None
    TMP_HDFS_DIR = None
    MODELS_DIR = None

    expected = """
+-----+-------------------+--------------------------------------------+---------------------+-------------+--------+----------+--------+
|uid  |score              |scores_raw                                  |scores_trf           |audience_name|category|dt        |uid_type|
+-----+-------------------+--------------------------------------------+---------------------+-------------+--------+----------+--------+
|uid_0|0.7945             |[0.030531482450085225, 0.9694685175499154]  |[0.7945]             |mnb          |positive|2020-10-15|HID     |
|uid_1|0.023              |[0.862641917263964, 0.13735808273603686]    |[0.023]              |mnb          |positive|2020-10-15|HID     |
|uid_2|0.2585             |[0.7281617221224774, 0.2718382778775232]    |[0.2585]             |mnb          |positive|2020-10-15|HID     |
|uid_3|0.211              |[0.7457932207208225, 0.25420677927917756]   |[0.211]              |mnb          |positive|2020-10-15|HID     |
|uid_4|0.902              |[0.002287510705929438, 0.997712489294072]   |[0.902]              |mnb          |positive|2020-10-15|HID     |
|uid_5|0.3035             |[0.7131917744089509, 0.28680822559104985]   |[0.3035]             |mnb          |positive|2020-10-15|HID     |
|uid_6|0.8565             |[0.003608486379748199, 0.9963915136202518]  |[0.8565]             |mnb          |positive|2020-10-15|HID     |
|uid_7|0.889              |[0.002545009141356248, 0.9974549908586431]  |[0.889]              |mnb          |positive|2020-10-15|HID     |
|uid_8|0.729              |[0.04738902244103701, 0.9526109775589624]   |[0.729]              |mnb          |positive|2020-10-15|HID     |
|uid_9|0.3035             |[0.7131917744089509, 0.28680822559104985]   |[0.3035]             |mnb          |positive|2020-10-15|HID     |
|uid_0|0.757              |[3.001154880166723E-11, 0.9999999999699885] |[0.757]              |sgd          |positive|2020-10-15|HID     |
|uid_1|0.6180000058020415 |[1.4503392298359508E-5, 0.9999854966077016] |[0.6180000058020415] |sgd          |positive|2020-10-15|HID     |
|uid_2|0.3025000135742895 |[0.9999999998456682, 1.543318068229884E-10] |[0.3025000135742895] |sgd          |positive|2020-10-15|HID     |
|uid_3|0.29350001743345344|[0.9999999999335232, 6.647678874272872E-11] |[0.29350001743345344]|sgd          |positive|2020-10-15|HID     |
|uid_4|0.7995             |[2.9265478929119126E-13, 0.9999999999997073]|[0.7995]             |sgd          |positive|2020-10-15|HID     |
|uid_5|0.44750007068173825|[0.9970256122036304, 0.0029743877963695595] |[0.44750007068173825]|sgd          |positive|2020-10-15|HID     |
|uid_6|0.8745             |[0.0, 1.0]                                  |[0.8745]             |sgd          |positive|2020-10-15|HID     |
|uid_7|0.8745             |[0.0, 1.0]                                  |[0.8745]             |sgd          |positive|2020-10-15|HID     |
|uid_8|0.759              |[2.1695756302619884E-11, 0.9999999999783042]|[0.759]              |sgd          |positive|2020-10-15|HID     |
|uid_9|0.3059999807749249 |[0.9999999996885132, 3.114868339525113E-10] |[0.3059999807749249] |sgd          |positive|2020-10-15|HID     |
+-----+-------------------+--------------------------------------------+---------------------+-------------+--------+----------+--------+      
        """

    @pytest.mark.parametrize("apply_adapter, test_name", [
        ("prj.adapters.apply.PythonApplyAdapter", "python_apply"),
        ("prj.adapters.apply.ScalaApplyAdapter", "scala_apply")
    ])
    def test_apply_adapters(self, apply_adapter, test_name, local_spark, tmpdir):
        self.TMP_HDFS_DIR_ROOT = tmpdir.strpath
        self.TMP_HDFS_DIR = os.path.join(self.TMP_HDFS_DIR_ROOT, "tmp")
        self.MODELS_DIR = os.path.join(self.TMP_HDFS_DIR_ROOT, "models")

        task_params = {
            "dt": self.DT,
            "uid_type": self.UID_TYPE,
            "source_db": self.TEST_DB,
            "source_table": self.SOURCE_TABLE,
            "source_keep_columns": ["uid"],
            "target_db": self.TEST_DB,
            "target_table": self.TARGET_TABLE,
            "apply_adapter": {"py_class": apply_adapter},
            "model_batch": [
                {
                    "adapter": model,
                    "audience_name": model_name,
                    "hdfs_dir": os.path.join(self.MODELS_DIR, "{}_{}_{}".format(test_name, model_name, uuid.uuid4().hex))
                }
                for model_name, model in zip(self.MODEL_NAMES, self.MODEL_CONFIGS)
            ]
        }
        control_params = {
            "ctid": "{}__success".format(self.NAME),
            "status_urls": [os.path.join(self.TMP_HDFS_DIR_ROOT, "status__success__{}".format(x)) for x in self.MODEL_NAMES],
            "output_urls": [os.path.join(self.TMP_HDFS_DIR_ROOT, "output__success__{}".format(x)) for x in self.MODEL_NAMES],
            "input_urls": [],
            "force": False
        }
        if "Scala" in apply_adapter:
            task_params["fallback_apply_adapter"] = {"py_class": "prj.adapters.apply.PythonApplyAdapter"}

        self._prepare_environment(local_spark, task_params)

        task = ApplyTask(**dict(control_params, **task_params))
        task.tmp_hdfs_dir = self.TMP_HDFS_DIR
        task.main(local_spark.sparkContext)

        assert self._is_model_applied(pd.DataFrame(parse_df(self.expected)))

    def _prepare_environment(self, spark, task_params):
        # model train data
        train, gf, label_names = make_lal_learnset(n_feature_groups=2)
        grouped_features = OrderedDict([
            ("fg_{}".format(k), v) for k, v in six.iteritems(gf)
        ])
        learnset = LookalikeLearnsetAdapter().set_data(train, grouped_features, label_names)

        # model fit and save
        for m in task_params["model_batch"]:
            self._save_model(
                model=construct(m["adapter"]).learn(learnset),
                gf_adapter=learnset.grouped_features_adapter,
                hdfs_dir=m["hdfs_dir"]
            )

        # setup hive tables
        source_table_name = "{}.{}".format(self.TEST_DB, self.SOURCE_TABLE)
        tables = {source_table_name: {
            "schema": [(k, v) for k, v in TABLES[source_table_name]["schema"]],
            "partitions": TABLES[source_table_name]["partitions"],
            "rows": self._source_table_rows(train, grouped_features)
        }}
        HadoopEnvironment.instance.empty_table(self.TEST_DB, self.TARGET_TABLE)
        HadoopEnvironment.instance.write_table(
            self.TEST_DB, self.SOURCE_TABLE,
            TableWrapper(source_table_name, tables, spark).df,
            mode="overwrite"
        )

    def _save_model(self, model, gf_adapter, hdfs_dir):
        hdfs = HdfsClient()
        with tempfile.TemporaryDirectory() as tmp_local_dir:
            model.save(tmp_local_dir)
            gf_adapter.save(tmp_local_dir)
            hdfs.mkdir(hdfs_dir, remove_if_exists=True)
            hdfs.put(tmp_local_dir, hdfs_dir, content_only=True)

    def _source_table_rows(self, train, grouped_features, rows_count=10):
        return [
            {
                "dt": self.DT,
                "uid_type": self.UID_TYPE,
                "uid": "uid_{}".format(i),
                "fg_0": [row[int(k)] for k in grouped_features["fg_0"]],
                "fg_1": {k: row[int(k)] for k in grouped_features["fg_1"] if row[int(k)] != 0}
            }
            for i, row in zip(range(rows_count), train.x.toarray().tolist())
        ]

    def _is_model_applied(self, expected_pdf=None):
        sort_columns = ["audience_name", "uid_type", "uid"]
        actual_df = HadoopEnvironment.read_from_hive(self.TEST_DB, self.TARGET_TABLE).sort(*sort_columns)

        if self._verbose:
            print("\nIs model applied? Actual DF:")
            actual_df.show(200, truncate=False)
            if expected_pdf is not None:
                show_pandas_df(expected_pdf.sort_values(by=sort_columns))

        if expected_pdf is not None:
            actual_pdf = actual_df.toPandas()
            test_cols = ["audience_name", "category", "dt", "scores_raw", "uid", "uid_type", "score", "scores_trf"]
            pd.testing.assert_frame_equal(
                actual_pdf[test_cols],
                expected_pdf[test_cols],
                check_less_precise=2,
                check_like=True
            )
        return True
