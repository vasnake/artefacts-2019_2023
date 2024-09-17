import os
import pytest
import pandas as pd

from prj.apps.learn.app import LearnTask
from prj.apps.apply.app import ApplyTask

from .service import HdfsClient


class TestLearnApply(object):

    STASH_DIR = "/tmp/DM-8178-scala-apply-apps/"

    def test_learn_sgd_classifier(self, local_spark, tmpdir):
        data_dir, model_dir = (os.path.join(tmpdir.strpath, x) for x in ("data", "model"))
        self._create_learnset(data_dir)

        task_params = {
            "model": {
                "adapter": {
                    "py_class": "prj.adapters.model.lal.LookalikeSGDClassifier",
                    "kwargs": {
                        "estimator": {
                            "postprocessors": [
                                "__default__",
                                ["equalizer", {"noise": 0}]
                            ]
                        }
                    }
                },
                "hdfs_dir": model_dir
            },
            "dataset": {
                "adapter": {"py_class": "prj.adapters.dataset.LookalikeLearnsetAdapter"},
                "hdfs_dir": data_dir
            }
        }
        control_params = {
            "ctid": "learn_sgd_classifier",
            "status_urls": [],
            "input_urls": [],
            "output_urls": ["learn_results"],
            "force": False
        }

        task = LearnTask(**dict(
            control_params,
            **task_params
        ))

        task.main(local_spark.sparkContext)
        assert self._is_model_saved(model_dir)

        self._stash(model_dir, os.path.join(self.STASH_DIR, "sgd"))

    def test_learn_multinomial_nb(self, local_spark, tmpdir):
        data_dir, model_dir = (os.path.join(tmpdir.strpath, x) for x in ("data", "model"))
        self._create_learnset(data_dir)

        task_params = {
            "model": {
                "adapter": {
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
                "hdfs_dir": model_dir
            },
            "dataset": {
                "adapter": {"py_class": "prj.adapters.dataset.LookalikeLearnsetAdapter"},
                "hdfs_dir": data_dir
            }
        }
        control_params = {
            "ctid": "learn_multinomial_nb",
            "status_urls": [],
            "input_urls": [],
            "output_urls": ["learn_results"],
            "force": False
        }

        task = LearnTask(**dict(
            control_params,
            **task_params
        ))

        task.main(local_spark.sparkContext)
        assert self._is_model_saved(model_dir)

        self._stash(model_dir, os.path.join(self.STASH_DIR, "mnb"))

    def test_apply_multinomial_nb(self, local_spark, tmpdir):

        task_params = {
            "dt": "2020-10-15",
            "uid_type": "VID",
            "source_db": "local_db",
            "source_table": "features_table",
            "source_keep_columns": ["uid"],
            "target_db": "local_db",
            "target_table": "audience_table",
            "apply_adapter": {"py_class": "prj.adapters.apply.PythonApplyAdapter"},
            "model_batch": [{
                "audience_name": "audience_01",
                "adapter": {"py_class": "prj.adapters.model.lal.LookalikeMultinomialNB"},
                "hdfs_dir": os.path.join(self.STASH_DIR, "mnb")
            }]
        }

        task = ApplyTask(**dict(
            {
                "ctid": "apply_multinomial_nb",
                "status_urls": [os.path.join(tmpdir.strpath, "apply-multinomial-stat1")],
                "input_urls": [os.path.join(tmpdir.strpath, "apply-multinomial-inp1")],
                "output_urls": [os.path.join(tmpdir.strpath, "apply-multinomial-out1")],
                "force": False
            },
            **task_params
        ))

        task.tmp_hdfs_dir = tmpdir.strpath
        task.main(local_spark.sparkContext)
        expected = pd.DataFrame([
            {
                "audience_name": "audience_01",
                "category": "positive",
                "dt": "2020-10-15",
                "score": 0.6032486273334455,
                "scores_raw": [0.5415418112198506, 0.45845818878015243],
                "scores_trf": [0.6032486273334455],
                "uid": "a",
                "uid_type": "VID"
            }
        ])
        assert self._is_model_applied(expected)

    def test_apply_empty_df(self, local_spark, tmpdir):
        task_params = {
            "dt": "2020-11-23",
            "uid_type": "HID",
            "source_db": "local_db",
            "source_table": "features_table",
            "source_keep_columns": ["uid"],
            "target_db": "local_db",
            "target_table": "audience_table",
            "apply_adapter": {"py_class": "prj.adapters.apply.PythonApplyAdapter"},
            "model_batch": [{
                "audience_name": "audience_01",
                "adapter": {"py_class": "prj.adapters.model.lal.LookalikeMultinomialNB"},
                "hdfs_dir": os.path.join(self.STASH_DIR, "mnb")
            }]
        }
        task = ApplyTask(**dict(
            {
                "ctid": "apply_multinomial_nb",
                "status_urls": [os.path.join(tmpdir.strpath, "apply-multinomial-stat1")],
                "input_urls": [os.path.join(tmpdir.strpath, "apply-multinomial-inp1")],
                "output_urls": [os.path.join(tmpdir.strpath, "apply-multinomial-out1")],
                "force": False
            },
            **task_params
        ))
        task.tmp_hdfs_dir = os.path.join(tmpdir.strpath, "tmp")
        task.main(local_spark.sparkContext)
        assert self._is_result_empty(uid_type="HID", dt="2020-11-23")

    def test_apply_batch(self, local_spark, tmpdir):

        task_params = {
            "dt": "2020-10-15",
            "uid_type": "VID",
            "source_db": "local_db",
            "source_table": "features_table",
            "source_keep_columns": ["uid"],
            "target_db": "local_db",
            "target_table": "audience_table",
            "apply_adapter": {"py_class": "prj.adapters.apply.PythonApplyAdapter"},
            "model_batch": [
                {
                    "audience_name": "mnb",
                    "adapter": {"py_class": "prj.adapters.model.lal.LookalikeMultinomialNB"},
                    "hdfs_dir": os.path.join(self.STASH_DIR, "mnb")
                },
                {
                    "audience_name": "sgd",
                    "adapter": {"py_class": "prj.adapters.model.lal.LookalikeSGDClassifier"},
                    "hdfs_dir": os.path.join(self.STASH_DIR, "sgd")
                }
            ]
        }
        task = ApplyTask(**dict(
            {
                "ctid": "apply_batch",
                "status_urls": [os.path.join(tmpdir.strpath, "stat-apply-{}".format(x)) for x in ("mnb", "sgd")],
                "input_urls": [os.path.join(tmpdir.strpath, "inp-apply-{}".format(x)) for x in ("mnb", "sgd")],
                "output_urls": [os.path.join(tmpdir.strpath, "out-apply-{}".format(x)) for x in ("mnb", "sgd")],
                "force": False
            },
            **task_params
        ))
        task.tmp_hdfs_dir = os.path.join(tmpdir.strpath, "tmp")
        task.main(local_spark.sparkContext)
        expected = pd.DataFrame([
            {
                "audience_name": "mnb",
                "category": "positive",
                "dt": "2020-10-15",
                "score": 0.6032486420039008,
                "scores_raw": [0.5415418112198506, 0.45845818878015243],
                "scores_trf": [0.6032486420039008],
                "uid": "a",
                "uid_type": "VID"
            },
            {
                "audience_name": "sgd",
                "category": "positive",
                "dt": "2020-10-15",
                "score": 0.8745,
                "scores_raw": [0.0, 1.0],
                "scores_trf": [0.8745],
                "uid": "a",
                "uid_type": "VID"
            }
        ])
        assert self._is_model_applied(expected)

    def test_apply_full_config(self, local_spark, tmpdir):
        TMP_HDFS_DIR = tmpdir.strpath
        MODELS_DIR = os.path.join(TMP_HDFS_DIR, "e2e-models")
        NAME = "e2e_apply"
        UID_TYPE = "VID"
        DT = "2020-10-15"
        TEST_DB = "local_db"
        SOURCE_TABLE = "features_table"
        TARGET_TABLE = "audience_table"
        SUFFIXES = ["mnb", "sgd"]

        MODEL_CONFIGS = [
            {
                "py_class": "prj.adapters.model.lal.LookalikeMultinomialNB",
                "kwargs": {
                    "estimator": {
                        "py_class": "dmcore.predictors.pipeline.MLPipeline",
                        "kwargs": {
                            "preprocessors": [
                                ("imputer", {"py_class": "dmcore.transformers.preprocessing.ImputeFeaturesTransformer"}),
                                ("bin", {"py_class": "sklearn.preprocessing.data.Binarizer"})
                            ],
                            "predictor": [["clf", {
                                "py_class": "dmcore.predictors.wrapper.SklearnPredictorWrapper",
                                "kwargs": {
                                    "predictor": [["mnb", {"py_class": "sklearn.naive_bayes.MultinomialNB"}]],
                                    "predict_method": "predict_proba"
                                }
                            }]],
                            "postprocessors": [
                                (
                                    "prob_col", {
                                        "py_class": "dmcore.transformers.postprocessing.Slice2DColumnsTransformer",
                                        "kwargs": {"columns": 1}
                                    }
                                ),
                                (
                                    "equalizer", {
                                        "py_class": "dmcore.transformers.postprocessing.ScoreEqualizeTransformer",
                                        "kwargs": {"noise": 0}}
                                )
                            ]
                        }
                    }
                }
            },
            {
                "py_class": "prj.adapters.model.lal.LookalikeSGDClassifier",
                "kwargs": {
                    "estimator": {
                        "py_class": "dmcore.predictors.pipeline.MLPipeline",
                        "kwargs": {
                            "preprocessors": [
                                ("imputer", {"py_class": "dmcore.transformers.preprocessing.ImputeFeaturesTransformer"}),
                                ("tfidf", {"py_class": "dmcore.transformers.preprocessing.GroupedFeaturesTfidfTransformer"}),
                                ("std", {
                                    "py_class": "sklearn.preprocessing.data.StandardScaler",
                                    "kwargs": {"with_mean": False}
                                })
                            ],
                            "predictor": [["clf", {
                                "py_class": "dmcore.predictors.wrapper.SklearnPredictorWrapper",
                                "kwargs": {
                                    "predictor": [["sgdc", {
                                        "py_class": "sklearn.linear_model.stochastic_gradient.SGDClassifier",
                                        "kwargs": {
                                            "n_iter": 15,
                                            "n_jobs": 1,
                                            "eta0": 0.01,
                                            "loss": "log",
                                            "penalty": "elasticnet",
                                            "random_state": 0
                                        }
                                    }]],
                                    "predict_method": "_predict_proba"
                                }
                            }]],
                            "postprocessors": [
                                (
                                    "prob_col", {
                                        "py_class": "dmcore.transformers.postprocessing.Slice2DColumnsTransformer",
                                        "kwargs": {"columns": 1}
                                    }
                                ),
                                (
                                    "equalizer", {
                                        "py_class": "dmcore.transformers.postprocessing.ScoreEqualizeTransformer",
                                        "kwargs": {"noise": 0}
                                    }
                                )
                            ]
                        }
                    }
                }
            }
        ]

        # model = LaLMultinomialNB()
        # from prj.adapters.utils import deconstruct
        # model_config = deconstruct(model, ignore_defaults=False)
        # fitted_model = model.learn(learnset)
        # fitted_model_config = deconstruct(fitted_model, ignore_defaults=False)
        # print(fitted_model_config.__repr__())
        # print(model_config.__repr__())
        # assert fitted_model_config == model_config  # TODO: tuples vs lists conflict

        task_params = {
            "dt": DT,
            "uid_type": UID_TYPE,
            "source_db": TEST_DB,
            "source_table": SOURCE_TABLE,
            "source_keep_columns": ["uid"],
            "target_db": TEST_DB,
            "target_table": TARGET_TABLE,
            "apply_adapter": {"py_class": "prj.adapters.apply.PythonApplyAdapter"},
            "model_batch": [
                {
                    "audience_name": name,
                    "adapter": model,
                    "hdfs_dir": os.path.join(self.STASH_DIR, name)
                }
                for name, model in zip(SUFFIXES, MODEL_CONFIGS)
            ]
        }
        task = ApplyTask(**dict(
            {
                "ctid": "{}__success".format(NAME),
                "status_urls": [os.path.join(TMP_HDFS_DIR, "status__success__{}".format(x)) for x in SUFFIXES],
                "output_urls": [os.path.join(TMP_HDFS_DIR, "output__success__{}".format(x)) for x in SUFFIXES],
                "input_urls": [],
                "force": False
            },
            **task_params
        ))
        task.tmp_hdfs_dir = os.path.join(TMP_HDFS_DIR, "tmp")
        task.main(local_spark.sparkContext)
        expected = pd.DataFrame([
            {
                "audience_name": "mnb",
                "category": "positive",
                "dt": "2020-10-15",
                "score": 0.6032505150360096,
                "scores_raw": [0.5415418112198506, 0.45845818878015243],
                "scores_trf": [0.6032505091622539],
                "uid": "a",
                "uid_type": "VID"
            },
            {
                "audience_name": "sgd",
                "category": "positive",
                "dt": "2020-10-15",
                "score": 0.8482067422749077,
                "scores_raw": [0.0, 1.0],
                "scores_trf": [0.8482067422749077],
                "uid": "a",
                "uid_type": "VID"
            }
        ])
        assert self._is_model_applied(expected)

    def test_apply_e2e_imitation(self, local_spark, tmpdir):
        TMP_HDFS_DIR = tmpdir.strpath
        MODELS_DIR = os.path.join(TMP_HDFS_DIR, "e2e_models")
        NAME = "e2e_apply"
        UID_TYPE = "HID"
        DT = "2020-10-15"
        TEST_DB = "local_db"
        SOURCE_TABLE = "features_e2e"
        SOURCE_DB_TABLE = "{}.{}".format(TEST_DB, SOURCE_TABLE)
        TARGET_TABLE = "audience_table"

        SUFFIXES_PA = ["clf", "oclf", "lal"]
        MODEL_CONFIGS_PA = [
            {
                "py_class": "prj.adapters.model.clf.ClfMultinomialNB"
            },
            {
                "py_class": "prj.adapters.model.oclf.OrdClfSGDRegressor",
                "kwargs": {
                    "estimator": {
                        "postprocessors": [
                            ["equalizer", {"noise": 0}],
                            "__default__"
                        ]
                    }
                }
            },
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
            }
        ]

        SUFFIXES_SA = ["mnb_e2e", "sgd_e2e"]
        MODEL_CONFIGS_SA = [
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

        # @formatter:off
        EXPECTED_DF_TEXT_PA = """
+-----+--------------------+------------------------------------------+---------------------------+-------------+--------+----------+--------+
|uid  |score               |scores_raw                                |scores_trf                 |audience_name|category|dt        |uid_type|
+-----+--------------------+------------------------------------------+---------------------------+-------------+--------+----------+--------+
|uid_0|0.5871372008079563  |[0.030531482450085225, 0.9694685175499154]|[1.0, 0.5871372008079563]  |clf          |1.0     |2020-10-15|HID     |
|uid_1|0.9530006667963468  |[0.862641917263964, 0.13735808273603686]  |[0.0, 0.9530006667963468]  |clf          |0.0     |2020-10-15|HID     |
|uid_2|0.4819013578683807  |[0.7281617221224774, 0.2718382778775232]  |[0.0, 0.4819013578683807]  |clf          |0.0     |2020-10-15|HID     |
|uid_3|0.5768955758836162  |[0.7457932207208225, 0.25420677927917756] |[0.0, 0.5768955758836162]  |clf          |0.0     |2020-10-15|HID     |
|uid_4|0.8083847011801825  |[0.002287510705929438, 0.997712489294072] |[1.0, 0.8083847011801825]  |clf          |1.0     |2020-10-15|HID     |
|uid_5|0.39195494923320523 |[0.7131917744089509, 0.28680822559104985] |[0.0, 0.39195494923320523] |clf          |0.0     |2020-10-15|HID     |
|uid_6|0.7131362879464671  |[0.003608486379748199, 0.9963915136202518]|[1.0, 0.7131362879464671]  |clf          |1.0     |2020-10-15|HID     |
|uid_7|0.7741751005240598  |[0.002545009141356248, 0.9974549908586431]|[1.0, 0.7741751005240598]  |clf          |1.0     |2020-10-15|HID     |
|uid_8|0.4579912910607573  |[0.04738902244103701, 0.9526109775589624] |[1.0, 0.4579912910607573]  |clf          |1.0     |2020-10-15|HID     |
|uid_9|0.3912734674237911  |[0.7131917744089509, 0.28680822559104985] |[0.0, 0.3912734674237911]  |clf          |0.0     |2020-10-15|HID     |
|uid_0|0.4967483743381249  |[0.4152263253951727]                      |[1.0, 0.4967483743381249]  |oclf         |1.0     |2020-10-15|HID     |
|uid_1|5.002449525169649E-4|[0.049445874605369394]                    |[0.0, 5.002449525169649E-4]|oclf         |0.0     |2020-10-15|HID     |
|uid_2|0.43171584167437    |[-0.2612691180406287]                     |[0.0, 0.43171584167437]    |oclf         |0.0     |2020-10-15|HID     |
|uid_3|0.28064030409250684 |[-0.13203292937367966]                    |[0.0, 0.28064030409250684] |oclf         |0.0     |2020-10-15|HID     |
|uid_4|0.5957978042209223  |[0.46922406383278964]                     |[1.0, 0.5957978042209223]  |oclf         |1.0     |2020-10-15|HID     |
|uid_5|0.14857420447348818 |[-0.048772816919512324]                   |[0.0, 0.14857420447348818] |oclf         |0.0     |2020-10-15|HID     |
|uid_6|0.7088544366351806  |[0.5319331336780141]                      |[1.0, 0.7088544366351806]  |oclf         |1.0     |2020-10-15|HID     |
|uid_7|0.9809905450449963  |[0.8059309228580054]                      |[1.0, 0.9809905450449963]  |oclf         |1.0     |2020-10-15|HID     |
|uid_8|0.6458229314461812  |[0.49431167901180667]                     |[1.0, 0.6458229314461812]  |oclf         |1.0     |2020-10-15|HID     |
|uid_9|0.3796899334166073  |[-0.22112532934065687]                    |[0.0, 0.3796899334166073]  |oclf         |0.0     |2020-10-15|HID     |
|uid_0|0.7945              |[0.030531482450085225, 0.9694685175499154]|[0.7945]                   |lal          |positive|2020-10-15|HID     |
|uid_1|0.023               |[0.862641917263964, 0.13735808273603686]  |[0.023]                    |lal          |positive|2020-10-15|HID     |
|uid_2|0.2585              |[0.7281617221224774, 0.2718382778775232]  |[0.2585]                   |lal          |positive|2020-10-15|HID     |
|uid_3|0.211               |[0.7457932207208225, 0.25420677927917756] |[0.211]                    |lal          |positive|2020-10-15|HID     |
|uid_4|0.902               |[0.002287510705929438, 0.997712489294072] |[0.902]                    |lal          |positive|2020-10-15|HID     |
|uid_5|0.3035              |[0.7131917744089509, 0.28680822559104985] |[0.3035]                   |lal          |positive|2020-10-15|HID     |
|uid_6|0.8565              |[0.003608486379748199, 0.9963915136202518]|[0.8565]                   |lal          |positive|2020-10-15|HID     |
|uid_7|0.889               |[0.002545009141356248, 0.9974549908586431]|[0.889]                    |lal          |positive|2020-10-15|HID     |
|uid_8|0.729               |[0.04738902244103701, 0.9526109775589624] |[0.729]                    |lal          |positive|2020-10-15|HID     |
|uid_9|0.3035              |[0.7131917744089509, 0.28680822559104985] |[0.3035]                   |lal          |positive|2020-10-15|HID     |
+-----+--------------------+------------------------------------------+---------------------------+-------------+--------+----------+--------+        
        """

        EXPECTED_DF_TEXT_SA = """
+-----+-------------------+--------------------------------------------+---------------------+-------------+--------+----------+--------+
|uid  |score              |scores_raw                                  |scores_trf           |audience_name|category|dt        |uid_type|
+-----+-------------------+--------------------------------------------+---------------------+-------------+--------+----------+--------+
|uid_0|0.7945             |[0.030531482450085225, 0.9694685175499154]  |[0.7945]             |mnb_e2e      |positive|2020-10-15|HID     |
|uid_1|0.023              |[0.862641917263964, 0.13735808273603686]    |[0.023]              |mnb_e2e      |positive|2020-10-15|HID     |
|uid_2|0.2585             |[0.7281617221224774, 0.2718382778775232]    |[0.2585]             |mnb_e2e      |positive|2020-10-15|HID     |
|uid_3|0.211              |[0.7457932207208225, 0.25420677927917756]   |[0.211]              |mnb_e2e      |positive|2020-10-15|HID     |
|uid_4|0.902              |[0.002287510705929438, 0.997712489294072]   |[0.902]              |mnb_e2e      |positive|2020-10-15|HID     |
|uid_5|0.3035             |[0.7131917744089509, 0.28680822559104985]   |[0.3035]             |mnb_e2e      |positive|2020-10-15|HID     |
|uid_6|0.8565             |[0.003608486379748199, 0.9963915136202518]  |[0.8565]             |mnb_e2e      |positive|2020-10-15|HID     |
|uid_7|0.889              |[0.002545009141356248, 0.9974549908586431]  |[0.889]              |mnb_e2e      |positive|2020-10-15|HID     |
|uid_8|0.729              |[0.04738902244103701, 0.9526109775589624]   |[0.729]              |mnb_e2e      |positive|2020-10-15|HID     |
|uid_9|0.3035             |[0.7131917744089509, 0.28680822559104985]   |[0.3035]             |mnb_e2e      |positive|2020-10-15|HID     |
|uid_0|0.757              |[3.001154880166723E-11, 0.9999999999699885] |[0.757]              |sgd_e2e      |positive|2020-10-15|HID     |
|uid_1|0.6180000058020415 |[1.4503392298359508E-5, 0.9999854966077016] |[0.6180000058020415] |sgd_e2e      |positive|2020-10-15|HID     |
|uid_2|0.3025000135742895 |[0.9999999998456682, 1.543318068229884E-10] |[0.3025000135742895] |sgd_e2e      |positive|2020-10-15|HID     |
|uid_3|0.29350001743345344|[0.9999999999335232, 6.647678874272872E-11] |[0.29350001743345344]|sgd_e2e      |positive|2020-10-15|HID     |
|uid_4|0.7995             |[2.9265478929119126E-13, 0.9999999999997073]|[0.7995]             |sgd_e2e      |positive|2020-10-15|HID     |
|uid_5|0.44750007068173825|[0.9970256122036304, 0.0029743877963695595] |[0.44750007068173825]|sgd_e2e      |positive|2020-10-15|HID     |
|uid_6|0.8745             |[0.0, 1.0]                                  |[0.8745]             |sgd_e2e      |positive|2020-10-15|HID     |
|uid_7|0.8745             |[0.0, 1.0]                                  |[0.8745]             |sgd_e2e      |positive|2020-10-15|HID     |
|uid_8|0.759              |[2.1695756302619884E-11, 0.9999999999783042]|[0.759]              |sgd_e2e      |positive|2020-10-15|HID     |
|uid_9|0.3059999807749249 |[0.9999999996885132, 3.114868339525113E-10] |[0.3059999807749249] |sgd_e2e      |positive|2020-10-15|HID     |
+-----+-------------------+--------------------------------------------+---------------------+-------------+--------+----------+--------+
        """
        # @formatter:on

        TEST_TASK_PARAMS = [
            ("pa", "prj.adapters.apply.PythonApplyAdapter", MODEL_CONFIGS_PA, SUFFIXES_PA, EXPECTED_DF_TEXT_PA),
            ("sa", "prj.adapters.apply.ScalaApplyAdapter", MODEL_CONFIGS_SA, SUFFIXES_SA, EXPECTED_DF_TEXT_SA),
        ]

        TEST_DATA = [
            (
                {
                    "dt": DT,
                    "uid_type": UID_TYPE,
                    "source_db": TEST_DB,
                    "source_table": SOURCE_TABLE,
                    "source_keep_columns": ["uid"],
                    "target_db": TEST_DB,
                    "target_table": TARGET_TABLE,
                    "apply_adapter": {"py_class": apply_adapter},
                    "fallback_apply_adapter": {
                        "py_class": "prj.adapters.apply.PythonApplyAdapter"
                    } if tid == "sa" else {},
                    "model_batch": [
                        {
                            "audience_name": name,
                            "adapter": model,
                            "hdfs_dir": os.path.join(MODELS_DIR, name)
                        }
                        for name, model in zip(suffixes, model_configs)
                    ],
                    "ctid": "{}_{}__success".format(NAME, tid),
                    "status_urls": [os.path.join(TMP_HDFS_DIR, "status__success__{}".format(x)) for x in suffixes],
                    "output_urls": [os.path.join(TMP_HDFS_DIR, "output__success__{}".format(x)) for x in suffixes],
                    "input_urls": [],
                    "force": False
                },
                {
                    "status": ["success"] * len(model_configs),
                    "data": expected_df_text
                }
            )
            for tid, apply_adapter, model_configs, suffixes, expected_df_text in TEST_TASK_PARAMS
        ]

        def _prepare_input(model_batch):
            import six
            from prj.apps.learn.test import make_lal_learnset
            from collections import OrderedDict
            from prj.adapters.dataset import LookalikeLearnsetAdapter
            from prj.adapters.utils import construct
            from backports import tempfile

            train, gf, label_names = make_lal_learnset(n_feature_groups=2)
            grouped_features = OrderedDict([
                ("fg_{}".format(k), v) for k, v in six.iteritems(gf)
            ])
            learnset = LookalikeLearnsetAdapter().set_data(
                learning_data=train,
                grouped_features=grouped_features,
                label_names=label_names
            )
            for m in model_batch:
                hdfs_dir = m["hdfs_dir"]
                model = construct(m["adapter"]).learn(learnset)
                with tempfile.TemporaryDirectory() as tmp_local_dir:
                    model.save(tmp_local_dir)
                    learnset.grouped_features_adapter.save(tmp_local_dir)
                    hdfs = HdfsClient()
                    hdfs.mkdir(hdfs_dir, remove_if_exists=True)
                    hdfs.put(tmp_local_dir, hdfs_dir, content_only=True)

            return [
                {
                    "uid": "uid_{}".format(i),
                    "uid_type": UID_TYPE,
                    "dt": DT,
                    "fg_0": [row[int(k)] for k in grouped_features["fg_0"]],
                    "fg_1": {k: row[int(k)] for k in grouped_features["fg_1"] if row[int(k)] != 0}
                }
                for i, row in zip(range(10), train.x.toarray().tolist())
            ]

        source_data = _prepare_input([model for cfg, _ in TEST_DATA for model in cfg["model_batch"]])
        from .data import TABLES
        from .service import TableWrapper, HadoopEnvironment
        tables = {SOURCE_DB_TABLE: {
            "schema": [(k, v) for k, v in TABLES[SOURCE_DB_TABLE]["schema"]],
            "partitions": TABLES[SOURCE_DB_TABLE]["partitions"],
            "rows": source_data
        }}
        HadoopEnvironment.instance.empty_table(TEST_DB, TARGET_TABLE)
        HadoopEnvironment.instance.empty_table(TEST_DB, SOURCE_TABLE)
        HadoopEnvironment.instance.write_table(TEST_DB, SOURCE_TABLE, TableWrapper(SOURCE_DB_TABLE, tables, local_spark).df)

        for task_params, expected in TEST_DATA:
            task = ApplyTask(**task_params)
            task.tmp_hdfs_dir = os.path.join(TMP_HDFS_DIR, "{}_tmp".format(task_params["ctid"]))
            task.main(local_spark.sparkContext)
            expected_text = expected["data"]
            expected_pdf = pd.DataFrame(self._parse_df(expected_text))
            assert self._is_model_applied(expected_pdf)

    @staticmethod
    def _parse_df(text):
        def values(line):
            return [x.strip() for x in line.split("|") if x.strip()]

        def convert_data_type(name, value):
            if name in {"score", "scores_raw", "scores_trf"}:
                return name, eval(value)
            return name, value

        def row(names, values):
            return dict([convert_data_type(name, value) for name, value in zip(names, values)])

        lines = text.strip().split("\n")
        return [row(values(lines[1]), values(lines[i])) for i in range(3, len(lines)-1)]

    @staticmethod
    def _create_learnset(data_dir):
        hdfs = HdfsClient()
        if not hdfs.exists(os.path.join(data_dir, "_SUCCESS")):
            HdfsClient().remove(data_dir)
            from prj.apps.learn.test.e2e.test_app import TestLearn
            TestLearn._build_learnset(data_dir)

    @staticmethod
    def _is_model_saved(model_dir):
        hdfs = HdfsClient()
        existence = {
            f: hdfs.exists(os.path.join(model_dir, f))
            for f in ["grouped_features.json", "model.tar.gz"]
        }
        print("\nmodel files saved: `{}`\n".format(existence))
        return all(x for _, x in existence.items())

    @staticmethod
    def _is_model_applied(expected=None):
        from .service import HadoopEnvironment
        actual = HadoopEnvironment.read_from_hive("local_db", "audience_table")
        print("\nIs model applied? Actual DF:")
        actual.show(200, truncate=False)
        actual = actual.toPandas()
        test_cols = ["audience_name", "category", "dt", "scores_raw", "uid", "uid_type"] + ["score", "scores_trf"]
        pd.testing.assert_frame_equal(
            actual[test_cols],
            expected[test_cols],
            check_exact=False,
            check_less_precise=1,
            check_like=True
        )
        return True

    @staticmethod
    def _is_result_empty(uid_type, dt):
        from .service import HadoopEnvironment
        actual = HadoopEnvironment.read_from_hive("local_db", "audience_table")
        return actual.where("uid_type = '{}' and dt = '{}'".format(uid_type, dt)).count() == 0

    @staticmethod
    def _stash(from_dir, to_dir):
        import shutil
        shutil.rmtree(to_dir, ignore_errors=True)
        shutil.copytree(from_dir, to_dir)
