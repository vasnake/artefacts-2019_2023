import os
import functools

from prj.apps.learn.app import LearnTask
from prj.apps.apply.app import ApplyTask

from .service import HdfsClient


class TestLearnApply(object):

    def test_learn_sgd_classifier(self, local_spark, session_temp_dir):
        data_dir, model_dir = (os.path.join(session_temp_dir, x) for x in ("data", "model"))
        self._create_learnset(data_dir)

        task_params = {
            "model_config": {"py_class": "prj.adapters.model.lal.LookalikeSGDClassifier"},
            "dataset_config": {"py_class": "prj.adapters.dataset.LookalikeLearnsetAdapter"},
            "model_hdfs_dir": model_dir,
            "dataset_hdfs_dir": data_dir
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
        task.control_client.send_results = functools.partial(self._check_results, task.control_client, url="", data={})

        task.main(local_spark.sparkContext)
        assert self._is_model_saved(model_dir)

        self._stash(model_dir, "/tmp/DM-8178-scala-apply/")

    def test_learn_multinomial_nb(self, local_spark, session_temp_dir):
        data_dir, model_dir = (os.path.join(session_temp_dir, x) for x in ("data", "model"))
        self._create_learnset(data_dir)

        task_params = {
            "model_config": {"py_class": "prj.adapters.model.lal.LookalikeMultinomialNB"},
            "dataset_config": {"py_class": "prj.adapters.dataset.LookalikeLearnsetAdapter"},
            "model_hdfs_dir": model_dir,
            "dataset_hdfs_dir": data_dir
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
        task.control_client.send_results = functools.partial(self._check_results, task.control_client, url="", data={})

        task.main(local_spark.sparkContext)
        assert self._is_model_saved(model_dir)

        self._stash(model_dir, "/tmp/DM-8178-scala-apply/")

    def test_apply_multinomial_nb(self, local_spark, session_temp_dir):
        model_dir = os.path.join(session_temp_dir, "model")

        if not self._is_model_saved(model_dir):
            model_dir = "/tmp/DM-8178-scala-apply/model"

        task_params = {
            "dt": "2020-10-15",
            "uid_type": "VID",
            "source_db": "local_db",
            "source_table": "features_table",
            "target_db": "local_db",
            "target_table": "audience_table",
            "target_schema": [  # should be derived from target table during app init stage
                ("score", "float"),
                ("scores_raw", "float"),
                ("scores_trf", "float"),
                ("category", "str")
            ],
            # upper level DF transformer
            "apply_config": {"py_class": "prj.adapters.apply.PythonBatchApplyAdapter"},
            # models batch, two synced lists
            "audience_names": ["audience_01"],
            "model_configs": [{  # model_adapter, hdfs_dir, target_partition, target_schema
                "model_adapter": {
                    "py_class": "prj.adapters.model.lal.LookalikeMultinomialNB"
                },
                "hdfs_dir": model_dir
            }]
        }
        task = ApplyTask(**dict(
            {
                "ctid": "apply_multinomial_nb",
                "status_urls": [],
                "input_urls": [],
                "output_urls": [],
                "force": False
            },
            **task_params
        ))

        def _check_runs_success(flags):
            assert flags == [True]

        task.tmp_hdfs_dir = os.path.join(session_temp_dir, "tmp")
        task.report_runs_success = functools.partial(_check_runs_success)

        task.main(local_spark.sparkContext)
        assert self._is_model_applied()

    @staticmethod
    def _check_results(obj, url, data):
        assert url == "learn_results"
        jvm_apply = [x["value"] for x in data if x["type"] == "data" and x["name"] == "jvm_apply"]
        assert len(jvm_apply) == 1
        assert jvm_apply[0] is True

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
            fname: hdfs.exists(os.path.join(model_dir, fname))
            for fname in ["grouped_features.json", "model.tar.gz", "model_repr.json"]
        }
        print("\nmodel files existence: `{}`\n".format(existence))
        return all(x for _, x in existence.items())

    @staticmethod
    def _stash(from_dir, to_dir):
        hdfs = HdfsClient()
        hdfs.remove(to_dir)
        hdfs.put(from_dir, to_dir)

    @staticmethod
    def _is_model_applied():
        from .service import HadoopEnvironment
        import pandas as pd
        actual = HadoopEnvironment.read_from_hive("local_db", "audience_table").toPandas()
        expected = pd.DataFrame([
            {
                "audience_name": "audience_01",
                "category": "positive",
                "dt": "2020-10-15",
                "score": 0.8316379805371369,
                "scores_raw": [0.018425355656401805, 0.981574644343601],
                "scores_trf": [0.83163978582651],
                "uid": "a",
                "uid_type": "VID"
            }
        ])
        test_cols = ["audience_name", "category", "dt", "scores_raw", "uid", "uid_type"] + ["score", "scores_trf"]
        pd.testing.assert_frame_equal(
            actual[test_cols],
            expected[test_cols],
            check_less_precise=True,
            check_like=True
        )
        return True
