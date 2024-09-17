import json
import os
import shutil

import pandas as pd
from scipy.sparse.csr import csr_matrix
from sklearn.naive_bayes import MultinomialNB
from sklearn.preprocessing import Binarizer

from prjcore.predictors import MLPipeline, SklearnPredictorWrapper
from prjcore.transformers import (
    ScoreEqualizeTransformer,
    ImputeFeaturesTransformer,
    Slice2DColumnsTransformer
)
from prj.adapters.label import OutputLabelAdapter, SliceColumnConverter, ConstantConverter
from prj.adapters.model.lal import LookalikeModelAdapter
from prj.adapters.predict import PredictAdapter
from prj.interface.models.clal.lal_binary_ranking import LalBinarizedMultinomialNbPLPA
from prj.adapters.test.conftest import *
from prj.interface.models.test.conftest import make_lal_learning_data


class LaLMultinomialNB(LookalikeModelAdapter):
    LOGGER_CUSTOM_LEVEL = {}

    estimator = MLPipeline(
        preprocessors=[
            ["imputer", ImputeFeaturesTransformer(default_fill=0, strategy="median")],
            ["bin", Binarizer()]
        ],
        predictor=[[
            "clf", SklearnPredictorWrapper(
                predict_method="predict_proba",
                predictor=[["mnb", MultinomialNB(alpha=0.898560392994, fit_prior=False)]]
            )
        ]],
        postprocessors=[
            ["prob_col", Slice2DColumnsTransformer(columns=1)],
            ["equalizer", ScoreEqualizeTransformer(noise=0.0001)]
        ]
    )
    predict_adapter = PredictAdapter(
        label_adapters={
            "scores_raw": OutputLabelAdapter(
                step="clf", label_type="array",
                converters=[
                    SliceColumnConverter(col_index=[1], dtype="float")
                ]
            ),
            "scores_trf": OutputLabelAdapter(step="equalizer", label_type="array"),
            "category": OutputLabelAdapter(
                step="equalizer", label_type="scalar",
                converters=[
                    SliceColumnConverter(col_index=0, dtype="float"),
                    ConstantConverter(constant="positive", dtype="str")
                ]
            ),
            "score": OutputLabelAdapter(
                step="equalizer", label_type="scalar",
                converters=[SliceColumnConverter(col_index=0, dtype="float")]
            )
        }
    )


class TestLearnApply(object):

    model_v1_repr = None
    model_v2_repr = None

    def test_reference_learn_apply(self):
        from prj.apps.learn.test import make_lal_learnset

        class LaLMultinomialNB(LookalikeModelAdapter):
            LOGGER_CUSTOM_LEVEL = {}
            estimator = MLPipeline(
                preprocessors=[
                    ["imputer", ImputeFeaturesTransformer(default_fill=0)],
                    ["bin", Binarizer()]
                ],
                predictor=[[
                    "clf", SklearnPredictorWrapper(
                        predict_method="predict_proba",
                        predictor=[["mnb", MultinomialNB()]]
                    )
                ]],
                postprocessors=[
                    ["prob_col", Slice2DColumnsTransformer(columns=1)],
                    ["equalizer", ScoreEqualizeTransformer(noise=0)]
                ]
            )

        train, grouped_features, label_names = make_lal_learnset()
        learnset = LookalikeLearnsetAdapter().set_data(
            learning_data=train,
            grouped_features=grouped_features,
            label_names=["score", "category"]
        )
        testset = LookalikeLearnsetAdapter().set_data(
            learning_data=train[0:10],
            grouped_features=grouped_features,
            label_names=["score", "category"]
        )
        actual_pdf = pd.DataFrame(LaLMultinomialNB().learn(learnset).apply(testset))
        expected_pdf = pd.DataFrame([
            # @formatter:off
            {"category": "positive", "score": 0.7945, "scores_raw": [0.0305314824501, 0.96946851755], "scores_trf": [0.7945]},
            {"category": "positive", "score": 0.0230, "scores_raw": [0.862641917264, 0.137358082736], "scores_trf": [0.023]},
            {"category": "positive", "score": 0.2585, "scores_raw": [0.728161722122, 0.271838277878], "scores_trf": [0.2585]},
            {"category": "positive", "score": 0.2110, "scores_raw": [0.745793220721, 0.254206779279], "scores_trf": [0.2110]},
            {"category": "positive", "score": 0.9020, "scores_raw": [0.00228751070593, 0.997712489294], "scores_trf": [0.9020]},
            {"category": "positive", "score": 0.3035, "scores_raw": [0.713191774409, 0.286808225591], "scores_trf": [0.3035]},
            {"category": "positive", "score": 0.8565, "scores_raw": [0.00360848637975, 0.99639151362], "scores_trf": [0.8565]},
            {"category": "positive", "score": 0.8890, "scores_raw": [0.00254500914136, 0.997454990859], "scores_trf": [0.8890]},
            {"category": "positive", "score": 0.7290, "scores_raw": [0.047389022441, 0.952610977559], "scores_trf": [0.7290]},
            {"category": "positive", "score": 0.3035, "scores_raw": [0.713191774409, 0.286808225591], "scores_trf": [0.3035]}
            # @formatter:on
        ])
        columns = ["category", "score", "scores_raw", "scores_trf"]
        pd.testing.assert_frame_equal(
            actual_pdf[columns],
            expected_pdf[columns],
            check_less_precise=5
        )

    def test_apply_speed_clf(self, clf_learnset_factory):
        from prj.adapters.model.oclf import (
            OrdClfSGDRegressor as OrdClfSGDRegressor_2
        )
        from timeit import default_timer as time
        # from prj.apps.learn.test import make_lal_learnset
        # from prj.adapters.utils import deconstruct, construct

        learn_size = 2000
        test_size = 999000
        learnset = clf_learnset_factory(n_samples=learn_size, random_state=0)
        testset = clf_learnset_factory(n_samples=test_size, random_state=0)

        # model = OrdClfLGBMRegressor()  # 20 sec
        model = OrdClfSGDRegressor_2()  # 10 sec

        start = time()
        model = model.learn(learnset)
        print("===\nLearned in {} seconds\n".format(time() - start))

        start = time()
        rows = list(model.apply(testset))
        elapsed = time() - start
        assert elapsed < 13  # 11.7 sec
        assert len(rows) == test_size
        print("\nApply & copy predictions to list of rows in {} seconds\n===".format(elapsed))
        print(rows[:3])

    def test_compare_models(self, make_lal_learning_data, tmpdir):
        train, test, unlabeled, grouped_features = make_lal_learning_data(uid_type_names=["HID"], random_state=0)
        assert isinstance(train, LearningData) and isinstance(test, LearningData)
        assert isinstance(train.x, csr_matrix)

        def _v1_predictions(train, test, unlabeled, grouped_features):
            model_params = {
                "model_dir": tmpdir.strpath,
                "audience_name": "a1",
                "project_params": {},
                "hyper_params": iter(LalBinarizedMultinomialNbPLPA.generate_hyper_params(grid_size=1, random_state=0)).next(),
                "extra_params": {
                    "min_features_per_sample": 1,
                    "max_features_per_sample": 10000,
                    "eval_set_size": 0.2
                },
                "feature_groups": None,  # select all feature domains
                "log_url": ""
            }
            model = LalBinarizedMultinomialNbPLPA(**model_params)

            stats = model.build(train, test, grouped_features, label_names=["score", "category"], unlabeled=train)

            # self.model_v1_repr = LalBinarizedMultinomialNbRepr(model.model).as_dict()
            # self._save_repr(self.model_v1_repr, session_temp_dir, "v1", "v1_model.json")

            return list(model.apply(x=test.x, uid_type="HID"))

        def _v2_predictions(train, test, unlabeled, grouped_features):
            #  set_data x.slice op do shuffle csr_matrix data, as a result you can't compare x data directly
            learnset = LookalikeLearnsetAdapter().set_data(train, grouped_features=grouped_features, label_names=["score", "category"])
            testset = LookalikeLearnsetAdapter().set_data(test, grouped_features=grouped_features, label_names=["score", "category"])

            # assert isinstance(learnset.learning_data.x, csr_matrix)
            # np.testing.assert_allclose(learnset.learning_data.x.toarray(), train.x.toarray(), atol=0.000001)
            # np.testing.assert_allclose(learnset.learning_data.x.todense(), train.x.todense(), atol=0.000001)
            # np.testing.assert_allclose(learnset.learning_data.x.A, train.x.A, atol=0.000001)

            model = LaLMultinomialNB().learn(learnset)

            # self.model_v2_repr = MLPipelineRepr(model.estimator).as_dict()
            # self._save_repr(self.model_v2_repr, session_temp_dir, "v2", "v2_model.json")

            return list(model.apply(testset))

        v1_pred = _v1_predictions(train, test, unlabeled, grouped_features)
        v2_pred = _v2_predictions(train, test, unlabeled, grouped_features)

        # learned params

        # imputer_v1 = self.model_v1_repr["imputer_repr"]
        # imputer_v2 = [x for x in self.model_v2_repr["preprocessors"] if x["stage_class"].endswith("ImputeFeaturesTransformer")][0]["stage_repr"]
        # assert len(imputer_v2["imputer_values"]) == len(imputer_v1["imputer_values"])
        # np.testing.assert_allclose(imputer_v2["imputer_values"], imputer_v1["imputer_values"], atol=0.000001)

        # binarizer_v1 = self.model_v1_repr["binarizer_repr"]
        # binarizer_v2 = [x for x in self.model_v2_repr["preprocessors"] if x["stage_class"].endswith("Binarizer")][0]["stage_repr"]
        # assert binarizer_v2["threshold"] == binarizer_v1["threshold"]

        # predictor_v1 = self.model_v1_repr["predictor_wrapper_repr"]["predictor_repr"]
        # predictor_v2 = self.model_v2_repr["predictor"]["stage_repr"]["predictor"]["stage_repr"]
        # np.testing.assert_allclose(predictor_v2["class_log_prior"], predictor_v1["class_log_prior"], atol=0.000001)
        # np.testing.assert_allclose(predictor_v2["feature_log_prob"], predictor_v1["feature_log_prob"], atol=0.000001)

        # equalizer_v1 = self.model_v1_repr["equalizer_repr"]["transformers"]["HID"]
        # equalizer_v2 = [x for x in self.model_v2_repr["postprocessors"] if x["stage_class"].endswith("ScoreEqualizeTransformer")][0]["stage_repr"]
        # np.testing.assert_almost_equal(equalizer_v2["eps"], equalizer_v1["eps"], decimal=5)
        # np.testing.assert_almost_equal(equalizer_v2["noise"], equalizer_v1["noise"], decimal=5)
        # np.testing.assert_almost_equal(equalizer_v2["maxval"], equalizer_v1["maxval"], decimal=5)
        # np.testing.assert_almost_equal(equalizer_v2["minval"], equalizer_v1["minval"], decimal=5)
        # np.testing.assert_allclose(equalizer_v2["coefficients"], equalizer_v1["coefficients"], atol=0.001)
        # np.testing.assert_allclose(equalizer_v2["intervals"], equalizer_v1["intervals"], atol=0.001)

        # predictions

        assert len(v1_pred) == len(v2_pred) and len(v1_pred) == test.x.shape[0]
        for r1, r2 in zip(v1_pred, v2_pred):
            if r1 is None:
                np.testing.assert_allclose(r2["scores_raw"], [0.5], atol=0.000001)

        pdf1 = pd.DataFrame([r for r in v1_pred if r])
        pdf2 = pd.DataFrame([r2 for r1, r2 in zip(v1_pred, v2_pred) if r1])
        print(pdf1)
        print(pdf2)
        columns = ["category", "score", "scores_raw", "scores_trf"]
        pd.testing.assert_frame_equal(
            pdf2[columns],
            pdf1[columns],
            check_less_precise=0,
            check_like=False
        )
        columns = ["category", "scores_raw"]
        pd.testing.assert_frame_equal(
            pdf2[columns],
            pdf1[columns],
            check_less_precise=3,
            check_like=False
        )

    # @staticmethod
    # def _save_repr(obj, temp_dir, subdir, file_name):
    #     with open(os.path.join(temp_dir, file_name), "w") as f:
    #         json.dump(obj, f, sort_keys=True, indent=4)
    #     TestLearnApply._stash(temp_dir, subdir)

    # STASH_DIR = "/tmp/DM-8178-scala-apply-models/"

    # @staticmethod
    # def _stash(from_dir, subdir="stash"):
    #     to_dir = os.path.join(TestLearnApply.STASH_DIR, subdir)
    #     shutil.rmtree(to_dir, ignore_errors=True)
    #     shutil.copytree(from_dir, to_dir)
    #     print("\nstash from `{}` to `{}` complete\n".format(from_dir, to_dir))
