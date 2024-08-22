import os
import abc
import six
import numpy as np
import sklearn.base as sb
from sklearn.model_selection import ParameterSampler, train_test_split
from sklearn.metrics import roc_auc_score
from scipy.stats import kendalltau

from pyspark.sql.types import StructField, StringType, DoubleType, ArrayType

from dmcore.predictors import MLPipeline
from dmcore.utils.common import filter_nans
from dmcore.utils.io import write_model, read_model, write_json, read_json

from dmgrinder.interface.tools import filter_grouped_features
from dmgrinder.utils.control_helpers import ControlLoggingMixin

from ..base import BaseModel, PLearnMixin, PApplyMixin


class BaseScoring(BaseModel):
    """Basic interface for Scoring models"""

    MODEL_FILENAME = "model.tar.gz"
    FEATURES_FILENAME = "grouped_features.json"

    LABEL_COLUMNS = ["score", "category"]

    OUTPUT_FIELDS = [
        StructField("score", DoubleType()),
        StructField("scores_raw", ArrayType(DoubleType())),
        StructField("scores_trf", ArrayType(DoubleType())),
        StructField("audience_name", StringType()),
        StructField("category", StringType())
    ]


class SApplyMixin(object):
    """Interface for model Scala-Apply"""

    JVM_APPLY = True

    def __init__(self, *args, **kwargs):
        if not isinstance(self, BaseModel):
            raise TypeError("Class '{mixin}' should be used together with '{main}' in subclasses".format(
                mixin="{}.{}".format(SApplyMixin.__module__, SApplyMixin.__name__),
                main="{}.{}".format(BaseModel.__module__, BaseModel.__name__)
            ))
        super(SApplyMixin, self).__init__(*args, **kwargs)

    @property
    def configuration(self):
        """Model meta-data"""
        return {
            "model_type": self.__class__.__name__,
            "audience_name": self.audience_name,
            "model_path": os.path.join(self.model_dir, self.MODEL_FILENAME),
            "features_path": os.path.join(self.model_dir, self.FEATURES_FILENAME)
        }

    def set_apply_uid_type(self, uid_type):
        """
        Pass parameter to 'model.apply'.
        Known usages: equalizer selector in SampleBasedGroupedTransformer postprocessing.
        """
        raise NotImplementedError()


class BaseLalBinaryRankerSA(SApplyMixin):
    MODEL_REPR_FACTORY = NotImplemented
    MODEL_REPR_FILENAME = "model_repr.json"

    def __init__(self, *args, **kwargs):
        super(BaseLalBinaryRankerSA, self).__init__(*args, **kwargs)
        self._equalizer_selector = ""

    def set_apply_uid_type(self, uid_type):
        self._equalizer_selector = uid_type

    @property
    def configuration(self):
        conf = super(BaseLalBinaryRankerSA, self).configuration
        conf.update({
            "model_repr_path":  os.path.join(self.model_dir, self.MODEL_REPR_FILENAME),
            "equalizer_selector": self._equalizer_selector
        })
        return conf

    def save(self, folder='.', **kwargs):
        write_json(
            self.MODEL_REPR_FACTORY(self.model).as_dict(),
            os.path.join(folder, self.MODEL_REPR_FILENAME)
        )


class LalBinarizedMultinomialNbPLPA(BaseLalBinaryRankerPLPA):
    """
    Look-alike binary ranking pipeline:
    <Imputer> --> <Binarizer> --> <MultinomialNB> --> <ScoreEqualizer>.
    PLPA = Python Learn Python Apply.
    """

    MODEL_TEMPLATE = sb.clone(BaseLalBinaryRankerPLPA.MODEL_TEMPLATE).set_params(
        core__preprocessors=[
            ("imputer", ImputeFeaturesTransformer(default_fill=0)),
            ("bin", Binarizer())
        ],
        core__predictor=("clf", SklearnNumericPredictorWrapper(
            predictor=("mnb", MultinomialNB()),
            predict_method="predict_proba"
        )),
        core__postprocessors=("prob_col", Slice2DColumnsTransformer(columns=1))
    )

    HYPER_PARAMS_SPEC = {
        "core__imputer__strategy": {
            "default": "median",
            "grid": ["mean", "median"]
        },
        "core__bin__threshold": {
            "default": 0.0,
            "grid": [0.0, 0.5, 1, 5]
        },
        "core__clf__mnb__alpha": {
            "default": 0.1,
            "grid": expon()
        },
        "core__clf__mnb__fit_prior": {
            "default": True,
            "grid": [False, True]
        }
    }

    def fit_predictor(self, x, y, stratify):
        self.model.set_params(
            core__clf__min_features_per_sample=self.extra_params["min_features_per_sample"],
            core__clf__max_features_per_sample=self.extra_params["max_features_per_sample"]
        ).fit(X=x, y=y)

    def get_feature_importance(self):
        inner_clf = self.model.get_params(deep=True)["core__clf__mnb"]
        pos_cls_log_probs = inner_clf.feature_log_prob_[1]
        flat_gf = flatten_grouped_features(self.grouped_features)

        feature_importance = []
        for (group, feature), log_prob in zip(flat_gf, pos_cls_log_probs):
            feature_importance.append({
                "name": "{}.{}".format(group, feature),
                "type": "importance",
                "value": abs(log_prob)
            })

        return feature_importance


class LalTfidfScaledSgdcPLPA(BaseLalBinaryRankerPLPA):
    """
    Look-alike binary ranking pipeline:
    <Imputer> --> <GroupedFeaturesTfidfTransformer> --> <StandardScaler> --> <SGDClassifier> --> <ScoreEqualizer>.
    PLPA = Python Learn Python Apply.
    """

    MODEL_TEMPLATE = sb.clone(BaseLalBinaryRankerPLPA.MODEL_TEMPLATE).set_params(
        core__preprocessors=[
            ("imputer", ImputeFeaturesTransformer(default_fill=0)),
            ("tfidf", GroupedFeaturesTfidfTransformer()),
            ("std", StandardScaler(with_mean=False))
        ],
        core__predictor=("clf", SklearnNumericPredictorWrapper(
            predictor=("sgdc", SGDClassifier(penalty="elasticnet", random_state=0, n_jobs=1, n_iter=5, eta0=0.01)),
            predict_method="_predict_proba"
        )),
        core__postprocessors=("prob_col", Slice2DColumnsTransformer(columns=1))
    )

    HYPER_PARAMS_SPEC = {
        "core__imputer__strategy": {
            "default": "median",
            "grid": ["mean", "median"]
        },
        "core__tfidf__norm": {
            "default": "l1",
            "grid": ["l1", "l2", None]
        },
        "core__tfidf__use_idf": {
            "default": True,
            "grid": [False, True]
        },
        "core__tfidf__smooth_idf": {
            "default": True,
            "grid": [True]
        },
        "core__tfidf__sublinear_tf": {
            "default": False,
            "grid": [False, True]
        },
        "core__std__with_std": {
            "default": True,
            "grid": [False, True]
        },
        "core__clf__sgdc__loss": {
            "default": "log",
            "grid": ["log", "modified_huber"]
        },
        "core__clf__sgdc__alpha": {
            "default": 0.00005,
            "grid": [0.005, 0.001, 0.0005, 0.0001, 0.00005, 0.00001]
        },
        "core__clf__sgdc__l1_ratio": {
            "default": 0.3,
            "grid": uniform(loc=0, scale=1)
        },
        "core__clf__sgdc__learning_rate": {
            "default": "invscaling",
            "grid": ["constant", "invscaling", "optimal"]
        },
        "core__clf__sgdc__eta0": {
            "default": 0.01,
            "grid": beta(scale=0.1, a=1, b=9)
        }
    }

    def fit_predictor(self, x, y, stratify):
        tfidf_features_mask = get_features_mask(
            grouped_features=self.grouped_features,
            preprocessed_groups=self.project_params.get("tfidf_feature_groups")
        )

        self.model.set_params(
            core__clf__min_features_per_sample=self.extra_params["min_features_per_sample"],
            core__clf__max_features_per_sample=self.extra_params["max_features_per_sample"]
        ).fit(
            X=x, y=y, core__fit__tfidf__fit__feature_groups=tfidf_features_mask
        )

    def get_feature_importance(self):
        inner_clf = self.model.get_params(deep=True)["core__clf__sgdc"]
        flat_gf = flatten_grouped_features(self.grouped_features)

        feature_importance = []
        for (group, feature), w in zip(flat_gf, inner_clf.coef_[0]):
            feature_importance.append({
                "name": "{}.{}".format(group, feature),
                "type": "importance",
                "value": w
            })

        return feature_importance
