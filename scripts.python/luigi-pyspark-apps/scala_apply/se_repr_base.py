import os
import shutil
import tempfile

import numpy as np

from sklearn.preprocessing import Binarizer
from sklearn.naive_bayes import MultinomialNB
from sklearn.linear_model import SGDClassifier
from sklearn.preprocessing import StandardScaler

from dmcore.predictors import SklearnNumericPredictorWrapper

from dmcore.transformers import (
    GroupedFeaturesTfidfTransformer,
    ImputeFeaturesTransformer,
    ScoreEqualizeTransformer,
    SampleBasedGroupedTransformer
)

import sklearn2pmml

from .ml_base import MLPipeline


def set_java_subprocess_env():
    """
    Add (if exists) `$JAVA_HOME/bin` to $PATH environment variable.
    We have to do this for sklearn2pmml pipeline converter subprocess,
    who call `java` executable in hope that it will be version 1.8.
    """
    java_home = os.getenv("JAVA_HOME")
    if java_home:
        java_bin = os.path.join(java_home, "bin")
        path = os.environ["PATH"]
        if java_bin not in path.split(os.pathsep):
            os.environ["PATH"] = java_bin + os.pathsep + path


class BaseRepr(object):

    def __init__(self):
        self._items = {}

    def as_dict(self):
        return self._items


class ImputeFeaturesTransformerRepr(BaseRepr):

    def __init__(self, imputer=None):
        super(ImputeFeaturesTransformerRepr, self).__init__()
        self._items["imputer_values"] = self.imputer_values(imputer)

    @staticmethod
    def imputer_values(imputer):
        if imputer is None:
            return []
        else:
            if not isinstance(imputer, ImputeFeaturesTransformer):
                raise TypeError("ImputeFeaturesTransformer type expected")
            missing_col_val = imputer.default_fill
            missing_cols_mask = imputer._all_missing_col_mask.tolist()
            iv_list = [missing_col_val for _ in missing_cols_mask]

            if all(missing_cols_mask):
                return iv_list

            data_col_vals = imputer._imputer.statistics_.tolist()
            data_col_indices = np.where(~imputer._all_missing_col_mask)[0].tolist()

            for v, k in zip(data_col_vals, data_col_indices):
                iv_list[k] = v

            return iv_list


class ScoreEqualizeTransformerRepr(BaseRepr):

    def __init__(self, transformer=None):
        super(ScoreEqualizeTransformerRepr, self).__init__()
        self._items["minval"] = 0
        self._items["maxval"] = 0
        self._items["noise"] = 0
        self._items["eps"] = 0
        self._items["coefficients"] = []  # shape (4, n_intervals)
        self._items["intervals"] = []  # shape (n_intervals + 1)

        if transformer is not None:
            if not isinstance(transformer, ScoreEqualizeTransformer):
                raise TypeError("ScoreEqualizeTransformer type expected")
            self._items["minval"] = transformer._minval
            self._items["maxval"] = transformer._maxval
            self._items["noise"] = transformer.noise
            self._items["eps"] = transformer.eps
            # _cdf: scipy.interpolate._cubic.PchipInterpolator object
            self._items["coefficients"] = [row.tolist() for row in transformer._cdf.c]
            self._items["intervals"] = transformer._cdf.x.tolist()


class SBGroupedTransformerRepr(BaseRepr):

    # TODO: inner_transformer_repr_factory should be a map: name => transformer_repr_factory
    def __init__(self, outer_transformer=None, inner_transformer_repr_factory=ScoreEqualizeTransformerRepr):
        super(SBGroupedTransformerRepr, self).__init__()
        self._items["transformers"] = {}

        if outer_transformer is not None:
            if not isinstance(outer_transformer, SampleBasedGroupedTransformer):
                raise TypeError("SampleBasedGroupedTransformer type expected")
            self._items["transformers"] = {
                name: inner_transformer_repr_factory(tr).as_dict()
                for name, tr in outer_transformer.transformers
            }


class SNPredictorWrapperRepr(BaseRepr):

    def __init__(self, predictor_repr_factory, wrapper=None):
        super(SNPredictorWrapperRepr, self).__init__()
        self._items["min_features_per_sample"] = 1
        self._items["max_features_per_sample"] = 1000
        self._items["predictor_repr"] = predictor_repr_factory().as_dict()

        if wrapper is not None:
            if not isinstance(wrapper, SklearnNumericPredictorWrapper):
                raise TypeError("SklearnNumericPredictorWrapper type expected")
            self._items["min_features_per_sample"] = wrapper.min_features_per_sample
            self._items["max_features_per_sample"] = wrapper.max_features_per_sample
            self._items["predictor_repr"] = self._extract_predictor_repr(wrapper, predictor_repr_factory).as_dict()

        self._items["predict_length"] = self._items["predictor_repr"]["predict_length"]

    @staticmethod
    def _extract_predictor_repr(wrapper, predictor_repr_factory):
        _, predictor = wrapper.predictor
        return predictor_repr_factory(predictor)


class BaseLalBinaryRankerRepr(BaseRepr):
    PREDICTOR_REPR_FACTORY = NotImplemented

    def __init__(self, model=None):
        super(BaseLalBinaryRankerRepr, self).__init__()
        self._items["imputer_repr"] = ImputeFeaturesTransformerRepr().as_dict()
        self._items["equalizer_repr"] = SBGroupedTransformerRepr().as_dict()

        self._items["predictor_wrapper_repr"] = SNPredictorWrapperRepr(
            predictor_repr_factory=self.PREDICTOR_REPR_FACTORY
        ).as_dict()

        if model is not None:
            if not isinstance(model, MLPipeline):
                raise TypeError("MLPipeline type expected")

            self._items["imputer_repr"] = ImputeFeaturesTransformerRepr(
                self._predictor_preprocessor(model, "imputer")
            ).as_dict()

            self._items["equalizer_repr"] = SBGroupedTransformerRepr(
                model.postprocessors[0][1]
            ).as_dict()

            self._items["predictor_wrapper_repr"] = SNPredictorWrapperRepr(
                predictor_repr_factory=self.PREDICTOR_REPR_FACTORY,
                wrapper=model.predictor[1].predictor[1]
            ).as_dict()

    @staticmethod
    def _predictor_preprocessor(model, name):
        preprocessors = model.predictor[1].preprocessors
        transformers = [obj for nm, obj in preprocessors if nm == name]
        return transformers[0]


class SGDClassifierRepr(BaseRepr):

    def __init__(self, clf=None):
        super(SGDClassifierRepr, self).__init__()
        self._items["input_length"] = 0
        self._items["predict_length"] = 1
        self._items["pmml_dump"] = ""

        if clf is not None:
            if not isinstance(clf, SGDClassifier):
                raise TypeError("SGDClassifier type expected")
            self._items["input_length"] = clf.coef_.shape[-1]
            self._items["predict_length"] = clf.classes_.shape[0]
            self._items["pmml_dump"] = self._extract_pmml_dump(clf)

    @staticmethod
    def _extract_pmml_dump(clf):
        path = os.path.join(tempfile.mkdtemp(), "sgd-classifier.pmml")
        try:
            set_java_subprocess_env()
            pipeline = sklearn2pmml.make_pmml_pipeline(clf)
            sklearn2pmml.sklearn2pmml(pipeline, path, with_repr=True, debug=False)
            with open(path, "r") as f:
                text = f.read()
        finally:
            shutil.rmtree(os.path.dirname(path), ignore_errors=True)
        return text


class GroupedFeaturesTfidfTransformerRepr(BaseRepr):

    def __init__(self, tfidf=None):
        super(GroupedFeaturesTfidfTransformerRepr, self).__init__()
        self._items["transformer_params"] = {}
        self._items["n_features"] = 0
        self._items["groups"] = []
        self._items["idf_diags"] = []

        if tfidf is not None:
            if not isinstance(tfidf, GroupedFeaturesTfidfTransformer):
                raise TypeError("GroupedFeaturesTfidfTransformer type expected")

            self._items["transformer_params"] = tfidf.get_params()  # norm, use_idf, smooth_idf, sublinear_tf
            self._items["n_features"] = tfidf._nfeatures
            self._items["groups"] = self._extract_groups(tfidf)
            self._items["idf_diags"] = self._extract_idf_diagonals(tfidf)

    def _extract_idf_diagonals(self, tfidf):
        if self._items["transformer_params"]["use_idf"] and len(self._items["groups"]) > 0:
            return [self._get_skl_idf_diagonal(tr) for tr in tfidf._transformers]
        else:
            return []

    @staticmethod
    def _get_skl_idf_diagonal(skl_transformer):
        return skl_transformer._idf_diag.diagonal().tolist()

    @staticmethod
    def _extract_groups(tfidf):
        groups = tfidf._groups_idx_lst
        if groups:
            return [group.tolist() for group in groups]
        else:
            return []


class StandardScalerRepr(BaseRepr):

    def __init__(self, scaler=None):
        super(StandardScalerRepr, self).__init__()
        self._items["with_mean"] = False
        self._items["with_std"] = False
        self._items["means"] = []
        self._items["scales"] = []

        if scaler is not None:
            if not isinstance(scaler, StandardScaler):
                raise TypeError("StandardScaler type expected")
            self._items["with_mean"] = scaler.with_mean
            self._items["with_std"] = scaler.with_std
            self._items["means"] = scaler.mean_.tolist() if scaler.with_mean else []
            self._items["scales"] = scaler.scale_.tolist() if scaler.with_std else []


class MultinomialNBRepr(BaseRepr):

    def __init__(self, predictor=None):
        super(MultinomialNBRepr, self).__init__()
        self._items["input_length"] = 0
        self._items["predict_length"] = 1
        self._items["class_log_prior"] = []
        self._items["feature_log_prob"] = []

        if predictor is not None:
            if not isinstance(predictor, MultinomialNB):
                raise TypeError("MultinomialNB type expected")
            self._items["input_length"] = predictor.feature_log_prob_.shape[1]
            self._items["predict_length"] = predictor.classes_.shape[0]
            self._items["class_log_prior"] = predictor.class_log_prior_.tolist()
            self._items["feature_log_prob"] = [row.tolist() for row in predictor.feature_log_prob_]


class BinarizerRepr(BaseRepr):

    def __init__(self, binarizer=None):
        super(BinarizerRepr, self).__init__()
        self._items["threshold"] = 0.0

        if binarizer is not None:
            if not isinstance(binarizer, Binarizer):
                raise TypeError("Binarizer type expected")
            self._items["threshold"] = binarizer.threshold
