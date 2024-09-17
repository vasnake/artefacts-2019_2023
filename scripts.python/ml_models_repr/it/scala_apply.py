import os
import sys
import six
import json
import traceback
import numpy as np

from backports import tempfile
from cached_property import cached_property

from prj.adapters.predict import PredictAdapter
from prjcore.utils.io import write_json
from prj.logs import LoggingMixin
from prj.adapters.utils import deconstruct, get_location

from sklearn.naive_bayes import MultinomialNB
from sklearn.linear_model import SGDClassifier
from prjcore.predictors.pipeline import MLPipeline
from sklearn.preprocessing import Binarizer, StandardScaler
from prjcore.predictors import SklearnNumericPredictorWrapper, SklearnPredictorWrapper

from prjcore.transformers import (
    ScoreEqualizeTransformer,
    ImputeFeaturesTransformer,
    Slice2DColumnsTransformer,
    SampleBasedGroupedTransformer,
    GroupedFeaturesTfidfTransformer
)

import sklearn2pmml


class BaseRepr(object):

    def __init__(self, obj=None, obj_type=None):
        self._items = {}
        if obj:
            if not isinstance(obj, obj_type):
                raise ValueError("Unknown object type {}, expected {}".format(type(obj), obj_type))
            self._put("py_class", get_location(obj))

    def as_dict(self):
        return self._items

    def _put(self, name, value):
        if not (isinstance(name, str) and len(name) >= 3):
            raise ValueError("Invalid item name: {} `{}`".format(type(name), name))
        if not isinstance(value, (list, dict, int, float, str)):
            raise ValueError("Unknown value type: {}, value: {}".format(type(value), value.__repr__()))
        self._items[name] = value

    def _get(self, name):
        return self._items.get(name)

    def _assert_content(self):
        if len(self._items) < 2:
            raise ValueError("Empty stage representation, {}".format(self._items))

    @staticmethod
    def _assert_stage_structure(stage):
        if not (isinstance(stage, (tuple, list)) and len(stage) == 2):
            raise ValueError("Stage must be a tuple (name, object), got {} {}".format(type(stage), stage))


class BasePredictorRepr(BaseRepr):

    def __init__(self, obj=None, obj_type=None, predict_method=None, expected_predict_method=None):
        super(BasePredictorRepr, self).__init__(obj, obj_type)

        if obj is not None:
            if predict_method != expected_predict_method:
                raise ValueError("Unknown predict method `{}`, expected `{}`".format(
                    predict_method,
                    expected_predict_method
                ))
            self._put("predict_method", predict_method)
            self._put("input_length", self._get_input_length(obj))
            self._put("predict_length", self._get_predict_length(obj))

    def _assert_content(self):
        if len(self._items) < 3:
            raise ValueError("Empty stage representation, {}".format(self._items))

    @staticmethod
    def _get_input_length(predictor):
        raise NotImplementedError

    @staticmethod
    def _get_predict_length(predictor):
        raise NotImplementedError


class BaseWrapperRepr(BaseRepr):

    REPR_FACTORIES = {}

    def __init__(self, obj=None, obj_type=None):
        super(BaseWrapperRepr, self).__init__(obj, obj_type)
        if not self.REPR_FACTORIES:
            self.REPR_FACTORIES = load_repr_factories()

    def _assert_factory(self, fqn):
        if fqn not in self.REPR_FACTORIES.keys():
            raise ValueError("Unknown stage FQN: `{}`".format(fqn))

    def _get_stage_repr(self, stage, **kwargs):
        self._assert_stage_structure(stage)
        name, obj = stage
        fqn = get_location(obj)
        self._assert_factory(fqn)
        stage_repr = self.REPR_FACTORIES[fqn](obj, **kwargs)  # type: BaseRepr
        stage_repr._assert_content()
        return dict(kwargs, **{
            "stage_name": name,
            "stage_class": fqn,
            "stage_repr": stage_repr.as_dict()
        })


class ImputeFeaturesTransformerRepr(BaseRepr):

    def __init__(self, imputer=None):
        super(ImputeFeaturesTransformerRepr, self).__init__(imputer, ImputeFeaturesTransformer)
        if imputer is not None:
            self._put("default_fill", imputer.default_fill)
            values = self.imputer_values(imputer)
            if values:
                self._put("imputer_values", values)

    @staticmethod
    def imputer_values(imputer):
        if imputer is None:
            return []
        else:
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


class BinarizerRepr(BaseRepr):
    def __init__(self, binarizer=None):
        super(BinarizerRepr, self).__init__(binarizer, Binarizer)
        if binarizer is not None:
            self._put("threshold", binarizer.threshold)


class GroupedFeaturesTfidfTransformerRepr(BaseRepr):

    def __init__(self, tfidf=None):
        super(GroupedFeaturesTfidfTransformerRepr, self).__init__(tfidf, GroupedFeaturesTfidfTransformer)

        if tfidf is not None:
            # TODO: check params values compatibility with scala-apply
            self._put("transformer_params", tfidf.get_params())  # norm, use_idf, smooth_idf, sublinear_tf
            self._put("n_features", tfidf._nfeatures)
            self._put("groups", self._extract_groups(tfidf))
            self._put("idf_diags", self._extract_idf_diagonals(tfidf))

    def _extract_idf_diagonals(self, tfidf):
        if self._get("transformer_params").get("use_idf") and len(self._get("groups")) > 0:
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
        super(StandardScalerRepr, self).__init__(scaler, StandardScaler)

        if scaler is not None:
            self._put("with_mean", scaler.with_mean)
            self._put("with_std", scaler.with_std)
            self._put("means", scaler.mean_.tolist() if scaler.with_mean else [])
            self._put("scales", scaler.scale_.tolist() if scaler.with_std else [])


class ScoreEqualizeTransformerRepr(BaseRepr):

    def __init__(self, transformer=None):
        super(ScoreEqualizeTransformerRepr, self).__init__(transformer, ScoreEqualizeTransformer)

        if transformer is not None:
            self._put("minval", transformer._minval)
            self._put("maxval", transformer._maxval)
            self._put("noise", transformer.noise)
            self._put("eps", transformer.eps)
            # _cdf: scipy.interpolate._cubic.PchipInterpolator object
            self._put("coefficients", [row.tolist() for row in transformer._cdf.c])  # shape (4, n_intervals)
            self._put("intervals", transformer._cdf.x.tolist())  # intervals edges: shape (n_intervals + 1)


class Slice2DColumnsTransformerRepr(BaseRepr):

    def __init__(self, slicer=None):
        super(Slice2DColumnsTransformerRepr, self).__init__(slicer, Slice2DColumnsTransformer)
        if slicer is not None:
            self._put("indices", slicer._colind.tolist())


class SGDClassifierRepr(BasePredictorRepr):

    def __init__(self, clf=None, predict_method=None):
        super(SGDClassifierRepr, self).__init__(clf, SGDClassifier, predict_method, "_predict_proba")

        if clf is not None:
            pmml = self._extract_pmml_dump(clf)
            if not pmml:
                raise ValueError("SGDClassifier sklearn2pmml dump has failed")
            self._put("pmml_dump", pmml)

    @staticmethod
    def _get_input_length(clf):
        return clf.coef_.shape[-1]

    @staticmethod
    def _get_predict_length(clf):
        return clf.classes_.shape[0]

    @staticmethod
    def _extract_pmml_dump(clf):
        # add_java_to_path()
        with tempfile.TemporaryDirectory() as tmp_local_dir:
            path = os.path.join(tmp_local_dir, "sgd-classifier.pmml")
            pipeline = sklearn2pmml.make_pmml_pipeline(clf)
            sklearn2pmml.sklearn2pmml(pipeline, path, with_repr=True, debug=False)
            with open(path, "r") as f:
                text = f.read()
        return text


class MultinomialNBRepr(BasePredictorRepr):

    def __init__(self, predictor=None, predict_method=None):
        super(MultinomialNBRepr, self).__init__(predictor, MultinomialNB, predict_method, "predict_proba")

        if predictor is not None:
            self._put("class_log_prior", predictor.class_log_prior_.tolist())
            self._put("feature_log_prob", [row.tolist() for row in predictor.feature_log_prob_])

    @staticmethod
    def _get_input_length(predictor):
        return predictor.feature_log_prob_.shape[1]

    @staticmethod
    def _get_predict_length(predictor):
        return predictor.classes_.shape[0]


class SBGroupedTransformerRepr(BaseWrapperRepr):

    def __init__(self, wrapper=None):
        super(SBGroupedTransformerRepr, self).__init__(wrapper, SampleBasedGroupedTransformer)

        if wrapper and wrapper.transformers:
            transformers = []
            for stage in wrapper.transformers:
                transformers.append(self._get_stage_repr(stage))
            self._put("transformers", transformers)


class SPredictorWrapperRepr(BaseWrapperRepr):

    def __init__(self, wrapper=None, obj_type=SklearnPredictorWrapper):
        super(SPredictorWrapperRepr, self).__init__(wrapper, obj_type)

        if wrapper and wrapper.predictor:
            stage = wrapper.predictor
            stage_dict = self._get_stage_repr(stage, predict_method=wrapper.predict_method)
            self._put("predictor", stage_dict)
            self._put("predict_length", stage_dict["stage_repr"].get("predict_length"))
            self._put("predict_method", wrapper.predict_method)


class SNPredictorWrapperRepr(SPredictorWrapperRepr):

    def __init__(self, wrapper=None):
        super(SNPredictorWrapperRepr, self).__init__(wrapper, SklearnNumericPredictorWrapper)
        if wrapper:
            self._put("min_features_per_sample", wrapper.min_features_per_sample)
            self._put("max_features_per_sample", wrapper.max_features_per_sample)


class MLPipelineRepr(BaseWrapperRepr):

    def __init__(self, pipeline=None):
        super(MLPipelineRepr, self).__init__(pipeline, MLPipeline)

        if pipeline:
            self._assert_stage_structure(pipeline.predictor)
            self._put("predictor", self._get_stage_repr(pipeline.predictor))
            for name, lst in [("preprocessors", pipeline.preprocessors), ("postprocessors", pipeline.postprocessors)]:
                if not isinstance(lst, (tuple, list)):
                    raise ValueError("Unknown pipeline {} list: {}".format(name, lst.__repr__()))
                self._put("{}_length".format(name), len(lst))
                self._put(name, [self._get_stage_repr(stage) for stage in lst])

    def _assert_content(self):
        if len(self._items) < 4:
            raise ValueError("Empty stage representation, {}".format(self._items))


def load_repr_factories():
    return {
        # preprocessors
        "sklearn.preprocessing.data.Binarizer": BinarizerRepr,
        "sklearn.preprocessing.data.StandardScaler": StandardScalerRepr,
        "prjcore.transformers.preprocessing.ImputeFeaturesTransformer": ImputeFeaturesTransformerRepr,
        "prjcore.transformers.preprocessing.GroupedFeaturesTfidfTransformer": GroupedFeaturesTfidfTransformerRepr,
        # postprocessors
        "prjcore.transformers.postprocessing.ScoreEqualizeTransformer": ScoreEqualizeTransformerRepr,
        "prjcore.transformers.postprocessing.Slice2DColumnsTransformer": Slice2DColumnsTransformerRepr,
        # predictors
        "sklearn.naive_bayes.MultinomialNB": MultinomialNBRepr,
        "sklearn.linear_model.stochastic_gradient.SGDClassifier": SGDClassifierRepr,
        # wrappers
        "prjcore.predictors.wrapper.SklearnPredictorWrapper": SPredictorWrapperRepr,
        "prjcore.predictors.wrapper.SklearnNumericPredictorWrapper": SNPredictorWrapperRepr,
        "prjcore.transformers.composition.SampleBasedGroupedTransformer": SBGroupedTransformerRepr,
        "prjcore.predictors.pipeline.MLPipeline": MLPipelineRepr
    }


def log_exception(f):
    def wrapper(*args, **kwargs):
        try:
            return f(*args, **kwargs)
        except Exception as e:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            args[0].error("Exception: {}, e: {},\nvalue: {},\nstacktrace:\n{}".format(
                exc_type,
                e,
                exc_value,
                "".join(traceback.format_tb(exc_traceback))
            ))
            return None
    return wrapper


class ScalaApplyModelAdapter(LoggingMixin):

    MODEL_FILENAME = "model_repr.json"

    PREPROCESSORS = (ImputeFeaturesTransformer, Binarizer, GroupedFeaturesTfidfTransformer, StandardScaler)
    POSTPROCESSORS = (Slice2DColumnsTransformer, SampleBasedGroupedTransformer, ScoreEqualizeTransformer)
    PREDICTOR_WRAPPERS = (SklearnNumericPredictorWrapper, SklearnPredictorWrapper)

    PREDICTORS = {
        "predict_proba": (MultinomialNB,),
        "_predict_proba": (SGDClassifier,)
    }

    # TODO: remove after adding dynamic output columns support to ApplyModelsTransformer
    LAL_COLUMNS = {
        "score": {
            "py_class": "prj.adapters.predict.PredictAdapter",
            "kwargs": {
                "label_adapter": {
                    "py_class": "prj.adapters.label.LabelAdapter",
                    "kwargs": {
                        "converters": [
                            {
                                "py_class": "prj.adapters.label.SliceColumnConverter",
                                "kwargs": {"dtype": "float", "col_index": 0}
                            }
                        ]
                    }
                }
            }
        },
        "scores_raw": {
            "py_class": "prj.adapters.predict.PredictAdapter",
            "kwargs": {
                "params": {"apply_postprocessors": False}
            }
        },
        "scores_trf": {
            "py_class": "prj.adapters.predict.PredictAdapter"
        },
        "category": {
            "py_class": "prj.adapters.predict.PredictAdapter",
            "kwargs": {
                "label_adapter": {
                    "py_class": "prj.adapters.label.LabelAdapter",
                    "kwargs": {
                        "converters": [
                            {
                                "py_class": "prj.adapters.label.SliceColumnConverter",
                                "kwargs": {"dtype": "float", "col_index": 0}
                            },
                            {
                                "py_class": "prj.adapters.label.ConstantConverter",
                                "kwargs": {"dtype": "str", "constant": "positive"}
                            }
                        ]
                    }
                }
            }
        }
    }

    def __init__(self, estimator=None, predict_adapters=None):
        self.estimator = estimator if estimator else MLPipeline()
        self.predict_adapters = predict_adapters if predict_adapters else {}

    @log_exception
    def write_model(self, save_dir=".", **kwargs):
        """Deconstruct model pipeline to stages and write model representation to json file.

        Should not raise any exception. On any error json file will not be created.
        """
        try:
            estimator_repr = MLPipelineRepr(self.estimator).as_dict()
        except Exception:
            self.error("Invalid pipeline: {}".format(self.estimator))
            raise

        write_json(dict(kwargs, **estimator_repr), os.path.join(save_dir, self.MODEL_FILENAME))
        return True

    @cached_property
    @log_exception
    def is_compatible(self):
        """Return True if model can be applied with ApplyModelsTransformer.

        Should not raise any exception. On any error model considered un-compatible.
        """
        # TODO: consider using self.write_model(dry_run=True) for compatibility checks
        # TODO: add logic for cases with empty estimator, will be used in apply app
        return (
            self._is_estimator_compatible(self.estimator)
            and
            self._is_predict_adapters_compatible(self.predict_adapters)
        )

    def _is_estimator_compatible(self, estimator):
        """Check estimator pipeline for scala-apply compatibility.

        Compatible elements

        preprocessors (ImputeFeaturesTransformer, Binarizer, GroupedFeaturesTfidfTransformer, StandardScaler)

        predictor.predict_method (
            SklearnNumericPredictorWrapper,
            MultinomialNB.predict_proba,
            SGDClassifier._predict_proba
        )

        postprocessors (Slice2DColumnsTransformer, SampleBasedGroupedTransformer, ScoreEqualizeTransformer)
        """
        if not isinstance(estimator, MLPipeline):
            self.warn("Estimator must be of type MLPipeline, got {}".format(type(estimator)))
            return False

        if estimator.preprocessors is None or estimator.postprocessors is None or estimator.predictor is None:
            raise ValueError("Empty estimator")

        def check_preprocessor(obj):
            res, msg = self._check_stage_type(obj, *self.PREPROCESSORS)
            if not res:
                self.warn(msg)
            return res

        def check_postprocessor(obj):
            res, msg = self._check_stage_type(obj, *self.POSTPROCESSORS)
            if not res:
                self.warn(msg)
                return False

            if isinstance(obj[1], SampleBasedGroupedTransformer):
                for x in obj[1].transformers:
                    res, msg = self._check_stage_type(x, *self.POSTPROCESSORS)
                    if not res:
                        self.warn(msg)
                        return False

            return True

        def check_predictor(obj):
            res, msg = self._check_stage_type(obj, *self.PREDICTOR_WRAPPERS)
            if not res:
                self.warn(msg)
                return False

            method = obj[1].predict_method
            if method not in self.PREDICTORS.keys():
                self.warn("Unknown predict method: `{}`, expected one of {}".format(method, self.PREDICTORS.keys()))
                return False

            res, msg = self._check_stage_type(obj[1].predictor, *self.PREDICTORS[method])
            if not res:
                self.warn(msg)
                return False

            return True

        preprocessors_compatible = all(check_preprocessor(x) for x in estimator.preprocessors)
        postprocessors_compatible = all(check_postprocessor(x) for x in estimator.postprocessors)
        predictor_compatible = check_predictor(estimator.predictor)

        return preprocessors_compatible and predictor_compatible and postprocessors_compatible

    def _is_predict_adapters_compatible(self, predict_adapters):
        """Return True if output columns supported by ApplyModelsTransformer"""
        # TODO: add dynamic output schema support to scala-apply

        def col_in_adapters(name, spec):
            if name not in predict_adapters.keys():
                self.warn("Column `{}` must be in predict_adapters set".format(name))
                return False

            adapter = predict_adapters[name]
            if not isinstance(adapter, PredictAdapter):
                self.warn("Unknown adapter type: {}".format(type(adapter)))
                return False
            if not deconstruct(adapter) == spec:
                self.warn("Wrong adapter `{}` spec:\n{}, expected\n{}".format(
                    name, json.dumps(deconstruct(adapter), indent=4), json.dumps(spec, indent=4)
                ))
                return False
            return True

        if not isinstance(predict_adapters, dict):
            self.warn("Unknown predict_adapters collection type: {}".format(type(predict_adapters)))
            return False
        if not len(predict_adapters.keys()) == 4:
            self.warn("Predicted columns collection size must be = 4")
            return False
        return all(col_in_adapters(name, spec) for name, spec in six.iteritems(self.LAL_COLUMNS))

    @staticmethod
    def _check_stage_type(stage, *valid_types):
        if not isinstance(stage, (tuple, list)):
            return False, "Stage must be of type tuple, got {}".format(type(stage))
        if not len(stage) == 2:
            return False, "Stage tuple size must be = 2, got {}".format(len(stage))
        if not isinstance(stage[1], tuple(valid_types)):
            return False, "Unknown stage type: {}, must be one of: {}".format(type(stage[1]), list(valid_types))
        return True, ""

    @staticmethod
    def _stage_fqn(stage):
        if isinstance(stage, (tuple, list)) and len(stage) == 2:
            return ScalaApplyModelAdapter._stage_fqn(stage[1])
        return get_location(stage)
