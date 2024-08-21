import os
import abc
import six
import numpy as np

import sklearn.base as sb
from sklearn.metrics import roc_auc_score
from sklearn.linear_model import SGDClassifier
from sklearn.naive_bayes import MultinomialNB, BernoulliNB
from sklearn.preprocessing import StandardScaler, Binarizer

from scipy.stats import kendalltau
from scipy.stats.distributions import beta, expon, uniform, randint

from lightgbm import LGBMClassifier

from ..base import SApplyMixin
from .base import BaseScoring, ScoringPLearnMixin, ScoringPApplyMixin

from dmcore.predictors import SklearnNumericPredictorWrapper
from dmcore.utils.io import write_json

from .base import BaseClal, ClalPLearnMixin, ClalPApplyMixin

from dmcore.metrics import uplift
from dmcore.utils.common import filter_nans
from dmcore.predictors import NMSLibKNeighbors, SklearnNumericPredictorWrapper

from dmcore.transformers import (
    ImputeFeaturesTransformer,
    GroupedFeaturesTfidfTransformer,
    Slice2DColumnsTransformer,
    GroupedFeaturesTfidfTransformer, ScoreEqualizeTransformer,
    Slice2DColumnsTransformer, SampleBasedGroupedTransformer,
    ImputeFeaturesTransformer
)

from dmgrinder.interface.models.base import SApplyMixin
from dmgrinder.interface.tools import flatten_grouped_features, get_features_mask

from dmgrinder.interface.models.clal.sa_repr.lal_binary_ranking import (
    LalTfidfScaledSgdcRepr,
    LalBinarizedMultinomialNbRepr
)


class ScoringCustomLogRegSA(BaseScoring, SApplyMixin):
    """
    Scala Apply for already learned and appropriately serialized
    custom logistic regression scoring model.

    Grinder model as a wrapper for scala-apply transformer called via
    'interface.transformers.scala_wrappers.ApplyModelsTransformer'.
    """

    MODEL_FILENAME = "model_weights.json"

    def set_apply_uid_type(self, uid_type):
        pass


class LalBinarizedMultinomialNbPLSA(BaseLalBinaryRankerSA, LalBinarizedMultinomialNbPLPA):
    MODEL_REPR_FACTORY = LalBinarizedMultinomialNbRepr

    def __init__(self, *args, **kwargs):
        for base in self.__class__.__bases__[::-1]:
            base.__init__(self, *args, **kwargs)

    def save(self, folder='.', **kwargs):
        for base in self.__class__.__bases__[::-1]:
            base.save(self, folder, **kwargs)


class LalTfidfScaledSgdcPLSA(BaseLalBinaryRankerSA, LalTfidfScaledSgdcPLPA):
    MODEL_REPR_FACTORY = LalTfidfScaledSgdcRepr

    def __init__(self, *args, **kwargs):
        for base in self.__class__.__bases__[::-1]:
            base.__init__(self, *args, **kwargs)

    def save(self, folder='.', **kwargs):
        for base in self.__class__.__bases__[::-1]:
            base.save(self, folder, **kwargs)
