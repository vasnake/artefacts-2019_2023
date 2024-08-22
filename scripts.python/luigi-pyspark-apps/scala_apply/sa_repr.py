from .se_repr_base import (
    BaseLalBinaryRankerRepr, 
    SGDClassifierRepr, 
    GroupedFeaturesTfidfTransformerRepr,
    StandardScalerRepr,
    MultinomialNBRepr,
    BinarizerRepr,
)


class LalTfidfScaledSgdcRepr(BaseLalBinaryRankerRepr):
    PREDICTOR_REPR_FACTORY = SGDClassifierRepr

    def __init__(self, model=None):
        super(LalTfidfScaledSgdcRepr, self).__init__(model)
        self._items["tfidf_repr"] = GroupedFeaturesTfidfTransformerRepr().as_dict()
        self._items["scaler_repr"] = StandardScalerRepr().as_dict()

        if model is not None:
            self._items["tfidf_repr"] = GroupedFeaturesTfidfTransformerRepr(
                self._predictor_preprocessor(model, "tfidf")
            ).as_dict()

            self._items["scaler_repr"] = StandardScalerRepr(
                self._predictor_preprocessor(model, "std")
            ).as_dict()


class LalBinarizedMultinomialNbRepr(BaseLalBinaryRankerRepr):
    PREDICTOR_REPR_FACTORY = MultinomialNBRepr

    def __init__(self, model=None):
        super(LalBinarizedMultinomialNbRepr, self).__init__(model)
        self._items["binarizer_repr"] = BinarizerRepr().as_dict()

        if model is not None:
            self._items["binarizer_repr"] = BinarizerRepr(
                self._predictor_preprocessor(model, "bin")
            ).as_dict()
