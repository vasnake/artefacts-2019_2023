"""Wrappers for spark.ml models implemented in scala lib

Code from pyspark.ml.feature was used as a template.
"""

from pyspark import keyword_only
from pyspark.ml.util import JavaMLReadable, JavaMLWritable
from pyspark.ml.common import inherit_doc
from pyspark.ml.wrapper import JavaModel, JavaEstimator
from pyspark.ml.param.shared import Param, Params, HasInputCol, TypeConverters

from .params import (
    HasNoise,
    HasLabels,
    HasRandom,
    HasEpsilon,
    HasRankCol,
    HasSampling,
    HasGroupCols,
    HasOutputCol,
    ParamsHelper,
    HasNumClasses,
    HasPriorValues,
)


@inherit_doc
class ScoreEqualizeTransformer(
    JavaEstimator,
    HasNoise,
    HasRandom,
    HasEpsilon,
    HasInputCol,
    HasSampling,
    HasOutputCol,
    HasGroupCols,
    ParamsHelper,
    JavaMLReadable,
    JavaMLWritable,
):
    """Python wrapper for Spark ML estimator, jvm class ScoreEqualizerEstimator.

    Implements the fit method of python :class:`postprocessing.ScoreEqualizeTransformer`.

    Use :meth:`fit` to produce trained equalizer transformer, using sample of input dataframe.
    Fitting involves materialization of input data, so you should persist input dataframe before calling :meth:`fit`.

    This implementation supports stratification: for different groups of records different equalizers could be fitted
    using ``groupColumns`` parameter. Sample of each group will be used, according to ``sampleSize`` parameter.

    ``inputCol`` data is considered as invalid if column value is NULL or NaN.
    On fit stage invalid data is filtered out, on transform stage invalid input produces NULL in output.

    A model could not be fitted if no valid data is found in ``inputCol``.
    On transform stage, an absence of fitted model is considered as invalid input.

    >>> from spark_ml.postprocessing import ScoreEqualizeTransformer
    >>> df = spark.createDataFrame( ... )
    >>> model = ScoreEqualizeTransformer(inputCol="score_raw_train", groupColumns=["uid_type", "category"]).fit(df)
    >>> out_df = model.setInputCol("score_raw").setOutputCol("score").transform(df)
    """

    _fqn = "com.github.vasnake.spark.ml.estimator.ScoreEqualizerEstimator"

    numBins = Param(
        Params._dummy(), "numBins", "number of bins for spline interpolator", typeConverter=TypeConverters.toInt
    )

    @keyword_only
    def __init__(
        self,
        numBins=None,
        inputCol=None,
        epsValue=None,
        outputCol=None,
        noiseValue=None,
        sampleSize=None,
        randomValue=None,
        groupColumns=None,
        sampleRandomSeed=None,
    ):
        super(ScoreEqualizeTransformer, self).__init__()
        self._java_obj = self._new_java_obj(self._fqn, self.uid)
        self._setDefault(
            numBins=10000,
            epsValue=1e-3,
            noiseValue=1e-4,
            sampleSize=100000,
            groupColumns=[],
        )
        self._setParams(**self._input_kwargs)

    @keyword_only
    def setParams(
        self,
        numBins=None,
        inputCol=None,
        epsValue=None,
        outputCol=None,
        noiseValue=None,
        sampleSize=None,
        randomValue=None,
        groupColumns=None,
        sampleRandomSeed=None,
    ):
        return self._setParams(**self._input_kwargs)

    def setNumBins(self, value):
        return self._set(numBins=value)

    def getNumBins(self):
        return self.getOrDefault(self.numBins)

    def _create_model(self, java_model):
        return ScoreEqualizeTransformerModel(java_model)


class ScoreEqualizeTransformerModel(JavaModel, HasOutputCol, HasInputCol, HasGroupCols, JavaMLReadable, JavaMLWritable):
    """Spark ML model fitted by ScoreEqualizeTransformer.

    Python wrapper for jvm class ScoreEqualizerModel with implementation of the python
    :class:`postprocessing.ScoreEqualizeTransformer` transform method.
    """

    _fqn = "com.github.vasnake.spark.ml.model.ScoreEqualizerModel"


@inherit_doc
class NEPriorClassProbaTransformer(
    JavaEstimator,
    HasInputCol,
    HasSampling,
    HasOutputCol,
    HasGroupCols,
    HasPriorValues,
    ParamsHelper,
    JavaMLReadable,
    JavaMLWritable,
):
    """Python wrapper for Spark ML estimator, jvm class NEPriorClassProbaEstimator.

    Implements the fit method of the python :class:`postprocessing.NEPriorClassProbaTransformer`.

    Use :meth:`fit` to produce trained transformer, using sample of input dataframe.
    Fitting involves materialization of input data, so you should persist input dataframe before calling :meth:`fit`.

    This implementation supports stratification: for different groups of records, different transformers could be fitted
    using ``groupColumns`` parameter. Sample of each group will be used, according to ``sampleSize`` parameter.

    ``inputCol`` data is considered as invalid if column value is NULL or array ``size != len(priorValues)`` or array
    contains NULL or NaN or negative values, or if ``sum(array) == 0``.
    On fit stage invalid data is filtered out, on transform stage invalid input produces NULL in output.

    A model could not be fitted if no valid rows is found in ``inputCol``.
    On transform stage, an absence of fitted model produces NULL output values.

    >>> from spark_ml.postprocessing import NEPriorClassProbaTransformer
    >>> df = spark.createDataFrame( ... )
    >>> model = NEPriorClassProbaTransformer(inputCol="scores_raw", priorValues=[1.0, 1.0]).fit(df)
    >>> out_df = model.setOutputCol("scores_trf").transform(df)
    """

    _fqn = "com.github.vasnake.spark.ml.estimator.NEPriorClassProbaEstimator"

    @keyword_only
    def __init__(
        self,
        inputCol=None,
        outputCol=None,
        sampleSize=None,
        priorValues=None,
        groupColumns=None,
        sampleRandomSeed=None,
    ):
        super(NEPriorClassProbaTransformer, self).__init__()
        self._java_obj = self._new_java_obj(self._fqn, self.uid)
        self._setDefault(sampleSize=100000, groupColumns=[])
        self._setParams(**self._input_kwargs)

    @keyword_only
    def setParams(
        self,
        inputCol=None,
        outputCol=None,
        sampleSize=None,
        priorValues=None,
        groupColumns=None,
        sampleRandomSeed=None,
    ):
        return self._setParams(**self._input_kwargs)

    def _create_model(self, java_model):
        return NEPriorClassProbaTransformerModel(java_model)


class NEPriorClassProbaTransformerModel(
    JavaModel, HasInputCol, HasOutputCol, HasGroupCols, JavaMLReadable, JavaMLWritable
):
    """Spark ML model fitted by NEPriorClassProbaTransformer.

    Python wrapper for jvm class NEPriorClassProbaModel with implementation of the python
    :class:`postprocessing.NEPriorClassProbaTransformer` transform method.
    """

    _fqn = "com.github.vasnake.spark.ml.model.NEPriorClassProbaModel"


@inherit_doc
class ScoreQuantileThresholdTransformer(
    JavaEstimator,
    HasLabels,
    HasRankCol,
    HasInputCol,
    HasSampling,
    HasGroupCols,
    HasOutputCol,
    HasPriorValues,
    ParamsHelper,
    JavaMLReadable,
    JavaMLWritable,
):
    """Python wrapper for Spark ML estimator, jvm class ScoreQuantileThresholdEstimator.

    Implements the fit method of python :class:`postprocessing.ScoreQuantileThresholdTransformer`.

    Use :meth:`fit` to produce trained transformer, using sample of input dataframe.
    Fitting involves materialization of input data, so you should persist input dataframe before calling :meth:`fit`.

    This implementation supports stratification: for different groups of records different transformers could be fitted
    using ``groupColumns`` parameter. Sample of each group will be used, according to ``sampleSize`` parameter.

    ``inputCol`` data is considered as invalid if column value is NULL or NaN.
    On fit stage invalid data is filtered out, on transform stage invalid input produces NULL in output.

    A model could not be fitted if no valid data is found in ``inputCol``.
    On transform stage, an absence of fitted model is considered as invalid input.

    >>> from spark_ml.postprocessing import ScoreQuantileThresholdTransformer
    >>> df = spark.createDataFrame( ... )
    >>> model = ScoreQuantileThresholdTransformer(inputCol="score_raw", priorValues=[1.0, 1.0]).fit(df)
    >>> out_df = model.setOutputCol("category").setRankCol("score").transform(df)
    """

    _fqn = "com.github.vasnake.spark.ml.estimator.ScoreQuantileThresholdEstimator"

    @keyword_only
    def __init__(
        self,
        labels=None,
        rankCol=None,
        inputCol=None,
        outputCol=None,
        sampleSize=None,
        priorValues=None,
        groupColumns=None,
        sampleRandomSeed=None,
    ):
        super(ScoreQuantileThresholdTransformer, self).__init__()
        self._java_obj = self._new_java_obj(self._fqn, self.uid)
        self._setDefault(sampleSize=100000, groupColumns=[])
        self._setParams(**self._input_kwargs)

    @keyword_only
    def setParams(
        self,
        labels=None,
        rankCol=None,
        inputCol=None,
        outputCol=None,
        sampleSize=None,
        priorValues=None,
        groupColumns=None,
        sampleRandomSeed=None,
    ):
        return self._setParams(**self._input_kwargs)

    def _create_model(self, java_model):
        return ScoreQuantileThresholdTransformerModel(java_model)


class ScoreQuantileThresholdTransformerModel(
    JavaModel, HasInputCol, HasOutputCol, HasRankCol, HasGroupCols, JavaMLReadable, JavaMLWritable
):
    """Spark ML model fitted by ScoreQuantileThresholdTransformer.

    Python wrapper for jvm class ScoreQuantileThresholdModel with implementation of the python
    :class:`postprocessing.ScoreQuantileThresholdTransformer` transform method.
    """

    _fqn = "com.github.vasnake.spark.ml.model.ScoreQuantileThresholdModel"
