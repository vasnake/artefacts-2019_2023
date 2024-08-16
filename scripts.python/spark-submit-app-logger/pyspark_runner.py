"""
TEST

pyspark job that make use of custom spark.ml transformer
"""

import sys
from pyspark import SparkContext
from pyspark.sql import SparkSession

from pyspark import keyword_only
from pyspark.ml.param.shared import *
from pyspark.ml.common import inherit_doc
from pyspark.ml.util import JavaMLReadable, JavaMLWritable
from pyspark.ml.wrapper import JavaEstimator, JavaModel


class HasGroupCols(Params):
    groupColumns = Param(
        Params._dummy(),
        "groupColumns",
        "stratification columns, CSV",
        typeConverter=TypeConverters.toString
    )

    def __init__(self):
        super(HasGroupCols, self).__init__()
        self._setDefault(groupColumns="")

    def setGroupColumns(self, value):
        return self._set(groupColumns=value)

    def getGroupColumns(self):
        return self.getOrDefault(self.groupColumns)


class HasNoise(Params):
    noiseValue = Param(Params._dummy(), "noiseValue", "noise addition", typeConverter=TypeConverters.toFloat)

    def __init__(self):
        super(HasNoise, self).__init__()
        self._setDefault(noiseValue=1e-4)

    def setNoiseValue(self, value):
        return self._set(noiseValue=value)

    def getNoiseValue(self):
        return self.getOrDefault(self.noiseValue)


class HasEpsilon(Params):
    epsValue = Param(Params._dummy(), "epsValue", "delta value", typeConverter=TypeConverters.toFloat)

    def __init__(self):
        super(HasEpsilon, self).__init__()
        self._setDefault(epsValue=1e-3)

    def setEpsValue(self, value):
        return self._set(epsValue=value)

    def getEpsValue(self):
        return self.getOrDefault(self.epsValue)


class HasRandom(Params):
    randomValue = Param(
        Params._dummy(),
        "randomValue",
        "`random` optional replacement",
        typeConverter=TypeConverters.toFloat
    )

    def __init__(self):
        super(HasRandom, self).__init__()
        self._setDefault(randomValue=float("nan"))

    def setRandomValue(self, value):
        return self._set(randomValue=value)

    def getRandomValue(self):
        return self.getOrDefault(self.randomValue)


class HasWeight(Params):
    weightValue = Param(
        Params._dummy(),
        "weightValue",
        "weight coefficient",
        typeConverter=TypeConverters.toFloat
    )

    def __init__(self):
        super(HasWeight, self).__init__()
        self._setDefault(weightValue=1.0)

    def setWeightValue(self, value):
        return self._set(weightValue=value)

    def getWeightValue(self):
        return self.getOrDefault(self.weightValue)


@inherit_doc
class ScoreEqualizeTransformer(
    JavaEstimator,
    HasInputCol,
    HasGroupCols,
    HasNoise,
    HasEpsilon,
    HasRandom,
    JavaMLReadable,
    JavaMLWritable
):

    _fqn = "estimator.ScoreEqualizeTransformer"

    # @formatter:off
    numBins = Param(Params._dummy(), "numBins", "number of bins for spline interpolator", typeConverter=TypeConverters.toInt)
    sampleSize = Param(Params._dummy(), "sampleSize", "max records count in each group", typeConverter=TypeConverters.toInt)
    # @formatter:on

    @keyword_only
    def __init__(
        self,
        inputCol="score_raw",
        numBins=10000,
        groupColumns="",
        noiseValue=1e-4,
        epsValue=1e-3,
        sampleSize=100000,
        randomValue=float("nan")
    ):
        super(ScoreEqualizeTransformer, self).__init__()
        self._java_obj = self._new_java_obj(self._fqn, self.uid)
        self._setDefault(
            inputCol="score_raw",
            numBins=10000,
            groupColumns="",
            noiseValue=1e-4,
            epsValue=1e-3,
            sampleSize=100000,
            randomValue=float("nan")
        )
        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    def setParams(
        self,
        inputCol="score_raw",
        numBins=10000,
        groupColumns="",
        noiseValue=1e-4,
        epsValue=1e-3,
        sampleSize=100000,
        randomValue=float("nan")
    ):
        kwargs = self._input_kwargs
        return self._set(**kwargs)

    def setNumBins(self, value):
        return self._set(numBins=value)

    def getNumBins(self):
        return self.getOrDefault(self.numBins)

    def setSampleSize(self, value):
        return self._set(sampleSize=value)

    def getSampleSize(self):
        return self.getOrDefault(self.sampleSize)

    def _create_model(self, java_model):
        return ScoreEqualizeTransformerModel(java_model)


class ScoreEqualizeTransformerModel(
    JavaModel,
    HasOutputCol,
    HasInputCol,
    HasGroupCols,
    JavaMLReadable,
    JavaMLWritable
):

    _fqn = "model.ScoreEqualizeTransformerModel"


class PySparkJob(object):

    def main(self, sc, *args):
        print("PySparkJob started, spark context: `{}`, args: `{}`".format(sc, args))
        self.equalizer_fit_transform(sc)
        print("PySparkJob done.")

    def equalizer_fit_transform(self, sc):
        spark = SparkSession(sc).builder.getOrCreate() # spark-submit app
        df = self._load_df(spark)
        df.show()
        model = ScoreEqualizeTransformer(inputCol="score_raw_train").fit(df)
        output = model.setInputCol("score_raw").setOutputCol("score").transform(df)
        output.show()

    @staticmethod
    def _load_df(spark):
        return spark.createDataFrame([
            ("a", 0.3,  0.1,  0.0),
            ("b", 0.7,  3.14, 0.27968233),
            ("c", 13.0, 26.0, 0.74796144),
            ("d", 17.0, 28.0, 1.0),
            ("e", 27.0, 15.0, 0.4982242)
        ]).toDF("uid", "score_raw_train", "score_raw", "expected")


class PySparkRunner(object):

    def __init__(self, *args):
        self.args = args
        self.job = PySparkJob()

    def run(self):
        with SparkContext() as sc:
            self.job.main(sc, *self.args)


if __name__ == "__main__":
    PySparkRunner(*sys.argv[1:]).run()
