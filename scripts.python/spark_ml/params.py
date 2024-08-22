"""Common parameters that can be used in any spark.ml model.

Based on existing spark.ml code, see pyspark.ml.param.shared.
"""

from pyspark.ml.param.shared import Param, Params, TypeConverters


class HasOutputCol(Params):
    """Mixin for parameter ``outputCol``: output column name."""

    outputCol = Param(Params._dummy(), "outputCol", "output column name.", typeConverter=TypeConverters.toString)

    def __init__(self):
        super(HasOutputCol, self).__init__()

    def setOutputCol(self, value):
        return self._set(outputCol=value)

    def getOutputCol(self):
        return self.getOrDefault(self.outputCol)


class HasGroupCols(Params):
    """Mixin for parameter ``groupColumns``: stratification columns names list."""

    groupColumns = Param(
        Params._dummy(), "groupColumns", "List of stratification columns", typeConverter=TypeConverters.toListString
    )

    def __init__(self):
        super(HasGroupCols, self).__init__()
        self._setDefault(groupColumns=[])

    def setGroupColumns(self, value):
        return self._set(groupColumns=list(value))

    def getGroupColumns(self):
        return self.getOrDefault(self.groupColumns)


class HasNoise(Params):
    """Mixin for parameter ``noiseValue``: range of a random noise value added to input or output."""

    noiseValue = Param(Params._dummy(), "noiseValue", "Noise addition", typeConverter=TypeConverters.toFloat)

    def __init__(self):
        super(HasNoise, self).__init__()
        self._setDefault(noiseValue=1e-4)

    def setNoiseValue(self, value):
        return self._set(noiseValue=value)

    def getNoiseValue(self):
        return self.getOrDefault(self.noiseValue)


class HasEpsilon(Params):
    """Mixin for parameter ``epsValue``: some delta value."""

    epsValue = Param(Params._dummy(), "epsValue", "Delta value", typeConverter=TypeConverters.toFloat)

    def __init__(self):
        super(HasEpsilon, self).__init__()
        self._setDefault(epsValue=1e-3)

    def setEpsValue(self, value):
        return self._set(epsValue=value)

    def getEpsValue(self):
        return self.getOrDefault(self.epsValue)


class HasRandom(Params):
    """Mixin for parameter ``randomValue``: optional value to replace call to the ``random`` method."""

    randomValue = Param(
        Params._dummy(), "randomValue", "Optional replacement for `random`", typeConverter=TypeConverters.toFloat
    )

    def __init__(self):
        super(HasRandom, self).__init__()

    def setRandomValue(self, value):
        return self._set(randomValue=value)

    def getRandomValue(self):
        return self.getOrDefault(self.randomValue)


class HasWeight(Params):
    """Mixin for parameter ``weightValue``: some weight coefficient."""

    weightValue = Param(Params._dummy(), "weightValue", "Weight coefficient", typeConverter=TypeConverters.toFloat)

    def __init__(self):
        super(HasWeight, self).__init__()
        self._setDefault(weightValue=1.0)

    def setWeightValue(self, value):
        return self._set(weightValue=value)

    def getWeightValue(self):
        return self.getOrDefault(self.weightValue)


class HasRankCol(Params):
    """Mixin for parameter ``rankCol``: name for column with rank values."""

    rankCol = Param(Params._dummy(), "rankCol", "Rank column name", typeConverter=TypeConverters.toString)

    def __init__(self):
        super(HasRankCol, self).__init__()

    def setRankCol(self, value):
        return self._set(rankCol=value)

    def getRankCol(self):
        return self.getOrDefault(self.rankCol)


class HasSampling(Params):
    """Mixin for parameters:

    - ``sampleSize``: max number of records in one sample.
    - ``sampleRandomSeed``: seed value for random function used for sampling.
    """

    sampleSize = Param(
        Params._dummy(), "sampleSize", "Max records count in single sample", typeConverter=TypeConverters.toInt
    )
    sampleRandomSeed = Param(
        Params._dummy(), "sampleRandomSeed", "Seed value for random function", typeConverter=TypeConverters.toFloat
    )

    def __init__(self):
        super(HasSampling, self).__init__()
        self._setDefault(sampleSize=100000)

    def setSampleSize(self, value):
        return self._set(sampleSize=value)

    def getSampleSize(self):
        return self.getOrDefault(self.sampleSize)

    def setSampleRandomSeed(self, value):
        return self._set(sampleRandomSeed=value)

    def getSampleRandomSeed(self):
        return self.getOrDefault(self.sampleRandomSeed)


class HasNumClasses(Params):
    """Mixin for parameter ``numClasses``: number of columns in sample row."""

    numClasses = Param(
        Params._dummy(), "numClasses", "Number of items in data vector", typeConverter=TypeConverters.toInt
    )

    def __init__(self):
        super(HasNumClasses, self).__init__()

    def setNumClasses(self, value):
        return self._set(numClasses=value)

    def getNumClasses(self):
        return self.getOrDefault(self.numClasses)


class HasLabels(Params):
    """Mixin for parameter ``labels``: list of unique values."""

    labels = Param(Params._dummy(), "labels", "List of unique values", typeConverter=TypeConverters.toListString)

    def __init__(self):
        super(HasLabels, self).__init__()

    def setLabels(self, value):
        return self._set(labels=list(value))

    def getLabels(self):
        return self.getOrDefault(self.labels)


class HasPriorValues(Params):
    """Mixin for parameter ``priorValues``: list of numeric values."""

    priorValues = Param(
        Params._dummy(), "priorValues", "List of numeric values", typeConverter=TypeConverters.toListFloat
    )

    def __init__(self):
        super(HasPriorValues, self).__init__()

    def setPriorValues(self, value):
        return self._set(priorValues=value)

    def getPriorValues(self):
        return self.getOrDefault(self.priorValues)


class ParamsHelper(Params):
    def _setParams(self, **kwargs):
        return self._set(**dict(filter(lambda k_v: k_v[1] is not None, kwargs.items())))
