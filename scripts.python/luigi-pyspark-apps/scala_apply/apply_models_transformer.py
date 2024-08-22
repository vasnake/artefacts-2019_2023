from pyspark import keyword_only
from pyspark.ml.param.shared import *
from pyspark.ml.common import inherit_doc
from pyspark.ml.wrapper import JavaTransformer
from pyspark.ml.util import JavaMLReadable, JavaMLWritable


@inherit_doc
class ApplyModelsTransformer(JavaTransformer, JavaMLReadable, JavaMLWritable):
    """
    Spark.ml transformer.
    Python wrapper for jvm class 'ApplyModelsTransformer'.
    Apply previously saved models to input DataFrame.

    >>> df = spark.createDataFrame(
    ...     [("a", {}, {}, {}, [])],
    ...     ["uid", "topics_m", "vk_groups", "ok_groups", "all_profiles"]
    ... )
    >>> transformer = ApplyModelsTransformer(
    ...     models=ModelConfig.pack_configs_list(*configs),
    ...     keep_columns=ModelConfig.pack_columns_list(*["uid"])
    ... )
    >>> if transformer.initialize():
    ...     transformer.transform(df).head()
    Row(uid=a, ...)

    Parameter `models` consists of a list of dictionaries encoded to a string.
    Each dict contains model configuration starting with `model_type` parameter.
    JVM implementation must know each model_type in order to apply it.

    Parameter `keep_columns` consists of a list of column names encoded to a string.
    These columns will be added to output DataFrame from input w/o transformation.
    """

    # transformer class in jvm
    _fqn = "com.github.vasnake.spark.ml.transformer.ApplyModelsTransformer"

    models = Param(Params._dummy(), "models", "List of models descriptions", typeConverter=TypeConverters.toString)

    keep_columns = Param(
        Params._dummy(),
        "keep_columns",
        "List of columns names to keep in output dataset",
        typeConverter=TypeConverters.toString
    )

    @keyword_only
    def __init__(self, models="", keep_columns=""):
        super(ApplyModelsTransformer, self).__init__()
        self._java_obj = self._new_java_obj(self._fqn, self.uid)
        self._setDefault(models="", keep_columns="")
        self.setParams(models=models, keep_columns=keep_columns)

    @keyword_only
    def setParams(self, models="", keep_columns=""):
        return self._set(models=models, keep_columns=keep_columns)

    def setModels(self, value):
        return self._set(models=value)

    def getModels(self):
        return self.getOrDefault(self.models)

    def setKeepColumns(self, value):
        return self._set(keep_columns=value)

    def getKeepColumns(self):
        return self.getOrDefault(self.keep_columns)

    def initialize(self):
        self._transfer_params_to_java()
        return self._java_obj.initialize()


class ModelConfig(object):
    """
    Config helper class.

    Models descriptions encode/decode helper.
    Separator markers must be synchronized with scala codebase.
    """

    CFG_MODELS_SEPARATOR = ";"
    CFG_MODEL_PARAMETERS_SEPARATOR = ","
    CFG_MODEL_KV_SEPARATOR = "->"
    CFG_COLUMNS_SEPARATOR = ","

    @staticmethod
    def pack_config(cfg):
        """Encode dictionary `cfg` to a string"""
        kv_list = ["{}{}{}".format(k, ModelConfig.CFG_MODEL_KV_SEPARATOR, v) for k, v in cfg.items()]
        text = ModelConfig.CFG_MODEL_PARAMETERS_SEPARATOR.join(kv_list)
        return text

    @staticmethod
    def pack_configs_list(*configs):
        """Encode list of dictionaries to a string"""
        lines = [ModelConfig.pack_config(x) for x in configs]
        text = ModelConfig.CFG_MODELS_SEPARATOR.join(lines)
        return text

    @staticmethod
    def pack_columns_list(*colnames):
        """Encode list of column names to a string"""
        text = ModelConfig.CFG_COLUMNS_SEPARATOR.join(colnames)
        return text


class ApplyModelsTransformerError(Exception):
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return repr(self.value)
