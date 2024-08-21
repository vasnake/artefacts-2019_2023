import pytest

from apply_models_transformer import ApplyModelsTransformer
from pyspark.sql.types import StructType, StructField, StringType, MapType, FloatType, ArrayType


@pytest.mark.usefixtures("local_spark")
class TestApplyModelsTransformerWrapper(object):

    def test_transformer_constructor(self):
        """Check transformer constructor with parameters"""
        descriptions = "a->foo,b->42;aa->bar,bb->3.14"
        transformer = ApplyModelsTransformer(models=descriptions, keep_columns="uid")
        assert transformer.getKeepColumns() == "uid"
        assert transformer.getModels() == descriptions
        assert not transformer.initialize()  # zero models in description

    def test_transformer_param_setter(self):
        descriptions = "a->foo,b->42;aa->bar,bb->3.14"
        transformer = ApplyModelsTransformer()
        # default params
        assert transformer.getModels() == ""
        assert transformer.getKeepColumns() == ""
        # setters/getters
        transformer = transformer.setModels(descriptions).setKeepColumns("uid")
        assert transformer.getModels() == descriptions
        assert transformer.getKeepColumns() == "uid"
        assert not transformer.initialize()

    def test_transformer_transform(self, local_spark):
        src_df = self._load_src_df(local_spark)
        transformer = ApplyModelsTransformer(models="model_type->BaseLineDummy", keep_columns="uid")
        assert transformer.initialize()
        output = transformer.transform(src_df)
        assert output.toPandas().shape == (1, 6)

    @staticmethod
    def _load_src_df(spark):
        rdd = spark.sparkContext.parallelize([('a', {'tm1': 1.1}, {'vk1': 1.2}, {'ok1': 1.3}, [1.4])])

        return spark.createDataFrame(rdd, schema=StructType([
            StructField('uid', StringType()),
            StructField('topics_m', MapType(StringType(), FloatType())),
            StructField('vk_groups', MapType(StringType(), FloatType())),
            StructField('ok_groups', MapType(StringType(), FloatType())),
            StructField('all_profiles', ArrayType(FloatType()))
        ]))
