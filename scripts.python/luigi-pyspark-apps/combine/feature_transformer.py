"""Feature columns transformation using template pattern."""

import abc

import pyspark.sql.functions as sqlfn

from pyspark.sql.types import ArrayType

from prj.apps.utils.control.client.logs import ControlLoggingMixin


class BaseFeatureTransformer(ControlLoggingMixin):
    """Basic feature columns transformation logic."""

    SOURCE_NAME = "feature"
    TARGET_NAME = NotImplemented
    VALID_ROWS_FILTER_EXPR = NotImplemented

    TARGET_SCHEMA = [
        ("score", "float"),
        ("score_list", "array<float>"),
        ("score_map", "map<string,float>"),
        ("cat_list", "array<string>"),
    ]

    def __init__(self, log_url=None):
        self.log_url = log_url

    def transform(self, df, max_collection_size, feature_hashing):
        """Transform one input feature column into a standard 4-column features schema.

        :param df: an input dataframe with columns [uid, uid_type, optional bu_link_id, feature].
        :type df: :class:`pyspark.sql.DataFrame`
        :param int max_collection_size: a maximum size of array or map feature columns.
            Over-sized collections are properly filtered.
        :param bool feature_hashing: whether make map keys or array categorical feature uint32-hashing.
            If ``False``, then filter out rows having not-uint32 values.
        :return: transformed dataframe with columns
            [uid, uid_type, optional bu_link_id, score, score_list, score_map, cat_list].
        :rtype: :class:`pyspark.sql.DataFrame`
        """
        df = self.cast(df)
        df = self.filter_valid_rows(df)

        if feature_hashing:
            df = self.convert_to_uint32(df)
        else:
            df = self.filter_uint32(df)

        df = self.slice_feature(df, max_collection_size)

        return df.selectExpr(
            *list({"uid", "uid_type", "bu_link_id"}.intersection(df.columns))
            + [
                "{} as {}".format(self.SOURCE_NAME if nm == self.TARGET_NAME else "cast(null as {})".format(tp), nm)
                for nm, tp in self.TARGET_SCHEMA
            ]
        )

    def cast(self, df):
        feature_type = dict(self.TARGET_SCHEMA)[self.TARGET_NAME]
        return df.withColumn(self.SOURCE_NAME, sqlfn.col(self.SOURCE_NAME).cast(feature_type))

    def filter_valid_rows(self, df):
        self.info("Filtering out invalid rows for `{}` feature ...".format(dict(self.TARGET_SCHEMA)[self.TARGET_NAME]))
        return df.dropna().where(self.VALID_ROWS_FILTER_EXPR.format(column=self.SOURCE_NAME))

    @abc.abstractmethod
    def slice_feature(self, df, max_collection_size):
        raise NotImplementedError

    @abc.abstractmethod
    def convert_to_uint32(self, df):
        raise NotImplementedError

    @abc.abstractmethod
    def filter_uint32(self, df):
        raise NotImplementedError


class FloatFeatureTransformer(BaseFeatureTransformer):
    TARGET_NAME = "score"
    VALID_ROWS_FILTER_EXPR = "isfinite({column})"

    def slice_feature(self, df, max_collection_size):
        return df

    def convert_to_uint32(self, df):
        return df

    def filter_uint32(self, df):
        return df


class CollectionFeatureTransformer(BaseFeatureTransformer):

    def slice_feature(self, df, max_collection_size):
        self.info("Selecting first-{} array elements ...".format(max_collection_size))

        return df.withColumn(
            self.SOURCE_NAME,
            sqlfn.when(
                sqlfn.expr("size({}) > {}".format(self.SOURCE_NAME, max_collection_size)),
                sqlfn.expr("slice({}, 1, {})".format(self.SOURCE_NAME, max_collection_size)),
            ).otherwise(sqlfn.col(self.SOURCE_NAME)),
        )


class ArrayFloatFeatureTransformer(CollectionFeatureTransformer):
    TARGET_NAME = "score_list"
    VALID_ROWS_FILTER_EXPR = "size({column}) > 0 and not exists({column}, _x -> not isfinite(_x))"

    def convert_to_uint32(self, df):
        return df

    def filter_uint32(self, df):
        return df


class ArrayStringFeatureTransformer(CollectionFeatureTransformer):
    TARGET_NAME = "cat_list"
    VALID_ROWS_FILTER_EXPR = "size({column}) > 0 and not exists({column}, _x -> isnull(_x))"

    def cast(self, df):
        if not isinstance(df.schema[self.SOURCE_NAME].dataType, ArrayType):
            df = df.withColumn(self.SOURCE_NAME, sqlfn.array(sqlfn.col(self.SOURCE_NAME)))

        return super(ArrayStringFeatureTransformer, self).cast(df)

    def convert_to_uint32(self, df):
        self.info("Hashing array values ...")

        return df.withColumn(
            self.SOURCE_NAME,
            sqlfn.expr("transform({}, _x -> hash_to_uint32(_x))".format(self.SOURCE_NAME)),
        )

    def filter_uint32(self, df):
        self.info("Filtering out rows with not-uint32 array values ...")
        return df.where("not exists({}, _x -> not is_uint32(_x))".format(self.SOURCE_NAME))


class MapFeatureTransformer(CollectionFeatureTransformer):
    TARGET_NAME = "score_map"

    def filter_valid_rows(self, df):
        self.info("Filtering out rows with invalid map values ...")

        filter_map = """
            map_from_entries(
                filter(
                    user_dmdesc.map_key_values({column}),
                    _x -> isfinite(_x['value'])
                )
            )
        """.format(
            column=self.SOURCE_NAME
        )

        return (
            df.dropna()
            .withColumn(self.SOURCE_NAME, sqlfn.expr(filter_map))
            .where("size({column}) > 0".format(column=self.SOURCE_NAME))
        )

    def slice_feature(self, df, max_collection_size):
        self.info("Selecting top-{} map elements by values ...".format(max_collection_size))

        slice_expr = """
            slice(
                array_sort(
                    transform(
                        user_dmdesc.map_key_values({column}),
                        _x -> (_x['value'], _x['key'])
                    )
                ),
                -{num}, {num}
            )
        """.format(
            column=self.SOURCE_NAME, num=max_collection_size
        )

        return df.withColumn(
            self.SOURCE_NAME,
            sqlfn.when(
                sqlfn.expr("size({}) > {}".format(self.SOURCE_NAME, max_collection_size)),
                sqlfn.map_from_entries(sqlfn.expr("transform({}, _x -> (_x['col2'], _x['col1']))".format(slice_expr))),
            ).otherwise(sqlfn.col(self.SOURCE_NAME)),
        )

    def convert_to_uint32(self, df):
        self.info("Hashing map keys ...")

        list_expr = """
            transform(
                user_dmdesc.map_key_values({column}),
                _x -> (hash_to_uint32(_x['key']), _x['value'])
            )
        """.format(
            column=self.SOURCE_NAME
        )

        return df.withColumn(self.SOURCE_NAME, sqlfn.map_from_entries(sqlfn.expr(list_expr)))

    def filter_uint32(self, df):
        self.info("Filtering out rows with not-uint32 map keys ...")

        filter_map = """
            map_from_entries(
                filter(
                    user_dmdesc.map_key_values({column}),
                    _x -> is_uint32(_x['key'])
                )
            )
        """.format(
            column=self.SOURCE_NAME
        )

        return df.withColumn(self.SOURCE_NAME, sqlfn.expr(filter_map)).where(
            "size({column}) > 0".format(column=self.SOURCE_NAME)
        )
