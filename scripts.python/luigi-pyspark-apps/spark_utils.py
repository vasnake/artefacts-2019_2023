import os
import re
import functools
import itertools as it

from pprint import pformat
from operator import itemgetter
from collections import Counter, OrderedDict, deque, defaultdict

import six
import numpy as np
import pandas as pd
import pyspark.sql.functions as sqlfn

from pyspark import TaskContext, AccumulatorParam
from pyspark.sql import Column, DataFrame, SQLContext
from pyspark.sql.types import MapType, ArrayType, StringType, BooleanType, IntegralType
from luigi.contrib.hdfs import create_hadoopcli_client
from pyspark.ml.wrapper import JavaWrapper
from luigi.contrib.hdfs.config import load_hadoop_cmd

from dmcore.utils.io import LearningData
from dmcore.utils.data import Dataset, MapField, ArrayField, PrimitiveField
from dmcore.utils.common import hstack

from .fs import HdfsClient, LocalFsClient
from .fs.hdfs import SUCCESS_FILENAME
from ..control.client.logs import ControlLoggingMixin

PYSPARK_NUMERIC_TYPES = {"boolean", "decimal", "double", "float", "byte", "integer", "long", "short"}
PYSPARK_PRIMITIVE_TYPES = PYSPARK_NUMERIC_TYPES.union({"null", "string", "date", "timestamp"})
PYSPARK_COLLECTION_TYPES = {"array", "map"}


class IntervalCheckpointService(ControlLoggingMixin):
    """Perform :class:`pyspark.sql.DataFrame` checkpoint for each `checkpoint_interval` operations."""

    def __init__(self, hdfs_base_dir, checkpoint_interval=15, log_url=None):
        self.hdfs_base_dir = hdfs_base_dir
        self.checkpoint_interval = checkpoint_interval
        self.log_url = log_url
        self._step = 0

    def checkpoint(self, df, force=False):
        self._step += 1

        if force or ((self.checkpoint_interval is not None) and (self._step % self.checkpoint_interval == 0)):
            checkpoint_dir = os.path.join(self.hdfs_base_dir, "checkpoint_{}".format(self._step))
            self.info("Perform checkpoint of {} to {} ...".format(df, checkpoint_dir))
            (
                df.write.mode("overwrite")
                .option("compression", "gzip")
                .option("mapreduce.fileoutputcommitter.algorithm.version", "2")
                .parquet(checkpoint_dir)
            )
            return df.sql_ctx.read.parquet(checkpoint_dir)

        return df


class CustomUDFLibrary(ControlLoggingMixin):
    """A single entry point to all available UDF/UDAF from a specified JAR."""

    # name -> alias
    CATALYST_UDF = {
        "generic_sum": "gsum",
        "generic_min": "gmin",
        "generic_max": "gmax",
        "generic_avg": "gavg",
        "generic_most_freq": "most_freq",
        "generic_coomul": "coomul",
        "generic_semidiff": "semidiff",
        "generic_semisum": "semisum",
        "generic_matmul": "matmul",
        "generic_isinf": "isinf",
        "generic_isfinite": "isfinite",
    }

    JAVA_UDF = {
        "map_values_ordered": "com.github.vasnake.spark.udf.collect.MapValuesOrderedUDF",
        "hash_to_uint32": "com.github.vasnake.spark.udf.export.HashToUINT32UDF",
        "is_uint32": "com.github.vasnake.spark.udf.export.CheckUINT32UDF",
        "map_join": "com.github.vasnake.spark.udf.export.MapJoinUDF",
        "uid2user": "com.github.vasnake.spark.udf.export.Uid2UserUDF",
        "uid64": "com.github.vasnake.spark.udf.export.Uid64UDF",
    }

    def __init__(self, spark, jar="hdfs:/lib/custom-transformers-assembly-SNAPSHOT.jar", log_url=None):
        self.spark = spark
        self.log_url = log_url

        if jar is None:
            self.warn("Jar is not given in init, so make sure that it is given upstream")
        elif not isinstance(jar, six.string_types) or not jar:
            raise ValueError("Jar must be a non-empty string, got `{}`".format(jar))
        else:
            if jar.startswith("hdfs:"):
                fs = HdfsClient()
            else:
                fs = LocalFsClient()

            if fs.exists(jar):
                self.info("Loading jar `{}` ...".format(jar))
                spark.sql("ADD JAR {}".format(jar))
                # TODO: `SQL add jar ...` and `spark._jsc.sc().addJar(jar)` won't work for ml.transformers and some Catalyst UDF use cases:
                #  problem: executors can't find lib classes (Classloader failure).
                #  We should generate spark-submit args for jar dependencies.
            else:
                raise ValueError("Jar `{}` doesn't exist.".format(jar))

    def register_all_udf(self):
        for name, new_name in six.iteritems(self.CATALYST_UDF):
            self.register_catalyst_udf(name, new_name)

        for name, _ in six.iteritems(self.JAVA_UDF):
            self.register_java_udf(name)

        return self

    def register_catalyst_udf(self, udf_name, new_name=None):
        if udf_name not in self.CATALYST_UDF:
            raise ValueError("Unknown catalyst UDF: `{}`".format(udf_name))

        new_name = new_name or self.CATALYST_UDF[udf_name]
        self.info("Register catalyst UDF: {} as {}".format(udf_name, new_name))
        # fmt: off
        self.spark._jvm.org.apache.spark.sql.catalyst.vasnake.udf.functions.registerAs(
            udf_name,
            new_name,
            self.spark._jsparkSession,
            True  # rewrite
        )
        # fmt: on
        return self

    def register_java_udf(self, udf_name, new_name=None):
        if udf_name not in self.JAVA_UDF:
            raise ValueError("Unknown java UDF: `{}`".format(udf_name))

        new_name = new_name or udf_name
        self.info("Register java UDF: {} as {}".format(udf_name, new_name))
        self.spark.udf.registerJavaFunction(new_name, self.JAVA_UDF[udf_name])
        return self


def insert_into_hive(
    df,
    database,
    table,
    max_rows_per_bucket,
    overwrite=True,
    raise_on_missing_columns=True,
    check_parameter=None,
    jar="hdfs:/lib/custom-transformers-assembly-SNAPSHOT.jar",
):
    """Write PySpark dataframe into an existing Hive table, with internal bucketing to support user control over average
    file size.

    Source dataframe is materialized (counting rows) and repartitioned before write
    to have (approximately) at most ``max_rows_per_bucket`` rows in each bucket (file).

    An exception is raised in case of:
        - Destination table doesn't exist.
        - `max_rows_per_bucket` is less than 1.
        - `check_parameter` is one of reserved words: "rawDataSize", "numFiles", "transient_lastDdlTime",
            "totalSize", "spark.sql.statistics.totalSize", "COLUMN_STATS_ACCURATE", "numRows".
        - Partition column is not of type `string` or missing from source dataframe.
        - Partition column contains the `NULL` value.
        - Non-partition columns are missing and `raise_on_missing_columns` is `True`.

    .. note::
        Column names comparison is case-sensitive, i.e. if dataframe has column `Foo` and Hive table has
        column `foo`, then corresponding dataframe column is considered to be missing.

        For `check_parameter` to take effect target table must be partitioned (PARTITIONED BY ...),
        otherwise `check_parameter` is ignored.

    :param df: source dataframe.
    :type df: :class:`pyspark.sql.DataFrame`
    :param str database: the destination Hive database name.
    :param str table: the destination Hive table name.
    :param int max_rows_per_bucket: a repartition parameter, maximum amount of rows per bucket (file).
    :param bool overwrite: specifies whether to overwrite existing data (only relevant partitions are overwritten).
    :param bool raise_on_missing_columns: raise AnalysisException if source dataframe doesn't contain all table columns.
        If the parameter is set to False, then missing columns are added and filled in with `NULL` value.
    :param check_parameter: a partition parameter name for metastore, value of that parameter is set to `"true"` on success.
        If `None` given (by default), then partitions parameters stay intact,
        otherwise metastore parameter is updated for all written partitions.
    :type check_parameter: typing.Union[str, type(None)]
    :param str jar: path to a jar file with procedure implementation.
    :return: None
    """
    df.sql_ctx.sql("ADD JAR {}".format(jar))
    writer = JavaWrapper._create_from_java_class("com.github.vasnake.hive.Writer")

    writer._java_obj.insertIntoHive(
        df._jdf,
        database,
        table,
        max_rows_per_bucket,
        overwrite,
        raise_on_missing_columns,
        check_parameter,
    )


def skewed_join(left, right, on, how="inner", salt_parts=10):
    """Perform PySpark dataframes join with salted key for skewed keys distribution.

    .. note:: Right dataframe is assumed to be much smaller than left dataframe, so ``right`` is repeated for each salt
        part. A scheme is ordinary: ``left`` join ``right``.

    :param left: left dataframe for join.
    :type left: :class:`pyspark.sql.DataFrame`
    :param right: right dataframe for join.
    :type right: :class:`pyspark.sql.DataFrame`
    :param on: join keys.
    :type on: typing.Union[
        typing.Union[str, :class:`pyspark.sql.Column`],
        typing.Union[list[str], list[:class:`pyspark.sql.Column`]
    ]
    :param str how: Spark SQL join type, allowed only {"inner", "left", "left_semi", "left_anti"} values.
    :param int salt_parts: amount of parts to split each join key into, 10 by default.
    :return: joined dataframe.
    :rtype: :class:`pyspark.sql.DataFrame`.
    """
    allowed_hows = {"inner", "left", "left_semi", "left_anti"}
    salt_column = "s" * (max(len(name) for name in left.columns + right.columns) + 1)

    if how not in allowed_hows:
        raise ValueError("Parameter `how` must be in {}, got '{}'".format(allowed_hows, how))

    left = left.withColumn(salt_column, sqlfn.monotonically_increasing_id() % salt_parts)
    right = right.withColumn(
        salt_column, sqlfn.explode(sqlfn.expr("array({})".format(",".join(map(str, range(salt_parts))))))
    )
    on = [on] if isinstance(on, (six.string_types, Column)) else list(on)

    normalized_on = []
    dup_columns = set()

    for c in on:
        if isinstance(c, six.string_types):
            normalized_on.append(left[c] == right[c])
            dup_columns.add(c)
        elif isinstance(c, Column):
            normalized_on.append(c)
        else:
            raise TypeError(
                "Parameter `on` should consist of only strings or pyspark.sql.Column, got {}".format(type(c))
            )

    joined = left.join(right, on=normalized_on + [left[salt_column] == right[salt_column]], how=how).drop(salt_column)

    new_columns = []
    columns_map = defaultdict(list)

    for i, name in enumerate(joined.columns):
        new_name = "col_{}".format(i)
        columns_map[name].append(new_name)
        new_columns.append(new_name)

    column_exprs = []

    for name, new_names in columns_map.items():
        if name in dup_columns:
            column_exprs.append("coalesce({}) as {}".format(",".join(new_names), name))
        else:
            column_exprs.extend(["{} as {}".format(new_name, name) for new_name in new_names])

    return joined.toDF(*new_columns).selectExpr(*column_exprs)


def configured_join(left, right, **conf):
    """Perform PySpark dataframe join according to conf parameters.

    :param left: left dataframe for join.
    :type left: :class:`pyspark.sql.DataFrame`
    :param right: right dataframe for join.
    :type right: :class:`pyspark.sql.DataFrame`
    :param conf: join configuration parameters - `on`, `how`, `type`, `salt_parts`.
        The last parameter is applicable only if `type` is set to "skewed", see ``skewed_join`` function.
        Other `type` options are: "regular" (default) and "broadcast".
        Parameter `on` is mandatory, its value is a single string or an iterable of strings - join columns names.
            Alternatively, :class:`pyspark.sql.Column` objects could be used instead of string values.
        Parameter `how` is optional with default value "inner".
        For "skewed" type of join only {"inner", "left", "left_semi", "left_anti"} values are acceptable.
    :return: joined dataframe.
    :rtype: :class:`pyspark.sql.DataFrame`
    """
    on = [conf["on"]] if isinstance(conf["on"], (six.string_types, Column)) else list(conf["on"])

    join_type = conf.get("type", "regular")
    how = conf.get("how", "inner")

    if join_type == "regular":
        return left.join(right, on, how)

    elif join_type == "broadcast":
        return left.join(sqlfn.broadcast(right.coalesce(1)), on, how)

    elif join_type == "skewed":
        return skewed_join(left=left, right=right, on=on, how=how, **{k: conf[k] for k in ["salt_parts"] if k in conf})

    else:
        raise ValueError("Invalid join type '{}' in conf {}.".format(join_type, pformat(conf)))


def read_orc_table(what, partition_filter_expr, spark, jar="hdfs:/lib/custom-transformers-SNAPSHOT.jar"):
    """Read Hive table stored in ORC format as a union of its partition paths.

    Increases data loading speed from tables with a large number of files
    by parallel listing leaf files and directories.

    .. note:: This method is a wrapper for a corresponding Scala implementation.

    :param str what: dot-separated database and table name.
    :param str partition_filter_expr: Spark SQL valid filter expression.
        Must contain ONLY table partitioning columns.
    :param spark: an instance of spark session.
    :type spark: :class:`pyspark.sql.SparkSession`
    :param str jar: a path to JAR containing related Scala implementation. It is intentionally defined by default.
    :return: result table as a PySpark dataframe.
    :rtype: :class:`pyspark.sql.DataFrame`
    """
    spark.sql("ADD JAR {}".format(jar))

    return DataFrame(
        JavaWrapper._new_java_obj(
            "com.github.vasnake.spark.io.hive.TableSmartReader.readTableAsUnionOrcFiles",
            what,
            sqlfn.expr(partition_filter_expr)._jc,
            spark._jsparkSession,
        ),
        SQLContext(spark.sparkContext),
    )


def join_filter(df, filter_dicts, exclusion=False, broadcast_max_size=250000, salt_parts=100):
    """Filter input dataframe with a list of rows, using join with synthetic DF and optional filter predicates.

    Selected Join type depends on filter size (rows_count * columns_count) and a given parameters.
    For a small filter the tool uses broadcast join; for a big filter there is a choice: skewed or regular join.
    This choice is managed by the `salt_parts` parameter.

    Two modes of filters are supported: inclusion or exclusion.

    .. note:: There are no restrictions for filter columns data type but filter data are transformed to strings before
    executing actual filtering. Transformation implemented using Spark cast mechanics.
    Dataframe columns used in filter also cast to string in order to perform equality check.
    Actually it make no sense to use types other than (StringType, IntegralType, BooleanType), and dataframe
    columns checked against these types during parameters validation phase.

    .. note:: `NULL` value can't be expressed in `filter_dicts` in any other way but using python `None` value.
    For other values that restriction is inapplicable.
    For example, you may express int value (e.g. 42) as string `"42"` or int 42.

    :param df: input dataframe, must have columns enumerated in `filter_dicts`.
    :type df: :class:`pyspark.sql.DataFrame`
    :param filter_dicts: a list of rows where each row is a dict that represents a mapping from column name to column
        value. Values equality are null-safe, see `<=>` operator in Spark SQL. Row can't be empty, each row must have
        the same set of keys.
        Empty list means 'empty filter' and, depending on `exclusion` parameter, function produces an empty dataframe as
        inclusion of input with empty filter; or un-filtered dataframe, as exclusion of empty filter from input.
    :type filter_dicts: list[dict[str, Any]]
    :param bool exclusion: one of the two modes of filtering: inclusion or exclusion.
        If `False`, then filter selects rows from input where values are equal to values of any `filter_dicts` element.
        Otherwise, filter condition is negated.
    :param int broadcast_max_size: amount of elements in filter (rows_count * columns_count) as a filter size limit.
        Filter under that limit uses broadcast join. Filter above that limit uses skewed or regular join where parameter
        `salt_parts` makes sense.
    :param int salt_parts: amount of join key subpartitions. Used only with skewed join when size of a filter exceeds
        `broadcast_max_size` limit. Values less-or-equal `1` works as a flag that forces regular join.
    :return: filtered dataframe.
    :rtype: :class:`pyspark.sql.DataFrame`
    """
    if filter_dicts:
        rows_keys = [set(row.keys()) for row in filter_dicts]
        filter_keys = set.intersection(*rows_keys)

        if filter_keys != set.union(*rows_keys):
            raise ValueError("Invalid `filter_dicts` value, rows must have identical structure")
        if not filter_keys:
            raise ValueError("Invalid `filter_dicts` value, each row must have at least one item")
        if not filter_keys.issubset(df.columns):
            raise ValueError("Invalid `filter_dicts` value, row keys must be a subset of `df.columns`")
        for col_name in filter_keys:
            if not isinstance(df.schema[col_name].dataType, (StringType, IntegralType, BooleanType)):
                raise ValueError(
                    "Invalid filter keys, df.schema[key].dataType must be one of "
                    "(StringType, IntegralType, BooleanType)"
                )

    else:
        if exclusion:
            return df
        else:
            return df.where("false")

    filter_pdf = pd.DataFrame(filter_dicts, dtype=object)

    # Filter constants with SQL expression

    count_unique = filter_pdf.nunique(dropna=False)

    constant_columns = {
        c: filter_pdf[c].tolist().pop() for c in filter_keys if count_unique[c] == 1
    }  # col_name -> constant_value

    if constant_columns and (not exclusion or len(constant_columns) == len(filter_keys)):
        conditions = [df[k].cast("string").eqNullSafe(sqlfn.lit(v).cast("string")) for k, v in constant_columns.items()]
        filter_expr = functools.reduce(Column.__and__, conditions)

        if exclusion:
            filter_expr = ~filter_expr

        df = df.where(filter_expr)
        filter_pdf = filter_pdf.drop(constant_columns.keys(), axis=1)

        if len(constant_columns) == len(filter_keys):
            return df

    # Filter the rest with join

    filter_columns = [str(c) for c in filter_pdf.columns]
    # N.B. str(c) to fix problem with `StructType keys should be strings` sql/types bug

    filter_df = df.sql_ctx.createDataFrame(
        data=filter_pdf.drop_duplicates(),
        schema=",".join(["{}:string".format(c) for c in filter_columns]),
    )

    if filter_pdf.size <= broadcast_max_size:
        join_type = "broadcast"
    elif salt_parts <= 1:
        join_type = "regular"
    else:
        join_type = "skewed"

    return configured_join(
        left=df,
        right=filter_df,
        on=functools.reduce(Column.__and__, [df[c].cast("string").eqNullSafe(filter_df[c]) for c in filter_df.columns]),
        how="left_anti" if exclusion else "left_semi",
        type=join_type,
        salt_parts=salt_parts,
    ).select(
        df.columns
    )  # Restore columns order
