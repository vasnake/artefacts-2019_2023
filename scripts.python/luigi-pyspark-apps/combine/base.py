import abc

import luigi
import pyspark.sql.functions as sqlfn

from pyspark.sql.types import MapType, ArrayType, StringType, StructType, IntegralType, FractionalType
from pyspark.sql.utils import CapturedException

from prj.apps.utils.common import METASTORE_PARTITION_SUCCESS, add_days
from prj.apps.utils.common.hive import format_table, select_clause
from prj.apps.utils.common.spark import insert_into_hive
from prj.apps.utils.common.luigix import HiveGenericTarget
from prj.apps.utils.control.luigix.task import ControlApp, ControlETLPySparkTask
from prj.apps.utils.control.client.status import MissingDepsStatusException

from .feature_transformer import (
    MapFeatureTransformer,
    FloatFeatureTransformer,
    ArrayFloatFeatureTransformer,
    ArrayStringFeatureTransformer,
)


class CombineFeaturesBaseTask(ControlETLPySparkTask):
    """ABC for combine features tasks with Hive source and target.

    Method `extract_transform` is implemented with before and after hooks: aggregate-and-combine a feature before
    validation and filtering; select top rows after.
    """

    feature_name = luigi.Parameter(description="Feature name")  # type: str
    target_dt = luigi.Parameter(description="Target dt")  # type: str

    target_db = luigi.Parameter(description="Hive target database")  # type: str
    target_table = luigi.Parameter(description="Hive target table")  # type: str

    source_db = luigi.Parameter(description="Hive source database")  # type: str
    source_table = luigi.Parameter(description="Hive source table")  # type: str
    source_partitions = luigi.ListParameter(description="Selected partitions list")  # type: list

    feature_hashing = luigi.BoolParameter(description="Transformation to uint32 flag", default=False)  # type: bool
    max_collection_size = luigi.IntParameter(description="Maximum number of items in feature", default=100)  # type: int

    output_max_rows_per_bucket = luigi.IntParameter(
        description="Approximate maximum rows amount per bucket within each output Hive partition", default=1000000
    )  # type: int

    def output(self):
        return HiveGenericTarget(
            database=self.target_db,
            table=self.target_table,
            partitions={"feature_name": self.feature_name, "dt": self.target_dt},
            check_parameter=METASTORE_PARTITION_SUCCESS,
        )

    def extract_transform(self, sql_ctx):
        # Used as output for invalid input
        _empty_df = sql_ctx.createDataFrame([], StructType([]))

        self.info(
            "Loading source table: {}".format(format_table(self.source_db, self.source_table, self.source_partitions))
        )
        df = sql_ctx.sql(
            select_clause(database=self.source_db, table=self.source_table, partition_dicts=self.source_partitions)
        )

        self.info("Aggregating and combining a feature ...")

        try:
            df = self.aggregate_combine(df)
        except CapturedException as e:
            self.warn("SQL exception: {}\n{}".format(e.desc, e.stackTrace))
            return _empty_df

        self.info("Validating feature data type ...")

        feature_type = df.schema["feature"].dataType

        if isinstance(feature_type, FractionalType):
            transformer = FloatFeatureTransformer()  # num

        elif isinstance(feature_type, ArrayType) and isinstance(feature_type.elementType, FractionalType):
            transformer = ArrayFloatFeatureTransformer()  # num

        elif (
            isinstance(feature_type, MapType)
            and isinstance(feature_type.keyType, (StringType, IntegralType))
            and isinstance(feature_type.valueType, (IntegralType, FractionalType))
        ):
            transformer = MapFeatureTransformer()  # num

        elif isinstance(feature_type, (StringType, IntegralType)) or (
            isinstance(feature_type, ArrayType) and isinstance(feature_type.elementType, (StringType, IntegralType))
        ):
            transformer = ArrayStringFeatureTransformer()  # cat

        else:
            self.warn("Unsupported feature data type `{}`".format(feature_type.simpleString()))
            return _empty_df

        self.info("Filtering and transforming a feature ...")

        transformer.log_url = self.log_url
        df = transformer.transform(df, self.max_collection_size, self.feature_hashing)

        return self.limit_rows(df)

    @abc.abstractmethod
    def aggregate_combine(self, df):
        """Perform Extract and Transform steps before feature type validation and filtering.

        :param df: source partitions dataframe.
        :type df: :class:`pyspark.sql.DataFrame`
        :return: transformed dataframe with columns [uid, uid_type, optional bu_link_id, feature].
        :rtype: :class:`pyspark.sql.DataFrame`
        """
        return NotImplementedError

    def limit_rows(self, df):
        return df

    def load(self, df):
        self.info("Writing results into temporary directory: {}".format(self.tmp_hdfs_dir))

        df.write.option("mapreduce.fileoutputcommitter.algorithm.version", "2").parquet(
            self.tmp_hdfs_dir, mode="overwrite"
        )

        df.unpersist()

        self.info("Saving to Hive into {}.{} ...".format(self.target_db, self.target_table))

        insert_into_hive(
            df=(
                df.sql_ctx.read.parquet(self.tmp_hdfs_dir)
                .withColumn("feature_name", sqlfn.lit(self.feature_name))
                .withColumn("dt", sqlfn.lit(self.target_dt))
                .persist()
            ),
            database=self.target_db,
            table=self.target_table,
            max_rows_per_bucket=self.output_max_rows_per_bucket,
            overwrite=True,
            raise_on_missing_columns=False,
            check_parameter=METASTORE_PARTITION_SUCCESS,
        )


class CombineFeaturesBaseApp(ControlApp):
    """ABC for combine features apps, implements a search for source partitions."""

    def prepare_config(self, config):
        max_dt = config["target_dt"]

        database = config["source_db"]
        table = config["source_table"]
        period = config.pop("period", 1)
        dt_selection_mode = config.pop("dt_selection_mode", "single_last")

        source_partitions = self.partitions_finder.find(
            database=database,
            table=table,
            partition_conf=config.pop("source_partition_conf"),
            min_dt=add_days(max_dt, -(period - 1)),
            max_dt=max_dt,
            agg_mode="max" if dt_selection_mode == "single_last" else "list",
        )

        if not self.partitions_finder.is_valid_dts(source_partitions, max_dt, period, dt_selection_mode):
            raise MissingDepsStatusException(
                "Not found partitions in Hive table {}.{} "
                "for period={} days from {} with mode '{}'".format(database, table, period, max_dt, dt_selection_mode)
            )

        config["source_partitions"] = source_partitions
        return config
