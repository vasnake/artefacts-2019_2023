import os
import traceback

from pprint import pformat

import six
import luigi
import pyspark.sql.functions as sqlfn

from pyspark.sql import SQLContext
from pyspark.sql.types import MapType, ArrayType, AtomicType

from prj.adapters.utils import deconstruct
from prj.apps.utils.common import (
    METASTORE_PARTITION_SUCCESS,
    add_days,
    hive_from_schema,
    schema_from_hive,
    unfreeze_json_param,
)
from prj.apps.utils.common.hive import format_table, select_clause, create_hive_table_ddl, partition_filter_expr
from prj.apps.utils.common.spark import (
    CustomUDFLibrary,
    IntervalCheckpointService,
    read_orc_table,
    configured_join,
    insert_into_hive,
)
from prj.apps.utils.common.luigix import HiveExternalTask, HiveGenericTarget
from prj.apps.utils.control.luigix.task import ControlApp, BasePySparkTask, ControlTaskMeta
from prj.apps.utils.control.client.exception import (
    FatalStatusException,
    TerminalStatusException,
    MissingDepsStatusException,
)

# from .test import show_df  # TODO: remove after debug
from .join_rule import JoinRuleEvaluator


class JoinerFeaturesTask(six.with_metaclass(ControlTaskMeta, luigi.WrapperTask)):

    target_dt = luigi.Parameter(description="Target dt")  # type: str
    target_db = luigi.Parameter(description="Hive target database")  # type: str
    target_table = luigi.Parameter(description="Hive target table")  # type: str

    uid_types = luigi.ListParameter(description="Target uid_type partitions")  # type: list
    feature_sources = luigi.DictParameter(description="Named features sources")  # type: dict
    join_rule = luigi.Parameter(description="Sources join rule")  # type: str
    domains = luigi.ListParameter(description="Features domains")  # type: list
    final_columns = luigi.ListParameter(description="Final select expressions")  # type: list

    strict_check_array = luigi.BoolParameter(description="Some checks enable/disable switch")  # type: bool
    min_target_rows = luigi.IntParameter(description="Acceptable minimum of output rows")  # type: int

    shuffle_partitions = luigi.IntParameter(description="spark.sql.shuffle.partitions")  # type: int
    checkpoint_interval = luigi.IntParameter(description="Number of joins before checkpoint")  # type: int
    executor_memory_gb = luigi.IntParameter(description="spark-submit `--executor-memory` parameter, GB")  # type: int

    output_max_rows_per_bucket = luigi.IntParameter(
        description="Approximate maximum rows amount per bucket within each output Hive partition"
    )  # type: int

    def requires(self):
        return [
            JoinerFeaturesSubTask(
                force=self.force,
                target_dt=self.target_dt,
                target_db=self.target_db,
                target_table=self.target_table,
                uid_type_conf=uid_type_conf,
                feature_sources=self.feature_sources,
                join_rule=self.join_rule,
                domains=self.domains,
                final_columns=self.final_columns,
                strict_check_array=self.strict_check_array,
                min_target_rows=uid_type_conf.get("min_target_rows", self.min_target_rows),
                shuffle_partitions=uid_type_conf.get("shuffle_partitions", self.shuffle_partitions),
                checkpoint_interval=uid_type_conf.get("checkpoint_interval", self.checkpoint_interval),
                executor_memory="{}G".format(uid_type_conf.get("executor_memory_gb", self.executor_memory_gb)),
                output_max_rows_per_bucket=uid_type_conf.get(
                    "output_max_rows_per_bucket", self.output_max_rows_per_bucket
                ),
            )
            for uid_type_conf in self.uid_types
        ]


class JoinerFeaturesSubTask(BasePySparkTask):
    ALLOWED_DB_PREFIXES = {"snb_", "dmPrj_"}

    target_dt = luigi.Parameter(description="Target dt")  # type: str
    target_db = luigi.Parameter(description="Hive target database")  # type: str
    target_table = luigi.Parameter(description="Hive target table")  # type: str

    uid_type_conf = luigi.DictParameter(description="Target uid_type partition")  # type: dict
    feature_sources = luigi.DictParameter(description="Named features sources")  # type: dict
    join_rule = luigi.Parameter(description="Sources join rule")  # type: str
    domains = luigi.ListParameter(description="Features domains")  # type: list
    final_columns = luigi.ListParameter(description="Final select expressions")  # type: list
    strict_check_array = luigi.BoolParameter(description="Some checks enable/disable switch")  # type: bool

    min_target_rows = luigi.IntParameter(description="Acceptable minimum of output rows")  # type: int
    shuffle_partitions = luigi.IntParameter(description="spark.sql.shuffle.partitions")  # type: int
    checkpoint_interval = luigi.IntParameter(description="Number of joins before checkpoint")  # type: int
    executor_memory = luigi.IntParameter(description="spark-submit `--executor-memory` parameter")  # type: str

    output_max_rows_per_bucket = luigi.IntParameter(
        description="Approximate maximum rows amount per bucket within each output Hive partition"
    )  # type: int

    retry_count = 0

    @property
    def name(self):
        return "Prj__{}__{}__{}__{}__{}".format(
            self.__class__.__name__,
            self.target_db,
            self.target_table,
            self.target_dt,
            self.uid_type_conf["output"],
        )

    def requires(self):
        matching = self.uid_type_conf.get("matching")
        if matching:
            yield HiveExternalTask(
                database=matching["db"],
                table=matching["table"],
                partitions=matching["partitions"],
            )

        for _, source_conf in six.iteritems(self.feature_sources):
            yield HiveExternalTask(
                database=source_conf["db"],
                table=source_conf["table"],
                partitions=source_conf["partitions"],
            )

            for md_source_conf in source_conf.get("md_sources", []):
                yield HiveExternalTask(
                    database=md_source_conf["db"],
                    table=md_source_conf["table"],
                    partitions=md_source_conf["partitions"],
                )

    def output(self):
        return HiveGenericTarget(
            database=self.target_db,
            table=self.target_table,
            partitions={"dt": self.target_dt, "uid_type": self.uid_type_conf["output"]},
            check_parameter=METASTORE_PARTITION_SUCCESS,
        )

    @property
    def conf(self):
        conf = super(JoinerFeaturesSubTask, self).conf
        conf["spark.sql.shuffle.partitions"] = self.shuffle_partitions
        conf["spark.sql.legacy.sizeOfNull"] = "false"
        return conf

    def main(self, sc, *args):
        try:
            sql_ctx = SQLContext(sc)
            CustomUDFLibrary(sql_ctx.sparkSession).register_all_udf()

            sources = self.prepare_sources(sql_ctx)  # type: dict
            containers = self.build_domains(sources)  # type: dict
            result_df = self.join_domains_containers(containers)

            try:
                if self.final_columns:
                    self.info("Select final_columns: {}".format(pformat(self.final_columns)))
                    result_df = result_df.select(
                        *([sqlfn.col("uid")] + [sqlfn.expr(expr) for expr in self.final_columns])
                    )
            except Exception:
                raise FatalStatusException(traceback.format_exc())

            persisted_df = self.save_to_staging(result_df)

            if not self.check(persisted_df, sources):
                raise FatalStatusException("Invalid task output")

            self.write(persisted_df)

        except BaseException as task_error:
            self.report_runs_exception(task_error)
            raise

    def prepare_sources(self, sql_ctx):
        matching_table_df = None
        matching = self.uid_type_conf.get("matching")
        if matching:
            self.info(
                "Loading matching table: {}".format(
                    format_table(matching["db"], matching["table"], matching["partitions"])
                )
            )

            matching_table_df = sql_ctx.sql(
                select_clause(
                    database=matching["db"],
                    table=matching["table"],
                    partition_dicts=matching["partitions"],
                )
            )

            try:
                matching_table_df = matching_table_df.select("uid1", "uid2", "uid1_type")
            except Exception:
                raise FatalStatusException(traceback.format_exc())

        loaded_sources = {}

        for domain in self.domains:
            source_name = domain["source"]
            if source_name not in loaded_sources:
                loaded_sources[source_name] = self._prepare_source(source_name, matching_table_df, sql_ctx)

        return loaded_sources

    def _prepare_source(self, source_name, matching_table_df, sql_ctx):
        source_conf = unfreeze_json_param(self.feature_sources[source_name])
        self.debug("Preparing source `{}`:\n{}".format(source_name, pformat(source_conf)))

        self.info(
            "Loading source `{}` table: {}".format(
                source_name,
                format_table(source_conf["db"], source_conf["table"], source_conf["partitions"]),
            )
        )

        if source_conf.get("read_as_big_orc"):
            df = read_orc_table(
                what="{}.{}".format(source_conf["db"], source_conf["table"]),
                partition_filter_expr=partition_filter_expr(source_conf["partitions"]),
                spark=sql_ctx.sparkSession,
            )
        else:
            df = sql_ctx.sql(
                select_clause(
                    database=source_conf["db"],
                    table=source_conf["table"],
                    partition_dicts=source_conf["partitions"],
                )
            )

        for md_source_conf in source_conf.get("md_sources", []):
            md_df = self._prepare_md_source(md_source_conf, sql_ctx)
            try:
                df = configured_join(df, md_df, **md_source_conf["join_conf"])
            except Exception:
                raise FatalStatusException(traceback.format_exc())

        uids_expr = "map({})".format(
            ", ".join(
                [
                    "{}, {}".format(uid_type_expr, uid_expr)
                    for uid_type_expr, uid_expr in six.iteritems(source_conf.get("uids", {"uid_type": "uid"}))
                ]
            )
        )

        try:
            df = df.where(source_conf.get("where", "true"))

            df = df.select(
                *(
                    [df[name] for name in df.schema.names if name not in {"uid_type", "uid"}]
                    + [sqlfn.explode(sqlfn.expr(uids_expr)).alias("uid_type", "uid")]
                )
            )

            df = df.where(
                df["uid_type"].isNotNull() & df["uid"].isNotNull() & df["uid_type"].isin(*self.uid_type_conf["input"])
            )
        except Exception:
            raise FatalStatusException(traceback.format_exc())

        agg_keys = [df["uid_type"], df["uid"]]

        if matching_table_df is not None:
            self.info(
                "Mapping source `{}` uid values from `{}` to `{}`".format(
                    source_name, self.uid_type_conf["input"], self.uid_type_conf["output"]
                )
            )

            try:
                df = (
                    configured_join(
                        df,
                        matching_table_df,
                        on=(df["uid_type"] == matching_table_df["uid1_type"])
                        & (df["uid"] == matching_table_df["uid1"]),
                    )
                    .drop("uid", "uid1", "uid_type", "uid1_type")
                    .withColumnRenamed("uid2", "uid")
                )
            except Exception:
                raise FatalStatusException(traceback.format_exc())

            agg_keys = [df["uid"]]

        features = [
            sqlfn.expr(expr).alias(name) for feature in source_conf["features"] for name, expr in six.iteritems(feature)
        ]

        try:
            if source_conf.get("agg"):
                df = df.groupBy(*agg_keys).agg(*features)
            else:
                df = df.select(*(agg_keys + features))

            source_df = df.drop("uid_type")
        except Exception:
            raise FatalStatusException(traceback.format_exc())

        if source_conf.get("checkpoint"):
            self.info("Checkpoint for prepared source `{}`".format(source_name))
            source_df = IntervalCheckpointService(
                base_dir=os.path.join(self.tmp_hdfs_dir, source_name),
                log_url=self.log_url,
            ).checkpoint(source_df, force=True)

        source_conf["df"] = source_df

        return source_conf

    def _prepare_md_source(self, md_source_conf, sql_ctx):
        self.info(
            "Loading md_source table: {}".format(
                format_table(md_source_conf["db"], md_source_conf["table"], md_source_conf["partitions"])
            )
        )

        md_df = sql_ctx.sql(
            select_clause(
                database=md_source_conf["db"],
                table=md_source_conf["table"],
                partition_dicts=md_source_conf["partitions"],
            )
        )

        try:
            md_df = (
                md_df.where(md_source_conf.get("where", "true"))
                .selectExpr(*["{} as {}".format(v, k) for k, v in six.iteritems(md_source_conf["select"])])
                .distinct()
            )
        except Exception:
            raise FatalStatusException(traceback.format_exc())

        return md_df

    def build_domains(self, sources):
        self.info("Transforming sources to domains containers, sources:\n{}".format(pformat(sources)))

        def _normalize_domain_conf(domain_conf, source_features):
            conf = unfreeze_json_param(domain_conf)
            conf["columns"] = [str(col_name) for col_name in conf.get("columns", source_features)]
            return conf

        # For each source: for each domain: add domain columns
        for source_name, source_conf in six.iteritems(sources):
            df = source_conf["df"]
            source_features = [col_name for col_name in df.columns if col_name != "uid"]

            source_domains = [
                _normalize_domain_conf(domain_conf, source_features)
                for domain_conf in self.domains
                if domain_conf["source"] == source_name
            ]

            array_columns_stats = self._source_arrays_stats(source_conf, source_domains)

            if self.strict_check_array:
                for array_col_name, array_stats in six.iteritems(array_columns_stats):
                    if array_stats["max_size"] != array_stats["min_size"]:
                        raise FatalStatusException(
                            "Strict array check: invalid size of source features of array type, "
                            "feature: `{}`, source:\n{},\nsize stats:\n{}".format(
                                array_col_name, pformat(source_conf), pformat(array_columns_stats)
                            )
                        )

            # Simplify stats, only max_size is needed downstream
            array_columns_stats = {
                col_name: col_stats["max_size"] for col_name, col_stats in six.iteritems(array_columns_stats)
            }

            result_cols = [sqlfn.col("uid")]
            try:
                for domain_conf in source_domains:
                    # N.B. Prefix domain produces more than one column
                    df = self._add_domain(domain_conf, df, array_columns_stats)
                    result_cols += [sqlfn.col(col_name) for col_name in domain_conf["domain_columns_names"]]

                source_conf["df"] = df.select(*result_cols)
            except Exception as ex:
                if isinstance(ex, TerminalStatusException):
                    raise
                raise FatalStatusException(traceback.format_exc())

        return sources

    def _source_arrays_stats(self, source_conf, source_domains):
        if any("array" in domain_conf["type"].lower() for domain_conf in source_domains):
            stats_expressions = [
                "map('max_size', max(size({col_name})), 'min_size', min(size({col_name}))) as {col_name}".format(
                    col_name=column.name
                )
                for column in source_conf["df"].schema.fields
                if isinstance(column.dataType, ArrayType)
            ]
            # N.B. The function returns null for null input if `spark.sql.legacy.sizeOfNull` is set to false or
            # `spark.sql.ansi.enabled` is set to true.
            # Otherwise, the function returns -1 for null input.
            # We're expecting null values for null input.

            if stats_expressions:
                source_conf["df"] = source_conf["df"].persist()
                row = source_conf["df"].selectExpr(*stats_expressions).head()
                self.debug("Statistics: {}, for source {}".format(row, source_conf))

                return {
                    col_name: stats
                    for col_name, stats in six.iteritems(row.asDict())
                    if stats["max_size"] is not None and stats["max_size"] > 0
                }

        return {}

    def _add_domain(self, domain_conf, df, array_columns_stats):
        self.info(
            "Adding domain {}\nto df {};\nsource statistics: {}".format(
                pformat(domain_conf), df.schema.simpleString(), pformat(array_columns_stats)
            )
        )

        domain_conf["domain_columns_names"] = [domain_conf["name"]]  # mutating config!

        domain_field = schema_from_hive([(domain_conf["name"], domain_conf["type"])]).fields[0]  # StructField

        if isinstance(domain_field.dataType, ArrayType):
            df = self._add_array_domain(df, domain_conf["columns"], domain_field, array_columns_stats)

        elif isinstance(domain_field.dataType, MapType):
            df = self._add_map_domain(df, domain_conf["columns"], domain_field, domain_conf["type"])

        elif isinstance(domain_field.dataType, AtomicType):
            df, names = self._add_primitive_domain(df, domain_conf["columns"], domain_field)
            domain_conf["domain_columns_names"] = names

        else:
            raise FatalStatusException("Unknown domain type: `{}`".format(domain_field))

        return df

    def _add_array_domain(self, df, source_expressions, domain_field, arrays_sizes):
        domain_name = domain_field.name

        self.info(
            "Adding array domain `{}` {}\nfrom columns {}\nof DF {};\narrays statistics: {}".format(
                domain_name, domain_field, source_expressions, df.schema.simpleString(), pformat(arrays_sizes)
            )
        )

        source_schema = zip(source_expressions, df.selectExpr(*source_expressions).schema.fields)

        for expr, field in source_schema:
            if not isinstance(field.dataType, (AtomicType, ArrayType)):
                raise FatalStatusException(
                    "Invalid domain `{}` {} config, source must be of primitive or array type, "
                    "got `{}` from `{}`".format(domain_name, domain_field, field, expr)
                )

        # Collect primitives

        primitives = [(expr, field) for expr, field in source_schema if isinstance(field.dataType, AtomicType)]

        if primitives:
            df = df.withColumn(
                domain_name, sqlfn.array(*[sqlfn.expr(expr) for expr, _ in primitives]).cast(domain_field.dataType)
            )

            source_schema = [(domain_name, domain_field)] + source_schema  # n.b. columns order is important
            arrays_sizes[domain_name] = len(primitives)

        # Merge collections

        def _max_size(col_name):
            return arrays_sizes.get(col_name, 0)

        arrays = [(expr, field) for expr, field in source_schema if isinstance(field.dataType, ArrayType)]

        if len(arrays) < 1:
            df = df.withColumn(domain_name, sqlfn.lit(None))
        elif len(arrays) == 1:
            df = df.withColumn(domain_name, sqlfn.expr(arrays[0][0]))
        else:
            df = df.withColumn(
                domain_name,
                sqlfn.concat(
                    *[
                        sqlfn.when(
                            sqlfn.expr(expr).isNull(),
                            sqlfn.expr("array_repeat(cast(null as float), {})".format(_max_size(field.name))),
                        )
                        .otherwise(
                            sqlfn.concat(  # With padding-right
                                sqlfn.expr(expr).cast(domain_field.dataType),
                                sqlfn.expr(
                                    "array_repeat(cast(null as float), {} - size({}))".format(
                                        _max_size(field.name), expr
                                    )
                                ).cast(domain_field.dataType),
                            )
                        )
                        .cast(domain_field.dataType)
                        for expr, field in arrays
                    ]
                ),
            )

        # If array is null or empty or all items is null => domain is null
        return df.withColumn(
            domain_name,
            sqlfn.when(
                sqlfn.size(sqlfn.expr("filter(coalesce({}, array()), _x -> _x IS NOT NULL)".format(domain_name))) < 1,
                None,
            )
            .otherwise(df[domain_name])
            .cast(domain_field.dataType),
        )

    def _add_map_domain(self, df, source_expressions, domain_field, domain_type):
        domain_name = domain_field.name

        self.info(
            "Adding map domain `{}` `{}` `{}` from columns\n{} of DF\n{}".format(
                domain_name, domain_type, domain_field, source_expressions, df.schema.simpleString()
            )
        )

        source_schema = zip(source_expressions, df.selectExpr(*source_expressions).schema.fields)

        for expr, field in source_schema:
            if not isinstance(field.dataType, (AtomicType, MapType)):
                raise FatalStatusException(
                    "Invalid domain `{}` {} config, source must be of primitive or map type, got `{}` from `{}`".format(
                        domain_name, domain_field, field, expr
                    )
                )

        # Collect primitives

        primitives = [(expr, field) for expr, field in source_schema if isinstance(field.dataType, AtomicType)]

        if primitives:
            key_val_tuples = [
                (sqlfn.lit(field.name), sqlfn.expr(expr).cast(domain_field.dataType.valueType))
                for expr, field in primitives
            ]

            df = df.withColumn(
                domain_name,
                sqlfn.create_map(*[col for columns in key_val_tuples for col in columns]).cast(domain_field.dataType),
            )

            source_schema = [(domain_name, domain_field)] + source_schema  # n.b. columns order is important

        # Merge collections

        maps = [(expr, field) for expr, field in source_schema if isinstance(field.dataType, MapType)]

        map_filter_template = (
            "map_from_entries(filter(arrays_zip(map_keys({expr}), map_values({expr})), " "_x -> _x['1'] IS NOT NULL))"
        )

        if len(maps) < 1:
            df = df.withColumn(domain_name, sqlfn.lit(None))
        elif len(maps) == 1:
            df = df.withColumn(domain_name, sqlfn.expr(map_filter_template.format(expr=maps[0][0])))
        else:
            df = df.withColumn(
                domain_name,
                sqlfn.expr(
                    "user_dmdesc.combine({})".format(
                        ",".join(
                            [
                                "cast({} as {})".format(map_filter_template.format(expr=expr), domain_type)
                                for expr, _ in maps
                            ]
                        )
                    )
                ),
            )

        # If map is null or empty => domain is null
        return df.withColumn(
            domain_name,
            sqlfn.when(sqlfn.size(sqlfn.coalesce(df[domain_name], sqlfn.expr("map()"))) < 1, None)
            .otherwise(df[domain_name])
            .cast(domain_field.dataType),
        )

    def _add_primitive_domain(self, df, source_expressions, domain_field):
        domain_name = domain_field.name

        self.info(
            "Adding primitive domain `{}` `{}` from columns\n{} of DF\n{}".format(
                domain_name, domain_field, source_expressions, df.schema.simpleString()
            )
        )

        columns_names = []

        for expr, field in zip(source_expressions, df.selectExpr(*source_expressions).schema.fields):
            col_name = "{}_{}".format(domain_name, field.name)
            columns_names.append(col_name)
            df = df.withColumn(col_name, sqlfn.expr(expr).cast(domain_field.dataType))

        return df, columns_names

    def join_domains_containers(self, containers):
        self.info(
            "Joining domains containers, join_rule: `{}`,\ncontainers: {}".format(self.join_rule, pformat(containers))
        )

        joiner = JoinRuleEvaluator(
            dataframes={name: config["df"] for name, config in six.iteritems(containers)},
            checkpoint_service=IntervalCheckpointService(
                base_dir=os.path.join(self.tmp_hdfs_dir, "join"),
                checkpoint_interval=self.checkpoint_interval,
                log_url=self.log_url,
            ),
            log_url=self.log_url,
        )

        return joiner.evaluate(self.join_rule)

    def save_to_staging(self, result_df):
        staging_dir = os.path.join(self.tmp_hdfs_dir, "staging")
        self.info("Write to parquet `{}` ...".format(staging_dir))

        (
            result_df.write.mode("overwrite")
            .option("compression", "gzip")
            .option("mapreduce.fileoutputcommitter.algorithm.version", "2")
            .parquet(staging_dir)
        )

        self.info("Read from parquet from `{}` ...".format(staging_dir))
        return result_df.sql_ctx.sparkSession.read.parquet(staging_dir).persist()

    def check(self, result_df, sources):
        self.info("Counting rows ...")
        actual_target_rows = result_df.count()

        if actual_target_rows < self.min_target_rows:
            self.warn(
                "Insufficient rows count in partition uid_type='{}', {} < {}".format(
                    self.uid_type_conf["output"],
                    actual_target_rows,
                    self.min_target_rows,
                )
            )

            return False

        if not all(source_config.get("features_aggregated", False) for _, source_config in six.iteritems(sources)):
            self.info("Searching duplicate uid values ...")
            duplicate_rows = result_df.groupBy(result_df["uid"]).count().where("count > 1").collect()

            if len(duplicate_rows) > 0:
                self.warn(
                    "Found duplicate uid values in partition uid_type='{}', first 100: {}".format(
                        self.uid_type_conf["output"],
                        duplicate_rows[:100],
                    )
                )

                return False

        return True

    def write(self, result_df):
        df = result_df.withColumn("dt", sqlfn.lit(self.target_dt)).withColumn(
            "uid_type", sqlfn.lit(self.uid_type_conf["output"])
        )

        if any([self.target_db.startswith(prefix) for prefix in self.ALLOWED_DB_PREFIXES]):
            ddl = create_hive_table_ddl(
                hive_from_schema(df.schema),
                self.target_db,
                self.target_table,
                partition_columns=["dt", "uid_type"],
            )
            self.info("Executing DDL: {}".format(ddl))
            result_df.sql_ctx.sparkSession.sql(ddl)
        else:
            self.warn(
                "Table creation allowed only for db names with prefixes {}".format(pformat(self.ALLOWED_DB_PREFIXES))
            )

        self.info("Saving to Hive into {}.{} ...".format(self.target_db, self.target_table))

        insert_into_hive(
            df=df,
            database=self.target_db,
            table=self.target_table,
            max_rows_per_bucket=self.output_max_rows_per_bucket,
            overwrite=True,
            raise_on_missing_columns=False,
            check_parameter=METASTORE_PARTITION_SUCCESS,
        )


class JoinerFeaturesApp(ControlApp):
    task_class = JoinerFeaturesTask
    control_xapp = "JoinerFeatures"

    def prepare_config(self, config):
        max_dt = config["target_dt"]

        if not any(source_name in config["join_rule"] for source_name, _ in six.iteritems(config["feature_sources"])):
            raise FatalStatusException("Invalid config, `join_rule` must contain feature_sources names")

        config["uid_types"] = [self._normalize_uid_type(uid_type_conf) for uid_type_conf in config["uid_types"]]

        target_uid_types = [uid_type_conf["output"] for uid_type_conf in config["uid_types"]]
        if len(target_uid_types) != len(set(target_uid_types)):
            raise FatalStatusException(
                "Invalid config, found duplicates in `uid_types.output`:\n{}".format(pformat(target_uid_types))
            )

        global_matching_flag = False  # `agg` not required
        for uid_type_conf in config["uid_types"]:
            if "matching" in uid_type_conf:
                global_matching_flag = True  # `agg` required
                self._find_matching_partitions(uid_type_conf, max_dt)

        for source_name, source_conf in six.iteritems(config["feature_sources"]):

            if not source_conf.get("agg"):
                dt_selection_mode = source_conf.get("dt_selection_mode", "single_last")
                if global_matching_flag or not dt_selection_mode.startswith("single_"):
                    raise FatalStatusException(
                        "Invalid config for source `{}`, `agg` must be enabled in order to use options: {}".format(
                            source_name,
                            {"matching": global_matching_flag, "dt_selection_mode": dt_selection_mode},
                        )
                    )

            self._find_source_partitions(source_name, source_conf, max_dt)

            for md_source_conf in source_conf.get("md_sources", []):
                self._find_md_source_partitions(source_name, md_source_conf, max_dt)

        return config

    @staticmethod
    def _normalize_uid_type(uid_type_conf):
        if isinstance(uid_type_conf, six.string_types):
            return {
                "output": uid_type_conf,
                "input": [uid_type_conf],
            }
        else:
            uid_type_conf["input"] = uid_type_conf.get("matching", {}).get("input", [uid_type_conf["output"]])

            return uid_type_conf

    def _find_matching_partitions(self, uid_type_conf, max_dt):
        matching = uid_type_conf["matching"]
        max_dt_diff = matching.pop("max_dt_diff", None)

        matching["partitions"] = self.partitions_finder.find(
            database=matching["db"],
            table=matching["table"],
            partition_conf={"uid1_type": matching["input"], "uid2_type": uid_type_conf["output"]},
            min_dt=None if max_dt_diff is None else add_days(max_dt, -max_dt_diff),
            max_dt=max_dt,
        )

        if not matching["partitions"]:
            msg = "Not found required partitions in Hive table {}.{}".format(matching["db"], matching["table"])
            self.error(msg)
            matching["partitions"] = deconstruct(MissingDepsStatusException(msg))

    def _find_source_partitions(self, source_name, source_conf, max_dt):
        database = source_conf["db"]
        table = source_conf["table"]
        period = source_conf.pop("period", 1)
        dt_selection_mode = source_conf.pop("dt_selection_mode", "single_last")

        source_conf["partitions"] = self.partitions_finder.find(
            database=database,
            table=table,
            partition_conf=source_conf.pop("partition_conf"),
            min_dt=add_days(max_dt, -(period - 1)),
            max_dt=max_dt,
            match_mode="any" if dt_selection_mode == "multiple_any" else "all",
            agg_mode="max" if dt_selection_mode == "single_last" else "list",
        )

        if not self.partitions_finder.is_valid_dts(source_conf["partitions"], max_dt, period, dt_selection_mode):
            msg = (
                "Source `{}`, not found required partitions in Hive table {}.{} "
                "for period={} days from {} with mode '{}'"
            ).format(source_name, database, table, period, max_dt, dt_selection_mode)

            self.error(msg)
            source_conf["partitions"] = deconstruct(MissingDepsStatusException(msg))

    def _find_md_source_partitions(self, source_name, md_source_conf, max_dt):
        database = md_source_conf["db"]
        table = md_source_conf["table"]
        max_dt_diff = md_source_conf.pop("max_dt_diff", None)

        md_source_conf["partitions"] = self.partitions_finder.find(
            database=database,
            table=table,
            partition_conf=md_source_conf.pop("partition_conf", {}),
            min_dt=None if max_dt_diff is None else add_days(max_dt, -max_dt_diff),
            max_dt=max_dt,
            use_cache=True,
        )

        if not md_source_conf["partitions"]:
            msg = "Source `{}`, not found required partitions in Hive table {}.{}".format(source_name, database, table)
            self.error(msg)
            md_source_conf["partitions"] = deconstruct(MissingDepsStatusException(msg))
