import os
import re

from pprint import pformat

import six
import luigi
import pyspark.sql.functions as sqlfn

from pyspark.sql import SQLContext
from pyspark.sql.types import MapType, ArrayType, FloatType, AtomicType, DoubleType
from pyspark.sql.utils import CapturedException
from luigi.contrib.hive import run_hive_cmd

from prj.common.hive import HiveMetastoreClient
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
    FeatureSchemaValidator,
    IntervalCheckpointService,
    read_orc_table,
    configured_join,
    insert_into_hive,
)
from prj.apps.utils.common.luigix import HiveExternalTask, HiveGenericTarget
from prj.apps.utils.control.luigix.task import ControlApp, BasePySparkTask, ControlTaskMeta
from prj.apps.utils.control.client.status import FatalStatusException

from .join_rule import JoinRuleEvaluator

ALLOWED_DB_PATTERN = re.compile(r"^(snb|sandbox)_")

RESERVED_SCHEMA = [
    # (name, type, comment, is_partition)
    ("uid", "string", "Universal ID", False),
    ("dt", "string", "Data date, YYYY-MM-DD", True),
    ("uid_type", "string", "Type of uid, e.g. HID, BANNER, etc.", True),
]
PARTITION_COLUMNS = [name for name, _, _, is_partition in RESERVED_SCHEMA if is_partition]


class JoinerFeaturesTask(six.with_metaclass(ControlTaskMeta, luigi.WrapperTask)):
    """Join various ML features into a single data mart, write partitions to a Hive table."""

    target_dt = luigi.Parameter(description="Target dt")  # type: str
    target_db = luigi.Parameter(description="Hive target database")  # type: str
    target_table = luigi.Parameter(description="Hive target table")  # type: str

    uid_types = luigi.ListParameter(description="Target uid_type partitions")  # type: list
    sources = luigi.DictParameter(description="Named features sources")  # type: dict
    join_rule = luigi.Parameter(description="Sources join rule")  # type: str
    domains = luigi.ListParameter(description="Features domains")  # type: list

    strict_check_array = luigi.BoolParameter(description="Array size check enable/disable switch")  # type: bool
    nan_inf_to_null = luigi.BoolParameter(description="NaN/Inf replace to NULL enable/disable switch")  # type: bool

    def __init__(self, *args, **kwargs):
        super(JoinerFeaturesTask, self).__init__(*args, **kwargs)

        # check target

        hive_client = HiveMetastoreClient()
        self.target_existent_schema = None

        if hive_client.table_exists(database=self.target_db, table=self.target_table):
            self.target_existent_schema = hive_client.table_schema(database=self.target_db, table=self.target_table)
            target_partition_columns = hive_client.get_partition_names(database=self.target_db, table=self.target_table)

            missing_reserved_fields = {(n, t) for n, t, _, _ in RESERVED_SCHEMA}.difference(self.target_existent_schema)

            if missing_reserved_fields:
                raise FatalStatusException(
                    "Invalid existing target table {}.{} - there are missing reserved fields: {}".format(
                        self.target_db, self.target_table, list(missing_reserved_fields)
                    )
                )

            if set(target_partition_columns) != set(PARTITION_COLUMNS):
                raise FatalStatusException(
                    "Invalid existing target table {}.{} - must have {} partitioning columns, but got {}".format(
                        self.target_db, self.target_table, PARTITION_COLUMNS, target_partition_columns
                    )
                )

        elif ALLOWED_DB_PATTERN.match(self.target_db) is None:
            raise FatalStatusException(
                "Invalid target table {}.{}: "
                "doesn't exist and can't be created automatically in specified database".format(
                    self.target_db, self.target_table
                )
            )

    def requires(self):
        return [
            JoinerFeaturesSubTask(
                target_dt=self.target_dt,
                target_db=self.target_db,
                target_table=self.target_table,
                target_existent_schema=self.target_existent_schema,
                uid_type_conf=uid_type_conf,
                sources=self.sources,
                join_rule=self.join_rule,
                domains=self.domains,
                strict_check_array=self.strict_check_array,
                nan_inf_to_null=self.nan_inf_to_null,
            )
            for uid_type_conf in self.uid_types
        ]


class JoinerFeaturesSubTask(BasePySparkTask):
    """Prepare and join sources to build feature domains for a specific output `uid_type`."""

    target_dt = luigi.Parameter(description="Target dt")  # type: str
    target_db = luigi.Parameter(description="Hive target database")  # type: str
    target_table = luigi.Parameter(description="Hive target table")  # type: str
    target_existent_schema = luigi.ListParameter(
        description="Hive table schema, a list of columns (<name>, <type>), if target table already exists or None otherwise"
    )  # type: list

    uid_type_conf = luigi.DictParameter(description="Target uid_type partition")  # type: dict
    sources = luigi.DictParameter(description="Named features sources")  # type: dict
    join_rule = luigi.Parameter(description="Sources join rule")  # type: str
    domains = luigi.ListParameter(description="Features domains")  # type: list
    strict_check_array = luigi.BoolParameter(description="Array size check enable/disable switch")  # type: bool
    nan_inf_to_null = luigi.BoolParameter(description="NaN/Inf replace to NULL enable/disable switch")  # type: bool

    shuffle_partitions = luigi.IntParameter(description="spark.sql.shuffle.partitions")  # type: int
    executor_memory_gb = luigi.IntParameter(description="spark-submit `--executor-memory` parameter, GB")  # type: int
    output_max_rows_per_bucket = luigi.IntParameter(
        description="Approximate maximum rows amount per bucket within each output Hive partition"
    )  # type: int
    min_target_rows = luigi.IntParameter(description="Acceptable minimum of output rows")  # type: int
    checkpoint_interval = luigi.IntParameter(description="Amount of joins before checkpoint")  # type: int

    retry_count = 0

    def __init__(self, *args, **kwargs):
        super(JoinerFeaturesSubTask, self).__init__(*args, **kwargs)

        self.memo = {}  # Memoise source tables as id -> dataframe
        self.primitive_domains_comments = {}  # Fill in when constructing primitive domains columns

        for param_name in [
            "shuffle_partitions",
            "executor_memory_gb",
            "output_max_rows_per_bucket",
            "min_target_rows",
            "checkpoint_interval",
        ]:
            setattr(self, param_name, self.uid_type_conf.get(param_name, getattr(self, param_name)))

        self.input_uid_types = self.uid_type_conf["input"]
        self.output_uid_type = self.uid_type_conf["output"]
        self.matching = self.uid_type_conf.get("matching")

        self._enhance_log()

    def _enhance_log(self):
        # add output_uid_type to log message
        def _decorate(log_method, msg_pattern):
            def _wrapper(msg, *args, **kwargs):
                log_method(msg_pattern.format(msg), *args, **kwargs)

            return _wrapper

        for level in ["debug", "info", "warn", "error", "critical"]:
            setattr(self, level, _decorate(getattr(self, level), msg_pattern="[{}] {{}}".format(self.output_uid_type)))

    @property
    def name(self):
        return "Grinder__{}__{}_{}__{}__{}__ctid__{}".format(
            self.__class__.__name__,
            self.target_db,
            self.target_table,
            self.target_dt,
            self.uid_type_conf["output"],  # This is called in super __init__, so can't use extra attributes
            self.ctid,
        )

    def output(self):
        return HiveGenericTarget(
            database=self.target_db,
            table=self.target_table,
            partitions={"dt": self.target_dt, "uid_type": self.output_uid_type},
            check_parameter=METASTORE_PARTITION_SUCCESS,
        )

    def requires(self):
        if self.matching:
            yield HiveExternalTask(
                database=self.matching["db"],
                table=self.matching["table"],
                partitions=self.matching["partitions"],
            )

        for source_conf in six.itervalues(self.sources):
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

    @property
    def conf(self):
        conf = super(JoinerFeaturesSubTask, self).conf
        conf["spark.sql.shuffle.partitions"] = self.shuffle_partitions
        conf["spark.sql.legacy.sizeOfNull"] = "false"
        return conf

    @property
    def executor_memory(self):
        return "{}G".format(self.executor_memory_gb)

    def main(self, sc, *args):
        sql_ctx = SQLContext(sc)
        CustomUDFLibrary(sql_ctx.sparkSession).register_all_udf()

        self.read_sources(sql_ctx)

        try:
            # early fail on sql analysis
            result_df = self.build_result(skip_actions=True)
        except CapturedException as e:
            raise FatalStatusException("SQL exception: {}\n{}".format(e.desc, e.stackTrace))

        self.info("Result schema: {}".format(result_df))

        if self.target_existent_schema is None:  # Target table doesn't exist
            comments = dict(
                [(dc["name"], dc.get("comment")) for dc in self.domains]
                + [(name, comment) for name, _, comment, _ in RESERVED_SCHEMA],
                **self.primitive_domains_comments
            )
            ct_ddl = create_hive_table_ddl(
                database=self.target_db,
                table=self.target_table,
                fields=[(n, t, comments[n]) for n, t in hive_from_schema(result_df.schema)],
                partition_columns=PARTITION_COLUMNS,
            )
            target_schema = None
        else:
            ct_ddl = None
            target_schema = schema_from_hive(self.target_existent_schema)

        try:
            FeatureSchemaValidator.validate(result_df.schema, target_schema)
        except (ValueError, TypeError) as e:
            raise FatalStatusException(e)

        result_df = self.build_result(skip_actions=False).persist()

        if ct_ddl is not None:
            self.info("Creating a new target table with DDL:\n{}".format(ct_ddl))
            run_hive_cmd(ct_ddl)

        self.info("Saving to Hive into {}.{} ...".format(self.target_db, self.target_table))

        insert_into_hive(
            df=result_df,
            database=self.target_db,
            table=self.target_table,
            max_rows_per_bucket=self.output_max_rows_per_bucket,
            overwrite=True,
            check_parameter=METASTORE_PARTITION_SUCCESS,
        )
        self.info("SUCCESS")

    def read_sources(self, sql_ctx):
        if self.matching:
            matching_table_id = format_table(self.matching["db"], self.matching["table"], self.matching["partitions"])
            self.info("Loading matching table {}".format(matching_table_id))

            self.memo[matching_table_id] = sql_ctx.sql(
                select_clause(
                    database=self.matching["db"],
                    table=self.matching["table"],
                    partition_dicts=self.matching["partitions"],
                )
            )

        for source_name, source_conf in six.iteritems(self.sources):
            source_table_id = format_table(source_conf["db"], source_conf["table"], source_conf["partitions"])
            self.info("Loading source `{}` table {}".format(source_name, source_table_id))

            if source_conf.get("read_as_big_orc"):
                source_df = read_orc_table(
                    what="{}.{}".format(source_conf["db"], source_conf["table"]),
                    partition_filter_expr=partition_filter_expr(source_conf["partitions"]),
                    spark=sql_ctx.sparkSession,
                )
            else:
                source_df = sql_ctx.sql(
                    select_clause(
                        database=source_conf["db"],
                        table=source_conf["table"],
                        partition_dicts=source_conf["partitions"],
                    )
                )

            self.memo[source_table_id] = source_df

            for md_source_conf in source_conf.get("md_sources", []):
                md_table_id = format_table(md_source_conf["db"], md_source_conf["table"], md_source_conf["partitions"])
                self.info("Loading md_source table {}".format(md_table_id))

                self.memo[md_table_id] = sql_ctx.sql(
                    select_clause(
                        database=md_source_conf["db"],
                        table=md_source_conf["table"],
                        partition_dicts=md_source_conf["partitions"],
                    )
                )

    def build_result(self, skip_actions=False):
        sources = self.prepare_sources(skip_actions)
        joined_sources_df = self.join_sources(sources, skip_actions)
        return (
            self.build_domains(joined_sources_df, skip_actions)
            .withColumn("dt", sqlfn.lit(self.target_dt))
            .withColumn("uid_type", sqlfn.lit(self.output_uid_type))
        )

    def prepare_sources(self, skip_actions=False):
        if self.matching:
            matching_df = self.memo[
                format_table(self.matching["db"], self.matching["table"], self.matching["partitions"])
            ]
        else:
            matching_df = None

        sources = {}
        all_feature_names = []

        for source_name in self.sources:
            source_df = self._prepare_source(source_name, matching_df, skip_actions)
            feature_names = source_df.selectExpr("{}.*".format(source_name)).columns

            try:
                FeatureSchemaValidator.validate_names(feature_names)
            except ValueError as e:
                raise FatalStatusException("Bad `features` config for source `{}`: {}".format(source_name, e))

            all_feature_names.extend(feature_names)
            sources[source_name] = source_df

        ambiguous_names = list(set(all_feature_names).intersection(sources))

        if ambiguous_names:
            raise FatalStatusException(
                "Invalid config, feature names must be different from source names, got {}".format(ambiguous_names)
            )

        return sources

    def _prepare_source(self, source_name, matching_df, skip_actions=False):
        source_conf = unfreeze_json_param(self.sources[source_name])
        self.info("Preparing source `{}`:\n{}".format(source_name, pformat(source_conf)))

        df = self.memo[format_table(source_conf["db"], source_conf["table"], source_conf["partitions"])]

        for md_source_conf in source_conf.get("md_sources", []):
            md_df = (
                self.memo[format_table(md_source_conf["db"], md_source_conf["table"], md_source_conf["partitions"])]
                .where(md_source_conf.get("where", "true"))
                .selectExpr(*["{} as {}".format(v, k) for k, v in six.iteritems(md_source_conf["select"])])
                .distinct()
            )
            df = configured_join(df, md_df, **md_source_conf["join_conf"])

        if "where" in source_conf:
            df = df.where(source_conf["where"])

        uids_expr = "map({})".format(
            ", ".join(
                [
                    "{}, {}".format(uid_type_expr, uid_expr)
                    for uid_type_expr, uid_expr in six.iteritems(source_conf["uids"])
                ]
            )
        )

        # TODO: create utility class taking a list of strings and generating a stream of unique-together strings
        uid_name, uid_type_name, uid2_name = [n * (max(len(c) for c in df.columns) + 1) for n in "abc"]

        df = df.select(*(df.columns + [sqlfn.explode(sqlfn.expr(uids_expr)).alias(uid_type_name, uid_name)]))

        df = df.where(
            df[uid_type_name].isNotNull()
            & df[uid_name].isNotNull()
            & df[uid_type_name].isin(*set(self.input_uid_types).union([self.output_uid_type]))
        )

        if self.nan_inf_to_null:
            df = self.replace_nan_inf_to_null(df)

        if matching_df is not None:
            self.info(
                "Mapping source `{}` uid values from `{}` to `{}`".format(
                    source_name, self.input_uid_types, self.output_uid_type
                )
            )

            if self.output_uid_type in self.input_uid_types:
                # Match everything
                match_filter = "true"
                union_filter = "false"
            else:
                # Match input uid_types, add unchanged existing output uid_type
                match_filter = "{} != '{}'".format(uid_type_name, self.output_uid_type)
                union_filter = "{} = '{}'".format(uid_type_name, self.output_uid_type)

            df = (
                df.where(match_filter)
                .join(
                    matching_df.selectExpr(
                        "uid1 as {}".format(uid_name),
                        "uid1_type as {}".format(uid_type_name),
                        "uid2 as {}".format(uid2_name),
                    ),
                    on=[uid_name, uid_type_name],
                    how="inner",
                )
                .unionByName(df.where(union_filter).withColumn(uid2_name, df[uid_name]))
            )
        else:
            uid2_name = uid_name

        # N.B. Expressions in `features` should be able to use values from original (uid, uid_type) columns
        df = df.groupBy(df[uid2_name].cast("string").alias("uid")).agg(
            *[sqlfn.expr(f) for f in source_conf["features"]]
        )
        source_df = df.select(df["uid"], sqlfn.struct(*[c for c in df.columns if c != "uid"]).alias(source_name))

        if not skip_actions and source_conf.get("checkpoint"):
            source_df = IntervalCheckpointService(
                hdfs_base_dir=os.path.join(self.tmp_hdfs_dir, "source", source_name),
                log_url=self.log_url,
            ).checkpoint(source_df, force=True)

        return source_df

    def replace_nan_inf_to_null(self, df):
        select_exprs = []

        for field in df.schema.fields:
            fname, ftype = field.name, field.dataType
            _FPTypes = (DoubleType, FloatType)

            if isinstance(ftype, _FPTypes):
                expr = "if(not isfinite({x}), null, {x})"

            elif isinstance(ftype, ArrayType) and isinstance(ftype.elementType, _FPTypes):
                expr = "transform({x}, _x -> if(not isfinite(_x), null, _x))"

            elif isinstance(ftype, MapType) and isinstance(ftype.valueType, _FPTypes):
                expr = """
                    map_from_entries(
                        arrays_zip(
                            map_keys({x}),
                            transform(map_values({x}), _x -> if(not isfinite(_x), null, _x))
                        )
                    )
                """

            else:
                self.debug("NaN/Inf replace to NULL is unsupported for type: {}".format(field))
                expr = "{x}"

            select_exprs.append("{} as {}".format(expr.format(x=fname), fname))

        return df.selectExpr(*select_exprs)

    def join_sources(self, sources, skip_actions=False):
        self.info("Joining sources with join_rule=`{}`:\n{}".format(self.join_rule, pformat(sources)))

        joiner = JoinRuleEvaluator(
            dataframes=sources,
            join_keys=["uid"],
            checkpoint_service=IntervalCheckpointService(
                hdfs_base_dir=os.path.join(self.tmp_hdfs_dir, "join"),
                checkpoint_interval=None if skip_actions else self.checkpoint_interval,
                log_url=self.log_url,
            ),
            log_url=self.log_url,
        )
        return joiner.evaluate(self.join_rule)

    def build_domains(self, joined_sources_df, skip_actions=False):
        source_exprs = []

        for c in joined_sources_df.drop("uid").columns:
            source_exprs.extend([c, "{}.*".format(c)])

        joined_sources_df = joined_sources_df.selectExpr(*(["uid"] + source_exprs))

        self.info(
            "Transforming joined sources {} to domains {}".format(
                joined_sources_df, [dc["name"] for dc in self.domains]
            )
        )

        raw_domain_structs = [
            sqlfn.struct(*[sqlfn.expr(v) for v in dc["columns"]]).alias(dc["name"]) for dc in self.domains
        ]
        raw_domains_df = joined_sources_df.select(*(["uid"] + raw_domain_structs))

        if skip_actions:
            arrays_sizes = {}
        else:
            raw_domains_df = (
                IntervalCheckpointService(
                    hdfs_base_dir=os.path.join(self.tmp_hdfs_dir, "staging"),
                    log_url=self.log_url,
                )
                .checkpoint(raw_domains_df, force=True)
                .persist()
            )

            self.info("Gathering dataset stats ...")

            # N.B. min, max functions: a function returns null for null input if
            # `spark.sql.legacy.sizeOfNull` is set to false or `spark.sql.ansi.enabled` is set to true.
            # Otherwise, a function returns -1 for null input.
            # We expect null values for null input.
            # BTW: `str(name)` is used as a fix for PySpark StructType implementation bug, causing this error:
            # TypeError: StructType keys should be strings, integers or slices, got <type 'unicode'> u'array_domain'

            stats_exprs = ["count(1) as total_rows_count"]

            for domain_conf in self.domains:
                if "array" in domain_conf["type"].lower():
                    stats_exprs.extend(
                        [
                            """
                            map(
                                'max_size', max(size({domain_name}.{field_name})),
                                'min_size', min(size({domain_name}.{field_name}))
                            )
                            as `{domain_name}.{field_name}`
                            """.format(
                                domain_name=domain_conf["name"], field_name=field.name
                            )
                            for field in raw_domains_df.schema[str(domain_conf["name"])].dataType.fields
                            if isinstance(field.dataType, ArrayType)
                        ]
                    )

            stats = raw_domains_df.selectExpr(*stats_exprs).head().asDict()
            self.info("Got dataset stats:\n{}".format(pformat(stats)))
            self.info("Checking data restrictions ...")

            got_target_rows = stats.pop("total_rows_count")

            if got_target_rows < self.min_target_rows:
                raise FatalStatusException(
                    "Gor insufficient result rows count: {} < {}".format(got_target_rows, self.min_target_rows)
                )

            if self.strict_check_array:
                for array_name, array_stats in six.iteritems(stats):
                    if array_stats["max_size"] != array_stats["min_size"]:
                        raise FatalStatusException(
                            "Array strict check, unstable array feature size `{}`: {}".format(array_name, array_stats)
                        )

            arrays_sizes = {k: v["max_size"] for k, v in six.iteritems(stats)}

        domain_exprs = []

        for domain_conf in self.domains:
            # Primitive domain produces several columns
            domain_exprs.extend(self._get_domain_exprs(domain_conf, raw_domains_df, arrays_sizes))

        result_df = raw_domains_df.selectExpr(*(["uid"] + domain_exprs))
        extra_exprs = {dc["name"]: dc["extra_expr"] for dc in self.domains if "extra_expr" in dc}

        return result_df.select(
            *[
                sqlfn.expr(extra_exprs.get(field.name, field.name)).cast(field.dataType).alias(field.name)
                for field in result_df.schema
            ]
        )

    def _get_domain_exprs(self, domain_conf, df, arrays_sizes):
        domain_field = schema_from_hive([(domain_conf["name"], domain_conf["type"])])[0]
        self.info("Building domain `{}` ...".format(domain_field.simpleString()))

        if isinstance(domain_field.dataType, ArrayType):
            return [self._get_array_domain_expr(df, domain_field, arrays_sizes)]

        elif isinstance(domain_field.dataType, MapType):
            return [self._get_map_domain_expr(df, domain_field)]

        elif isinstance(domain_field.dataType, AtomicType):
            if "extra_expr" in domain_conf:
                raise FatalStatusException(
                    "Invalid config for domain `{}`: "
                    "parameter `extra_expr` is not allowed for primitive domains".format(domain_field.simpleString())
                )

            return self._get_primitive_domain_exprs(df, domain_field, domain_conf.get("comment"))

        else:
            raise FatalStatusException("Unsupported domain type: `{}`".format(domain_field.simpleString()))

    def _get_array_domain_expr(self, df, domain_field, arrays_sizes):
        domain_name = domain_field.name
        domain_etype = domain_field.dataType.elementType

        primitive_exprs, arrays_exprs = [], []

        for sf in df.selectExpr("{}.*".format(domain_name)).schema:
            sf_expr = "{}.{}".format(domain_name, sf.name)

            if isinstance(sf.dataType, AtomicType):
                primitive_exprs.append("cast({} as {})".format(sf_expr, domain_etype.simpleString()))
            elif isinstance(sf.dataType, ArrayType):
                # Padding-right with null
                arrays_exprs.append(
                    """
                    if(
                        {sf_expr} is null,
                        array_repeat(cast(null as {etype}), {max_size}),
                        concat(
                            cast({sf_expr} as {dtype}),
                            array_repeat(cast(null as {etype}), {max_size} - size({sf_expr}))
                        )
                    )
                    """.format(
                        sf_expr=sf_expr,
                        max_size=arrays_sizes.get(sf_expr) or 0,  # As NULL arrays get NULL size
                        etype=domain_etype.simpleString(),
                        dtype=domain_field.dataType.simpleString(),
                    )
                )
            else:
                raise FatalStatusException(
                    "Invalid config for domain `{}`: all columns must be primitive or array, got `{}`".format(
                        domain_field.simpleString(), sf.simpleString()
                    )
                )

        if primitive_exprs:
            # Array from primitives comes first
            arrays_exprs = ["array({})".format(",".join(primitive_exprs))] + arrays_exprs

        domain_expr = """
            cast(
                if(exists(coalesce({expr}, array()), _0 -> _0 is not null), {expr}, null)
                as {dtype}
            ) as {dname}
            """.format(
            expr="concat({})".format(",".join(arrays_exprs)),
            dtype=domain_field.dataType.simpleString(),
            dname=domain_name,
        )

        self.debug("Got domain `{}` expression:\n{}".format(domain_field.simpleString(), domain_expr))
        return domain_expr

    def _get_map_domain_expr(self, df, domain_field):
        domain_name = domain_field.name
        domain_vtype = domain_field.dataType.valueType

        primitive_kvs, maps_exprs = [], []

        for sf in df.selectExpr("{}.*".format(domain_name)).schema:
            sf_expr = "{}.{}".format(domain_name, sf.name)

            if isinstance(sf.dataType, AtomicType):
                primitive_kvs.extend(
                    ["'{}'".format(sf.name), "cast({} as {})".format(sf_expr, domain_vtype.simpleString())]
                )
            elif isinstance(sf.dataType, MapType):
                maps_exprs.append(sf_expr)
            else:
                raise FatalStatusException(
                    "Invalid config for domain `{}`: all columns must be primitive or map, got `{}`".format(
                        domain_field.simpleString(), sf.simpleString()
                    )
                )

        if primitive_kvs:
            maps_exprs = ["map({})".format(",".join(primitive_kvs))] + maps_exprs

        # Filter NULL map values
        maps_exprs = [
            """
            cast(
                map_from_entries(
                    filter(
                        arrays_zip(map_keys({expr}), map_values({expr})),
                        _0 -> _0['1'] is not null
                    )
                )
                as {dtype}
            )
            """.format(
                expr=expr, dtype=domain_field.dataType.simpleString()
            )
            for expr in maps_exprs
        ]

        if len(maps_exprs) > 1:
            # UDF takes 2 or more arguments, and `map_concat` is incorrect as it preserves duplicates (spark 2.4)
            domain_expr = "user_dmdesc.combine({})".format(",".join(maps_exprs))
        else:
            domain_expr = maps_exprs[0]

        domain_expr = """
            cast(
                if(size(coalesce({expr}, map())) > 0, {expr}, null)
                as {dtype}
            ) as {dname}
            """.format(
            expr=domain_expr, dtype=domain_field.dataType.simpleString(), dname=domain_name
        )

        self.debug("Got domain `{}` expression:\n{}".format(domain_field.simpleString(), domain_expr))
        return domain_expr

    def _get_primitive_domain_exprs(self, df, domain_field, domain_comment):
        domain_name = domain_field.name
        domain_exprs = []

        for sf in df.selectExpr("{}.*".format(domain_name)).schema:
            domain_sf_name = "{}_{}".format(domain_name, sf.name)

            if isinstance(sf.dataType, AtomicType):
                domain_exprs.append(
                    "cast({expr} as {dtype}) as {dname}".format(
                        expr="{}.{}".format(domain_name, sf.name),
                        dtype=domain_field.dataType.simpleString(),
                        dname=domain_sf_name,
                    )
                )
                self.primitive_domains_comments[domain_sf_name] = "{} ({})".format(domain_comment, sf.name)
            else:
                raise FatalStatusException(
                    "Invalid config for domain `{}`: all columns must be primitive, got `{}`".format(
                        domain_field.simpleString(), sf.simpleString()
                    )
                )

        self.debug("Got domain `{}` expressions:\n{}".format(domain_field.simpleString(), pformat(domain_exprs)))
        return domain_exprs


class JoinerFeaturesApp(ControlApp):
    task_class = JoinerFeaturesTask
    control_xapp = "JoinerFeatures"

    def prepare_config(self, config):
        max_dt = config["target_dt"]
        config["uid_types"] = [self._normalize_uid_type(uid_type_conf) for uid_type_conf in config["uid_types"]]

        target_uid_types = [uid_type_conf["output"] for uid_type_conf in config["uid_types"]]

        if len(target_uid_types) != len(set(target_uid_types)):
            raise FatalStatusException(
                "Invalid config, found duplicates in `uid_types.output`: {}".format(target_uid_types)
            )

        for uid_type_conf in config["uid_types"]:
            if "matching" in uid_type_conf:
                self._add_matching_partitions(uid_type_conf, max_dt)

        missing_sources = list(set(config["sources"]).difference(JoinRuleEvaluator.tokenize(config["join_rule"])))

        if missing_sources:
            raise FatalStatusException(
                "Invalid config, `join_rule` must contain all source names, missing: {}".format(missing_sources)
            )

        for source_name, source_conf in six.iteritems(config["sources"]):
            self._add_source_partitions(source_name, source_conf, max_dt)

            for md_source_conf in source_conf.get("md_sources", []):
                md_source_conf["select"]["dt"] = "dt"
                join_on = md_source_conf["join_conf"]["on"]
                md_source_conf["join_conf"]["on"] = [join_on] if isinstance(join_on, six.string_types) else join_on
                md_source_conf["join_conf"]["on"].append("dt")

        domain_names = [dc["name"] for dc in config["domains"]]

        if len(domain_names) != len(set(domain_names)):
            raise FatalStatusException("Invalid config, domain names are not unique: {}".format(domain_names))

        return config

    @staticmethod
    def _normalize_uid_type(uid_type_conf):
        if isinstance(uid_type_conf, six.string_types):
            uid_type_conf = {
                "output": uid_type_conf,
                "input": [uid_type_conf],
            }
        else:
            uid_type_conf["input"] = uid_type_conf.get("matching", {}).get("input", [uid_type_conf["output"]])

        return uid_type_conf

    def _add_matching_partitions(self, uid_type_conf, max_dt):
        matching = uid_type_conf["matching"]
        max_dt_diff = matching.pop("max_dt_diff", None)

        matching["partitions"] = (
            self.partitions_finder.find(
                database=matching["db"],
                table=matching["table"],
                partition_conf={"uid1_type": matching["input"], "uid2_type": uid_type_conf["output"]},
                min_dt=None if max_dt_diff is None else add_days(max_dt, -max_dt_diff),
                max_dt=max_dt,
                use_cache=True,
            )
            or HiveExternalTask.MISSING
        )

        if matching["partitions"] == HiveExternalTask.MISSING:
            self.warn("Not found required partitions in Hive table {}.{}".format(matching["db"], matching["table"]))

    def _add_source_partitions(self, source_name, source_conf, max_dt):
        database = source_conf["db"]
        table = source_conf["table"]
        period = source_conf.pop("period")
        dt_selection_mode = source_conf.pop("dt_selection_mode")

        source_conf["partitions"] = self.partitions_finder.find(
            database=database,
            table=table,
            partition_conf=source_conf.pop("partition_conf"),
            min_dt=add_days(max_dt, -(period - 1)),
            max_dt=max_dt,
            agg_mode="max" if dt_selection_mode == "single_last" else "list",
        )

        if not self.partitions_finder.is_valid_dts(source_conf["partitions"], max_dt, period, dt_selection_mode):
            self.warn(
                "Not found required partitions in Hive table {}.{} for source=`{}`".format(database, table, source_name)
            )
            source_conf["partitions"] = HiveExternalTask.MISSING

        for md_source_conf in source_conf.get("md_sources", []):
            if source_conf["partitions"] == HiveExternalTask.MISSING:
                md_source_conf["partitions"] = HiveExternalTask.MISSING
            else:
                database = md_source_conf["db"]
                table = md_source_conf["table"]
                partition_conf = md_source_conf.pop("partition_conf", {})
                partition_conf["dt"] = {p["dt"] for p in source_conf["partitions"]}

                md_source_conf["partitions"] = self.partitions_finder.find(
                    database=database,
                    table=table,
                    partition_conf=partition_conf,
                    agg_mode="list",
                    use_cache=True,
                )

                if not self.partitions_finder.is_valid_dts(
                    md_source_conf["partitions"], max_dt, period, dt_selection_mode
                ):
                    self.warn(
                        "Not found required partitions in Hive table {}.{} for source=`{}`".format(
                            database, table, source_name
                        )
                    )
                    md_source_conf["partitions"] = HiveExternalTask.MISSING
