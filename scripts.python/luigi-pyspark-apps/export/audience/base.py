import os
import abc

import luigi
import pyspark.sql.functions as sqlfn

from pyspark.sql.utils import CapturedException
from pyspark.sql.window import Window

from prj.common.hive import HiveMetastoreClient
from prj.apps.utils.common import add_days
from prj.apps.utils.common.hive import format_table, select_clause
from prj.apps.utils.common.luigix import HdfsTarget, HiveExternalTask
from prj.apps.utils.control.luigix.task import ControlApp
from prj.apps.utils.control.client.status import FatalStatusException

from ..base import ExportHdfsFileBaseTask


class ExportAudienceBaseTask(ExportHdfsFileBaseTask):
    """ABC to export an audience from Hive to files into a possibly existing HDFS directory."""

    source_db = luigi.Parameter(description="Hive source database")  # type: str
    source_table = luigi.Parameter(description="Hive source table")  # type: str
    source_partitions = luigi.ListParameter(description="Selected partitions list")  # type: list
    source_where = luigi.Parameter(description="Spark SQL where-clause", default=None)  # type: str

    audience_id = luigi.IntParameter(description="Audience ID, uint32")  # type: int
    uid_type = luigi.Parameter(description="User ID type for this run")  # type: str
    target_dt = luigi.Parameter(description="Export dt")  # type: str

    min_score = luigi.FloatParameter(description="Minimum acceptable score value")  # type: float
    max_score = luigi.FloatParameter(description="Maximum acceptable score value")  # type: float

    extra_filters = luigi.ListParameter(description="Extra audience filter configurations", default=[])  # type: list

    max_target_rows_per_partition = luigi.IntParameter(
        description="Maximum partition size limit to meet overall audience size, non-positive for no limit"
    )  # type: int

    success_hdfs_basedir = luigi.Parameter(description="HDFS upper-level directory for success flags")  # type: str

    @property
    def success_hdfs_dir(self):
        return os.path.join(self.success_hdfs_basedir, self.target_dt, "")

    @property
    def success_filename(self):
        return "{cls}_{audience_id}-{uid_type}-{target_dt}.success".format(
            cls=self.__class__.__name__, audience_id=self.audience_id, uid_type=self.uid_type, target_dt=self.target_dt
        )

    def output(self):
        return HdfsTarget(path=os.path.join(self.success_hdfs_dir, self.success_filename), isdir=False)

    def requires(self):
        yield HiveExternalTask(database=self.source_db, table=self.source_table, partitions=self.source_partitions)

        for filter_conf in self.extra_filters:
            yield HiveExternalTask(
                database=filter_conf["db"],
                table=filter_conf["table"],
                partitions=filter_conf["partitions"],
            )

    def extract_transform(self, sql_ctx):
        self.info("Loading table: {}".format(format_table(self.source_db, self.source_table, self.source_partitions)))

        df = sql_ctx.sql(
            select_clause(database=self.source_db, table=self.source_table, partition_dicts=self.source_partitions)
        )

        try:
            if self.source_where is not None:
                df = df.where(self.source_where)

            self.info("Aggregating by uid ...")
            df = df.select("uid", "score").dropna().groupBy("uid").agg(sqlfn.avg("score").alias("score"))

            if self.min_score is not None:
                self.info("Filtering score > {} ...".format(self.min_score))
                df = df.where("score > {}".format(self.min_score))

            if self.max_score is not None:
                self.info("Filtering score <= {} ...".format(self.max_score))
                df = df.where("score <= {}".format(self.max_score))

            for filter_conf in self.extra_filters:
                filter_df = sql_ctx.sql(
                    select_clause(
                        database=filter_conf["db"],
                        table=filter_conf["table"],
                        partition_dicts=filter_conf["partitions"],
                    )
                )

                if filter_conf.get("where") is not None:
                    filter_df = filter_df.where(filter_conf["where"])

                if filter_conf["filter_type"] == "intersection":
                    join_type = "inner"
                elif filter_conf["filter_type"] == "subtraction":
                    join_type = "left_anti"
                else:
                    raise FatalStatusException("Unsupported `filter_type`: '{}'".format(filter_conf["filter_type"]))

                df = df.join(filter_df.select("uid").distinct(), on=["uid"], how=join_type)

        except CapturedException as e:
            raise FatalStatusException("SQL exception: {}\n{}".format(e.desc, e.stackTrace))

        if self.max_target_rows_per_partition > 0:
            self.info(
                "Selecting at most {} top-score rows ...".format(
                    self.max_target_rows_per_partition * self.shuffle_partitions
                )
            )

            df = (
                df.repartition("uid")
                .withColumn(
                    "_rn_",
                    sqlfn.row_number().over(
                        Window.partitionBy(sqlfn.spark_partition_id()).orderBy(sqlfn.desc("score"))
                    ),
                )
                .where("_rn_ <= {}".format(self.max_target_rows_per_partition))
                .drop("_rn_")
            )

        return self.custom_transform(df)

    @abc.abstractmethod
    def custom_transform(self, df):
        """Custom transform steps implemented in subclasses.

        :param df: input data as two columns (uid, score): unique uid with avg score, filtered, selected top-score rows.
        :type df: :class:`pyspark.sql.DataFrame`

        :return: transformed data, ready for export.
        :rtype: :class:`pyspark.sql.DataFrame`
        """
        raise NotImplementedError

    def load(self, df):
        super(ExportAudienceBaseTask, self).load(df)
        self.hdfs.mkdir(self.success_hdfs_dir)
        self.hdfs.touchz(os.path.join(self.success_hdfs_dir, self.success_filename))


class ExportAudienceBaseApp(ControlApp):
    ALLOWED_UID_TYPES = NotImplemented

    hive_client = HiveMetastoreClient()

    def prepare_config(self, config):
        if config["uid_type"] not in self.ALLOWED_UID_TYPES:
            raise FatalStatusException("Unknown uid_type: `{}`".format(config["uid_type"]))

        max_target_rows = config.pop("max_target_rows")
        max_target_rows_per_partition = int(float(max_target_rows) / config["shuffle_partitions"])

        if (max_target_rows > 0) and (
            max_target_rows_per_partition * config["shuffle_partitions"] < config["min_target_rows"]
        ):
            raise FatalStatusException(
                "Limits inconsistency - the following condition is violated with enabled `max_target_rows` option: "
                "`floor(max_target_rows / shuffle_partitions) * shuffle_partitions` >= `min_target_rows`. "
                "Got {} < {}".format(
                    max_target_rows_per_partition * config["shuffle_partitions"], config["min_target_rows"]
                )
            )

        self._validate_schema(
            db=config["source_db"],
            table=config["source_table"],
            required_columns=["uid", "score"],
            required_partition_names=["dt", "uid_type"],
        )

        max_dt = config["target_dt"]
        max_dt_diff = config.pop("max_dt_diff", None)

        source_partitions = self.partitions_finder.find(
            database=config["source_db"],
            table=config["source_table"],
            partition_conf=dict(config.pop("source_partition_conf"), uid_type=config["uid_type"]),
            min_dt=None if max_dt_diff is None else add_days(max_dt, -max_dt_diff),
            max_dt=max_dt,
        )

        for filter_conf in config.get("extra_filters", []):
            self._validate_schema(
                db=filter_conf["db"],
                table=filter_conf["table"],
                required_columns=["uid"],
                required_partition_names=["dt", "uid_type"],
            )

            max_dt_diff = filter_conf.pop("max_dt_diff", None)

            filter_conf["partitions"] = (
                self.partitions_finder.find(
                    database=filter_conf["db"],
                    table=filter_conf["table"],
                    partition_conf=dict(filter_conf.pop("partition_conf", {}), uid_type=config["uid_type"]),
                    min_dt=None if max_dt_diff is None else add_days(max_dt, -max_dt_diff),
                    max_dt=max_dt,
                )
                or HiveExternalTask.MISSING
            )

        config["source_partitions"] = source_partitions or HiveExternalTask.MISSING
        config["max_target_rows_per_partition"] = max_target_rows_per_partition
        return config

    def _validate_schema(self, db, table, required_columns, required_partition_names):
        columns = self.hive_client.get_columns(database=db, table=table)
        partition_names = self.hive_client.get_partition_names(database=db, table=table)

        if not set(required_columns).issubset(columns):
            raise FatalStatusException("No {} columns in Hive table {}.{}".format(required_columns, db, table))

        if not set(required_partition_names).issubset(partition_names):
            raise FatalStatusException(
                "Hive table {}.{} is not sufficiently partitioned, "
                "required partitions are {}".format(db, table, required_partition_names)
            )
