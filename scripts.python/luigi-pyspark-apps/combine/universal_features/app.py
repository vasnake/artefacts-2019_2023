import six
import luigi
import pyspark.sql.functions as sqlfn

from prj.apps.utils.common import add_days
from prj.apps.utils.common.hive import select_clause
from prj.apps.utils.common.spark import configured_join
from prj.apps.utils.common.luigix import HiveExternalTask
from prj.apps.utils.control.client.status import MissingDepsStatusException

from ..base import CombineFeaturesBaseApp, CombineFeaturesBaseTask


class CombineUniversalFeaturesTask(CombineFeaturesBaseTask):
    """Combine and save user universal features from Hive source into Hive target table."""

    source_where = luigi.Parameter(description="SQL expression for table filter", default=None)  # type: str
    combine_columns = luigi.DictParameter(description="SQL expressions for used columns")  # type: dict
    filter_config = luigi.DictParameter(description="Filter table configuration", default={})  # type: dict

    @property
    def bu_link_id(self):
        return self.combine_columns.get("bu_link_id", {})

    @property
    def matching_config(self):
        return self.bu_link_id.get("matching_config", {})

    def requires(self):
        yield HiveExternalTask(database=self.source_db, table=self.source_table, partitions=self.source_partitions)

        if self.matching_config:
            yield HiveExternalTask(
                database=self.matching_config["db"],
                table=self.matching_config["table"],
                partitions=self.matching_config["partitions"],
            )

        if self.filter_config:
            yield HiveExternalTask(
                database=self.filter_config["db"],
                table=self.filter_config["table"],
                partitions=self.filter_config["partitions"],
            )

    def aggregate_combine(self, df):
        key_columns = {k: sqlfn.expr(self.combine_columns[k]) for k in ["uid", "uid_type"]}

        if self.bu_link_id:
            key_columns["bu_link_id"] = sqlfn.expr(self.bu_link_id["expr"])

        for _, col in six.iteritems(key_columns):
            df = df.where(col.isNotNull())

        if self.source_where:
            df = df.where(self.source_where)

        if self.matching_config:
            self.info("Matching bu_link_id ...")

            new_col_nchars = max(len(name) for name in df.columns) + 1
            id_from_alias = "a" * new_col_nchars
            id_to_alias = "b" * new_col_nchars

            matching_df = (
                df.sql_ctx.sql(
                    select_clause(
                        database=self.matching_config["db"],
                        table=self.matching_config["table"],
                        columns=[
                            "{} as {}".format(self.matching_config["bu_link_id_from"], id_from_alias),
                            "{} as {}".format(self.matching_config["bu_link_id_to"], id_to_alias),
                        ],
                        partition_dicts=self.matching_config["partitions"],
                    )
                )
                .where(self.matching_config.get("where", "true"))
                .distinct()
            )

            df = configured_join(
                left=df,
                right=matching_df,
                on=(key_columns["bu_link_id"] == sqlfn.col(id_from_alias)),
                **self.matching_config["join_conf"]
            )

            key_columns["bu_link_id"] = sqlfn.col(id_to_alias)

        df = df.groupBy(*[col.alias(name) for name, col in six.iteritems(key_columns)]).agg(
            sqlfn.expr(self.combine_columns["feature"]).alias("feature")
        )

        if self.filter_config:
            self.info("Applying user filter ...")

            filter_df = (
                df.sql_ctx.sql(
                    select_clause(
                        database=self.filter_config["db"],
                        table=self.filter_config["table"],
                        columns=["{} as {}".format(self.filter_config["columns"][k], k) for k in ["uid", "uid_type"]],
                        partition_dicts=self.filter_config["partitions"],
                    )
                )
                .where(self.filter_config.get("where", "true"))
                .distinct()
            )

            df = df.join(filter_df, on=["uid", "uid_type"], how="inner")

        return df


class CombineUniversalFeaturesApp(CombineFeaturesBaseApp):
    task_class = CombineUniversalFeaturesTask
    control_xapp = "CombineUniversalFeatures"

    def prepare_config(self, config):
        config = super(CombineUniversalFeaturesApp, self).prepare_config(config)
        max_dt = config["target_dt"]

        bu_link_id = config["combine_columns"].get("bu_link_id")
        if bu_link_id:
            matching_config = bu_link_id.get("matching_config")
            if matching_config:
                database = matching_config["db"]
                table = matching_config["table"]
                max_dt_diff = matching_config.pop("max_dt_diff", None)

                matching_config["partitions"] = self.partitions_finder.find(
                    database=database,
                    table=table,
                    partition_conf=matching_config.pop("partition_conf"),
                    min_dt=None if max_dt_diff is None else add_days(max_dt, -max_dt_diff),
                    max_dt=max_dt,
                )

                if not matching_config["partitions"]:
                    raise MissingDepsStatusException("Not found partitions in Hive table {}.{}".format(database, table))

            else:
                self.info("Matching table for bu_link_id is undefined.")
        else:
            self.info("bu_link_id is undefined.")

        filter_config = config.get("filter_config")
        if filter_config:
            database = filter_config["db"]
            table = filter_config["table"]
            max_dt_diff = filter_config.pop("max_dt_diff", None)

            filter_config["partitions"] = self.partitions_finder.find(
                database=database,
                table=table,
                partition_conf=filter_config.pop("partition_conf"),
                min_dt=None if max_dt_diff is None else add_days(max_dt, -max_dt_diff),
                max_dt=max_dt,
            )

            if not filter_config["partitions"]:
                raise MissingDepsStatusException("Not found partitions in Hive table {}.{}".format(database, table))

        else:
            self.info("Filter table is undefined.")

        return config
