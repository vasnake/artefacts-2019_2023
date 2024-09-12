from datetime import datetime

import luigi
import pyspark.sql.functions as sqlfn

from prj.apps.export.audience.base import ExportAudienceBaseApp, ExportAudienceBaseTask
from prj.apps.utils.control.client.status import FatalStatusException


class ExportAudienceRbTask(ExportAudienceBaseTask):
    """Prepare and export part of an audience from Hive table into HDFS directory in TSV format."""

    UID_CONVERSIONS = {
        "GAID": """regexp_replace(lower(uid), "^(.{8})(.{4})(.{4})(.{4})(.{12})$", "$1-$2-$3-$4-$5")""",
        "IDFA": """regexp_replace(upper(uid), "^(.{8})(.{4})(.{4})(.{4})(.{12})$", "$1-$2-$3-$4-$5")""",
    }

    UID_TYPE_TO_ID_TYPE = {"EMAIL": "email", "OKID": "ok", "VID": "vid", "VKID": "vk", "IDFA": "idfa", "GAID": "gaid"}

    life_time = luigi.IntParameter(description="Amount of days before expiration", default=60)  # type: int
    exclude_audience_ids = luigi.ListParameter(description="Audience ids to exclude uid from", default=[])  # type: list

    @property
    def target_hdfs_subdirs(self):
        return []

    def target_filename(self, idx):
        return "task.{datetime}-{ctid}-RB-{audience_id}-{uid_type}-{target_dt}-{part}".format(
            datetime=datetime.now().strftime("%Y%m%d%H%M%S"),
            ctid=self.ctid,
            audience_id=self.audience_id,
            uid_type=self.uid_type,
            target_dt=self.target_dt,
            part="%05d" % idx,
        )

    def custom_transform(self, df):
        self.info("Building target rows ...")

        # All values are validated to be unique.
        audiences = list(self.exclude_audience_ids) + [self.audience_id]

        return df.select(
            sqlfn.lit(self.UID_TYPE_TO_ID_TYPE[self.uid_type]).alias("id_type"),
            sqlfn.expr(self.UID_CONVERSIONS.get(self.uid_type, "uid")).alias("user_id"),
            sqlfn.explode(
                sqlfn.map_from_arrays(
                    sqlfn.array([sqlfn.lit(a) for a in audiences]),
                    sqlfn.array([sqlfn.lit(self.life_time if a == self.audience_id else -1) for a in audiences]),
                )
            ).alias("audience_id", "life_time"),
        )

    def write(self, df, hdfs_dir):
        (
            df.write
            .option("mapreduce.fileoutputcommitter.algorithm.version", "2")
            .csv(hdfs_dir, mode="overwrite", sep="\t", quote="", header=False)
        )


class ExportAudienceRbApp(ExportAudienceBaseApp):
    task_class = ExportAudienceRbTask
    control_xapp = "ExportAudienceRb"

    ALLOWED_UID_TYPES = set(ExportAudienceRbTask.UID_TYPE_TO_ID_TYPE.keys())

    def prepare_config(self, config):
        if config["audience_id"] in config["exclude_audience_ids"]:
            raise FatalStatusException("Current `audience_id` must not be in `exclude_audience_ids` list")
        return super(ExportAudienceRbApp, self).prepare_config(config)
