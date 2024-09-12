from datetime import datetime
from collections import namedtuple

import luigi
import pyspark.sql.functions as sqlfn

from prj.apps.export.audience.base import ExportAudienceBaseApp, ExportAudienceBaseTask
from prj.apps.utils.control.client.status import FatalStatusException


class TargetAudienceScorePbConfig(object):
    """Protobuf reader/writer config"""

    file_format = "dwh.proto2.input.RbPbFileFormat"
    proto_class_name = "dwh.proto2.proto.TargetAudienceScoreProto$TargetAudienceScore"
    proto_header_type = "309"
    proto_magic = "F2E2F0E2"


class ExportAudienceTrgTask(ExportAudienceBaseTask):
    """Prepare and export part of an audience from Hive table into HDFS directory in protobuf format."""

    UID_CONVERSIONS = {
        "GAID": """regexp_replace(lower(uid), "^(.{8})(.{4})(.{4})(.{4})(.{12})$", "$1-$2-$3-$4-$5")""",
        "IDFA": """regexp_replace(upper(uid), "^(.{8})(.{4})(.{4})(.{4})(.{12})$", "$1-$2-$3-$4-$5")""",
        "EMAIL": """cast(uid64(uid) as decimal(21,0))""",
        "VID": """cast(conv(uid, 16, 10) as decimal(21,0))""",
    }

    UidTypeConfig = namedtuple("UidTypeConfig", ["dir_name", "pb_value", "id_name", "id_type"])

    UID_TYPE_MAPPING = {
        "IDFA": UidTypeConfig(dir_name="device", pb_value="DEVICE", id_name="id_string", id_type="string"),
        "GAID": UidTypeConfig(dir_name="device", pb_value="DEVICE", id_name="id_string", id_type="string"),
        "IPV4": UidTypeConfig(dir_name="ipv4", pb_value="IPv4", id_name="id_number", id_type="long"),
        "VKID": UidTypeConfig(dir_name="vk", pb_value="VKID", id_name="id_number", id_type="long"),
        "OKID": UidTypeConfig(dir_name="ok", pb_value="OKID", id_name="id_number", id_type="long"),
        "EMAIL": UidTypeConfig(dir_name="mm", pb_value="MM", id_name="id_number", id_type="long"),
        "MMID": UidTypeConfig(dir_name="mm", pb_value="MM", id_name="id_number", id_type="long"),
        "VID": UidTypeConfig(dir_name="vid", pb_value="VID", id_name="id_number", id_type="long"),
        "HID": UidTypeConfig(dir_name="hid", pb_value="HID", id_name="id_number", id_type="long"),
    }

    scale_conf = luigi.DictParameter(description="Score scaling config")  # type: dict

    @property
    def target_hdfs_subdirs(self):
        return [self.UID_TYPE_MAPPING[self.uid_type].dir_name, self.target_dt]

    def target_filename(self, idx):
        return "audience_score.{datetime}-{ctid}-TRG-{audience_id}-{uid_type}-{target_dt}-{part}.pb.gz".format(
            datetime=datetime.now().strftime("%Y%m%d%H%M%S"),
            ctid=self.ctid,
            audience_id=self.audience_id,
            uid_type=self.uid_type,
            target_dt=self.target_dt,
            part="%05d" % idx,
        )

    def custom_transform(self, df):
        if self.scale_conf["scale"]:
            self.info("Scaling score column ...")

            df = df.crossJoin(
                sqlfn.broadcast(df.selectExpr("min(score) as min_score", "max(score) as max_score"))
            ).selectExpr(
                "uid",
                """
                (
                    {new_min} + ({new_max} - {new_min}) * (
                        {revert} + (1 - 2 * {revert}) * nvl((score - min_score) / (max_score - min_score), 1.0)
                    )
                ) as score
                """.format(
                    new_min=self.scale_conf["min"],
                    new_max=self.scale_conf["max"],
                    revert=int(self.scale_conf["revert"]),
                ),
            )

        user_id_name = self.UID_TYPE_MAPPING[self.uid_type].id_name
        user_id_type = self.UID_TYPE_MAPPING[self.uid_type].id_type
        uid_type_proto = self.UID_TYPE_MAPPING[self.uid_type].pb_value

        self.info("Building target rows ...")

        return df.select(
            sqlfn.lit(uid_type_proto).alias("id_type"),
            sqlfn.expr(self.UID_CONVERSIONS.get(self.uid_type, "uid")).cast(user_id_type).alias(user_id_name),
            sqlfn.lit(self.audience_id).alias("audience_id"),
            sqlfn.round("score").cast("int").alias("score"),
            sqlfn.unix_timestamp().cast("int").alias("ts"),
        )

    def write(self, df, hdfs_dir):
        (
            df.write.mode("overwrite")
            .option("mapreduce.fileoutputcommitter.algorithm.version", "2")
            .option("proto_class_name", TargetAudienceScorePbConfig.proto_class_name)
            .option("proto_header_type", TargetAudienceScorePbConfig.proto_header_type)
            .option("proto_magic", TargetAudienceScorePbConfig.proto_magic)
            .format(TargetAudienceScorePbConfig.file_format)
            .save(hdfs_dir)
        )


class ExportAudienceTrgApp(ExportAudienceBaseApp):
    task_class = ExportAudienceTrgTask
    control_xapp = "ExportAudienceTrg"

    ALLOWED_UID_TYPES = set(ExportAudienceTrgTask.UID_TYPE_MAPPING.keys())

    def prepare_config(self, config):
        scale_conf = config["scale_conf"]

        if scale_conf["scale"] and (scale_conf["min"] >= scale_conf["max"]):
            raise FatalStatusException("Wrong scale_conf - required `scale_conf['min'] < scale_conf['max']`")

        return super(ExportAudienceTrgApp, self).prepare_config(config)
