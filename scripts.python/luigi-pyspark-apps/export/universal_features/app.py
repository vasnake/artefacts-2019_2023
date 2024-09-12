import os

from pprint import pformat

import luigi
import pyspark.sql.functions as sqlfn

from pyspark.sql.types import MapType, ArrayType, StringType, IntegralType

from ..base import ExportFeaturesBaseApp, ExportFeaturesBaseTask


class ExportUniversalFeaturesTask(ExportFeaturesBaseTask):
    """Combine and export user universal features from Hive into HDFS directory in CSV format."""

    csv_params = luigi.DictParameter(description="CSV format parameters")  # type: dict

    def __init__(self, *args, **kwargs):
        super(ExportUniversalFeaturesTask, self).__init__(*args, **kwargs)
        if self.force and self.output().exists() and not self.is_data_loaded():
            raise AssertionError(
                "Output exists and is not loaded into SWARM yet. "
                "Forced run can be done only after processing all existing files."
            )

    def is_data_loaded(self):
        """Check that processed-flag-file exists for each output data file."""
        data_files = set()
        processed_files = set()

        for path in self.hdfs.listdir(self.target_hdfs_dir):
            filename = os.path.basename(path)

            if not self.is_skipped(filename):
                if filename.endswith(".success") or filename.endswith(".fail"):
                    processed_files.add(".".join(filename.split(".")[:-1]))
                else:
                    data_files.add(filename)

        unprocessed_files = data_files.difference(processed_files)

        if unprocessed_files:
            self.warn("Found unprocessed files in {}:\n{}".format(self.target_hdfs_dir, pformat(unprocessed_files)))
            return False

        return True

    @property
    def bu_link_id(self):
        return self.export_columns.get("bu_link_id", {})

    @property
    def key_columns(self):
        return [
            sqlfn.expr("uid2user({}, {})".format(self.export_columns["uid"], self.export_columns["uid_type"])).alias(
                "user"
            )
        ] + ([sqlfn.expr(self.bu_link_id["expr"]).cast("string").alias("bu_link_id")] if self.bu_link_id else [])

    def filter_keys(self, df):
        df = super(ExportUniversalFeaturesTask, self).filter_keys(df)

        if self.bu_link_id and self.bu_link_id["link_type"] != "DummyStrLink":
            return df.where("is_uint32(cast(({}) as string))".format(self.bu_link_id["expr"]))

        return df

    def repartition(self, df, num_parts):
        return df.repartition(num_parts, "user").sortWithinPartitions("user")

    def write(self, df, hdfs_dir):
        self.info("Formatting features as CSV ...")

        for feature_name, _ in self.features:
            df = self.format_feature(df, feature_name)

        df = df.selectExpr(
            *(
                ["user"]
                + (["bu_link_id as `link_type:{}`".format(self.bu_link_id["link_type"])] if self.bu_link_id else [])
                + ["`{}`".format(c) for c in df.columns if c not in {"user", "bu_link_id"}]
            )
        )

        self.info("Writing to CSV ...")

        df.write.option("mapreduce.fileoutputcommitter.algorithm.version", "2").csv(
            path=hdfs_dir,
            mode="overwrite",
            sep=self.csv_params["delimiter"],
            quote=self.csv_params["quote"],
            header=self.csv_params["header"],
        )

    def format_feature(self, df, feature_name):
        feature_type = df.schema[feature_name].dataType

        if isinstance(feature_type, ArrayType):
            feature_header = "num:{}"
            if isinstance(feature_type.elementType, (StringType, IntegralType)):
                feature_header = "cat:{}"

            feature_header = feature_header.format(feature_name)
            self.info("Formatting array feature `{}` as CSV ...".format(feature_header))
            expr = sqlfn.expr("array_join({}, '{}')".format(feature_name, self.csv_params["collection_items_sep"]))

        elif isinstance(feature_type, MapType):
            feature_header = "num:{}".format(feature_name)
            self.info("Formatting map feature `{}` as CSV ...".format(feature_header))
            expr = sqlfn.expr(
                "map_join({}, '{}', '{}')".format(
                    feature_name, self.csv_params["collection_items_sep"], self.csv_params["map_kv_sep"]
                )
            )

        else:
            feature_header = "num:{}"
            if isinstance(feature_type, (StringType, IntegralType)):
                feature_header = "cat:{}"

            feature_header = feature_header.format(feature_name)
            self.info("Formatting primitive feature `{}` as CSV ...".format(feature_header))
            expr = sqlfn.expr("cast({} as string)".format(feature_name))

        return df.withColumn(feature_name, expr).withColumnRenamed(feature_name, feature_header)


class ExportUniversalFeaturesApp(ExportFeaturesBaseApp):
    task_class = ExportUniversalFeaturesTask
    control_xapp = "ExportUniversalFeatures"
