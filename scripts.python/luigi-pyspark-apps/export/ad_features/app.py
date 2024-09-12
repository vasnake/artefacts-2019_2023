import luigi
import pyspark.sql.functions as sqlfn

from pyspark.sql.types import MapType, ArrayType

from ..base import ExportFeaturesBaseApp, ExportFeaturesBaseTask


class ExportAdFeaturesTask(ExportFeaturesBaseTask):
    """Combine and export AD features from Hive into HDFS directory in CSV format."""

    max_target_rows = luigi.IntParameter(description="Maximum rows limit")  # type: int
    csv_params = luigi.DictParameter(description="CSV format parameters")  # type: dict

    @property
    def key_columns(self):
        return [sqlfn.expr(self.export_columns["uid"]["expr"]).cast("string").alias(self.export_columns["uid"]["name"])]

    def filter_keys(self, df):
        df = super(ExportAdFeaturesTask, self).filter_keys(df)
        return df.where("is_uint32(cast(({}) as string))".format(self.export_columns["uid"]["expr"]))

    def check(self, df):
        success = False
        got_rows = df.count()

        if got_rows < self.min_target_rows:
            self.warn("Got {} < {} required target rows".format(got_rows, self.min_target_rows))
        elif got_rows > self.max_target_rows:
            self.warn("Got {} > {} allowed target rows".format(got_rows, self.max_target_rows))
        else:
            self.info("Got {} final target rows".format(got_rows))
            success = True

        return success

    def write(self, df, hdfs_dir):
        self.info("Formatting features as CSV ...")

        for feature_name, _ in self.features:
            df = self.format_feature(df, feature_name)

        df = df.select(*([self.export_columns["uid"]["name"]] + [name for name, _ in self.features]))

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
            self.info("Formatting array feature {} as CSV ...".format(feature_name))
            df = df.withColumn(
                feature_name,
                sqlfn.expr("array_join({}, '{}')".format(feature_name, self.csv_params["collection_items_sep"])),
            )

        elif isinstance(feature_type, MapType):
            self.info("Formatting map feature {} as CSV ...".format(feature_name))
            df = df.withColumn(
                feature_name,
                sqlfn.expr(
                    "map_join({}, '{}', '{}')".format(
                        feature_name, self.csv_params["collection_items_sep"], self.csv_params["map_kv_sep"]
                    )
                ),
            )

        else:
            self.info("Formatting primitive feature {} as CSV ...".format(feature_name))
            df = df.withColumn(feature_name, sqlfn.expr("cast({} as string)".format(feature_name)))

        return df


class ExportAdFeaturesApp(ExportFeaturesBaseApp):
    task_class = ExportAdFeaturesTask
    control_xapp = "ExportAdFeatures"
