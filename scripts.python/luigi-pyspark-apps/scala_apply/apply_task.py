import os
import sys
import six
import luigi
import logging
import numpy as np

from pydoc import locate
from functools import partial
from pyspark.files import SparkFiles
from pyspark.sql import SQLContext, Row
from pyspark.sql.types import StructType
from boltons.iterutils import chunked_iter

from dmcore.utils.logs import configure_logging

from dmgrinder.interface.tools import merge_grouped_features
from dmgrinder.interface.transformers import ApplyModelsTransformer, ModelConfig
from dmgrinder.tasks.ml.base import ControlPySparkTask, BaseRunner, BaseConfigGetter
from dmgrinder.utils.control_helpers import CONTROL_APP, create_url_handler
from dmgrinder.utils.hive import HiveGenericTarget, HiveExternalTask, select_clause, METASTORE_PARTITION_SUCCESS
from dmgrinder.utils.spark import PySparkRows2LearningData, insert_into_hive, distribute_files


class ApplyTask(ControlPySparkTask):
    """
    This job applies a batch of learned models in a single run, separately for different UID_TYPEs.
    Activity filtering can be also set, to score only active users using specified activity dataset.
    """

    uid_type = luigi.Parameter(description="User ID type for this run")  # type: str
    jvm_apply = luigi.BoolParameter(description="Whether all models are applied directly in JVM")  # type: bool

    source_db = luigi.Parameter(description="Hive database with source features")  # type: str
    source_table = luigi.Parameter(description="Hive table with source features")  # type: str
    source_partitions = luigi.ListParameter(
        description="List of dicts, partitions to take from features table"
    )  # type: list

    activity_db = luigi.Parameter(description="Active users list database", default=None)  # type: str
    activity_table = luigi.Parameter(description="Active users list table", default=None)  # type: str
    activity_partitions = luigi.ListParameter(
        description="List of dicts, partitions to take from active users table",
        default=[]
    )  # type: list

    target_db = luigi.Parameter(description="Hive database for output")  # type: str
    target_table = luigi.Parameter(description="Hive table for output")  # type: str
    target_dt = luigi.Parameter(description="Hive output partition date")  # type: str
    target_partitions = luigi.ListParameter(
        description="List of dicts, output partitions after models apply"
    )  # type: list

    models_info = luigi.ListParameter(description="List of dicts with applied models specification")  # type: list

    columns_order = luigi.ListParameter(
        description="Order of columns (including partitioning) to output into a target table"
    )  # type: list

    python_batch_size = luigi.IntParameter(
        description="Rows batch size for model apply in Python",
        default=10000
    )  # type: int

    output_max_rows_per_bucket = luigi.IntParameter(
        description="Approximate top limit of rows per bucket within each output Hive partition",
        default=1000000
    )  # type: int

    def __init__(self, *args, **kwargs):
        super(ApplyTask, self).__init__(*args, **kwargs)
        self.apply_model_list = []
        for model_info in self.models_info:
            model_class = locate(model_info["python_class"])
            # instantiate model
            md = model_class(
                model_dir=model_info["model_dir"],
                audience_name=model_info["audience_name"],
                project_params=model_info["project_params"],
                hyper_params=model_info["hyper_params"],
                extra_params=model_info["extra_params"],
                feature_groups=model_info["feature_groups"],
                log_url=self.log_url,
            )
            self.apply_model_list.append(md)
        self.output_fields = self._get_output_fields()

    def _get_output_fields(self):
        models_output = [md.OUTPUT_FIELDS for md in self.apply_model_list]
        if len(set(map(str, models_output))) != 1:
            raise TypeError("All applied models must have the same OUTPUT_FIELDS schema")
        output_fields = models_output[0]
        output_field_names = [f.name for f in output_fields]
        output_columns_required = set(self.columns_order).difference(["uid", "dt", "uid_type"])
        if set(output_field_names) != output_columns_required:
            raise AssertionError(
                "Incorrect model class '{}': output field names {} must consist of a set {}".format(
                    self.apply_model_list[0].__class__.__name__,
                    output_field_names, output_columns_required
                )
            )
        return output_fields

    @property
    def name(self):
        return "{}__{}".format(super(ApplyTask, self).name, self.uid_type)

    def output(self):
        return HiveGenericTarget(
            database=self.target_db,
            table=self.target_table,
            partitions=self.target_partitions,
            check_parameter=METASTORE_PARTITION_SUCCESS,
        )

    def requires(self):
        yield HiveExternalTask(
            database=self.source_db,
            table=self.source_table,
            partitions=self.source_partitions
        )
        if self.activity_partitions:
            yield HiveExternalTask(
                database=self.activity_db,
                table=self.activity_table,
                partitions=self.activity_partitions
            )

    def main(self, sc, *args):
        sql_context = SQLContext(sc)

        configure_logging(
            logger_name="dmcore",
            level=logging.DEBUG,
            stream=sys.stdout,
            web_handler=create_url_handler(url=self.log_url)
        )

        self.info("UID TYPE: '{}'".format(self.uid_type))
        self.info("MODELS TOTAL AMOUNT: {}\n".format(len(self.apply_model_list)))
        self.info("Distributing models files to executor nodes ...")

        for md in self.apply_model_list:
            self.info("Loading model directory: {}".format(md.model_dir))
            distribute_files(sc, md.model_dir, recursive=True)

        self.info("Successfully distributed all models\n")

        self.info("Loading and preparing Hive source:\n\ttable: {db}.{table}\n\tpartitions: {part}".format(
            db=self.source_db, table=self.source_table, part=self.source_partitions
        ))

        source_df = sql_context.sql(select_clause(
            database=self.source_db,
            table=self.source_table,
            partition_dicts=self.source_partitions
        )).drop("dt", "uid_type")

        if len(self.activity_partitions) > 0:
            self.info("Filtering active users using Hive:\n\ttable: {db}.{table}\n\tpartitions: {part}".format(
                db=self.activity_db, table=self.activity_table, part=self.activity_partitions
            ))
            source_df = source_df.join(
                sql_context.sql(select_clause(
                    database=self.activity_db,
                    table=self.activity_table,
                    columns=["uid"],
                    partition_dicts=self.activity_partitions
                )),
                on=["uid"], how="inner"
            )
        else:
            self.info("No activity filter, score all users from source dataset")
            source_df = source_df.repartition("uid")  # some guarantee of evenly distributed rows

        self.info("Applying models to Hive source:\n\ttable: {db}.{table}\n\tpartitions: {part}".format(
            db=self.source_db, table=self.source_table, part=self.source_partitions
        ))

        self.info("Trying Scala-Apply first ...")
        success, target_df = self.apply_scala_models(source_df)

        if not success:
            self.info("Scala-Apply is unavailable")
            target_df = self.apply_python_models(source_df)

        self.info("Writing Apply results to HDFS {} ...".format(self.tmp_hdfs_dir))

        target_df.selectExpr(*self.get_target_columns()).write.option(
            "mapreduce.fileoutputcommitter.algorithm.version", "2"
        ).orc(self.tmp_hdfs_dir, mode="overwrite")

        self.info("Saving to Hive table {}.{} ...".format(self.target_db, self.target_table))

        insert_into_hive(
            df=sql_context.read.orc(self.tmp_hdfs_dir).persist(),
            database=self.target_db, table=self.target_table,
            max_rows_per_bucket=self.output_max_rows_per_bucket,
            overwrite=True,
            check_parameter=METASTORE_PARTITION_SUCCESS,
        )

        self.info("SUCCESS")

    def apply_python_models(self, source_df):
        self.info("Executing Python-Apply ...")
        self.info("Collecting features indices for all models ...")

        for md in self.apply_model_list:
            local_dir = SparkFiles.get(os.path.basename(md.model_dir))
            features_local_path = os.path.join(local_dir, md.FEATURES_FILENAME)
            self.info("Collecting model features indices from driver path: {}".format(features_local_path))
            md.grouped_features = md.read_features(features_local_path)

        self.info("Successfully collected all models features indices\n")

        self.info("Merging features indices for all models ...")
        grouped_features_all, model_column_slices = merge_grouped_features(
            *(md.grouped_features for md in self.apply_model_list)
        )
        self.info("Merged feature groups count: {}".format(
            [(g, len(f)) for g, f in six.iteritems(grouped_features_all)]
        ))

        sql_context = source_df.sql_ctx
        sc = sql_context.sparkSession.sparkContext

        rows_transformer = sc.broadcast(
            PySparkRows2LearningData(
                schema=source_df.schema, numeric_type=np.float32,
                null_format=None, null_value=None,
                features=grouped_features_all
            )
        )

        apply_model_list = sc.broadcast(self.apply_model_list)
        model_column_slices = sc.broadcast(model_column_slices)

        def _apply_fun(df_rows, uid_type, batch_size, keep_columns):
            models = [
                md.load(SparkFiles.get(os.path.basename(md.model_dir)))
                for md in apply_model_list.value
            ]
            for chunk in chunked_iter(df_rows, batch_size):
                x_all = rows_transformer.value.transform(chunk).x  # load features from rows to np matrix
                keep_column_dicts = [dict((k, row[k]) for k in keep_columns) for row in chunk]
                for md, col_ind in zip(models, model_column_slices.value):
                    output_dicts = md.apply(x=x_all[:, col_ind], uid_type=uid_type)
                    for keep_column_dict, output_dict in zip(keep_column_dicts, output_dicts):
                        if output_dict is not None:
                            yield Row(**dict(keep_column_dict, **output_dict))

        return sql_context.createDataFrame(
            source_df.rdd.mapPartitions(
                partial(_apply_fun, uid_type=self.uid_type, batch_size=self.python_batch_size, keep_columns=["uid"]),
                preservesPartitioning=True
            ),
            schema=StructType([source_df.schema["uid"]] + list(self.output_fields))
        )

    def apply_scala_models(self, source_df):
        if not self.jvm_apply:
            return False, None

        self.info("Executing Scala-Apply ...")

        for md in self.apply_model_list:
            md.set_apply_uid_type(self.uid_type)

        descriptions = ModelConfig.pack_configs_list(*[md.configuration for md in self.apply_model_list])
        columns = ModelConfig.pack_columns_list(*["uid"])

        self.info("Initializing Scala-Apply spark.ml.transformer with models: '{}'".format(descriptions))
        transformer = ApplyModelsTransformer(models=descriptions, keep_columns=columns)

        if not transformer.initialize():
            self.warn("Transformer `initialize` has failed")
            return False, None

        self.info("Invoking Scala-Apply spark.ml.transformer `transform` ...")
        return True, transformer.transform(source_df)

    def get_target_columns(self):
        columns = []
        for col_name in self.columns_order:
            if col_name == "dt":
                columns.append("'{}' as dt".format(self.target_dt))
            elif col_name == "uid_type":
                columns.append("'{}' as uid_type".format(self.uid_type))
            else:
                columns.append(col_name)
        return columns
