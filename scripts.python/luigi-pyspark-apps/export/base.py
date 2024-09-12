import os
import abc
import math

from pprint import pformat

import luigi
import pyspark.sql.functions as sqlfn

from pyspark.sql import SparkSession
from pyspark.sql.types import MapType, ArrayType, StringType, IntegralType, FractionalType
from pyspark.sql.utils import CapturedException
from luigi.contrib.hdfs.error import HDFSCliError

from dmcore.utils.common import to_str
from prj.apps.utils.common import add_days
from prj.apps.utils.common.hive import format_table, select_clause
from prj.apps.utils.common.luigix import HdfsTarget, HiveExternalTask
from prj.apps.utils.common.fs.hdfs import SUCCESS_FILENAME, HdfsClient
from prj.apps.utils.control.luigix.task import ControlApp, ControlETLPySparkTask
from prj.apps.utils.control.client.status import FatalStatusException


class ExportHdfsBaseTask(ControlETLPySparkTask):
    """ABC to export various data into HDFS destination."""

    hdfs = HdfsClient()

    target_hdfs_basedir = luigi.Parameter(description="HDFS upper-level directory to write result")  # type: str

    output_max_rows_per_file = luigi.IntParameter(
        description="Approximate maximum of rows per file", default=1000000
    )  # type: int

    @property
    def target_hdfs_dir(self):
        """HDFS final directory to write result.

        :return: path to target directory with a trailing slash.
        :rtype: str
        """
        return os.path.join(self.target_hdfs_basedir, *(list(self.target_hdfs_subdirs) + [""]))

    @abc.abstractproperty
    def target_hdfs_subdirs(self):
        """HDFS sub-directories (if any) relative to `target_hdfs_basedir` producing `target_hdfs_dir`.

        :return: an iterable of names for required sub-directories.
        :rtype: list[str]
        """
        raise NotImplementedError

    def load(self, df):
        rows_count = df.count()
        num_parts = int(math.ceil(rows_count / float(self.output_max_rows_per_file)))

        self.info(
            "Repartitioning {} rows to {} partitions with maximum rows per file limit: {}".format(
                rows_count, num_parts, self.output_max_rows_per_file
            )
        )
        df = self.repartition(df, num_parts)

        self.info(
            "Writing {} result partitions into temporary directory {} ...".format(
                df.rdd.getNumPartitions(), self.tmp_hdfs_dir
            )
        )
        self.write(df, self.tmp_hdfs_dir)

        self.info("Moving files from {} to {} ...".format(self.tmp_hdfs_dir, self.target_hdfs_dir))
        self.move(self.tmp_hdfs_dir, self.target_hdfs_dir)

    def repartition(self, df, num_parts):
        """Repartition prepared data before calling `write` method.

        :param df: dataframe prepared for export.
        :type df: :class:`pyspark.sql.DataFrame`
        :param int num_parts: required number of files.
        :return: repartitioned dataframe.
        :rtype: :class:`pyspark.sql.DataFrame`
        """
        return df.repartition(num_parts)

    @abc.abstractmethod
    def write(self, df, hdfs_dir):
        """Write prepared dataframe into specified HDFS directory in a required format.

        .. note:: Specified destination directory is considered as temporary, export is done with :meth:`move`.

        :param df: data frame prepared for export.
        :type df: :class:`pyspark.sql.DataFrame`
        :param str hdfs_dir: destination HDFS directory.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def move(self, src_hdfs_dir, dst_hdfs_dir):
        """Move saved data to it's destination.

        :param str src_hdfs_dir: source HDFS directory.
        :param str dst_hdfs_dir: destination HDFS directory.
        """
        raise NotImplementedError

    @staticmethod
    def is_skipped(filename):
        """Determine whether a file with a specified name should be skipped.

        :param str filename: a file name to check.
        :return: whether a file should be skipped.
        :rtype: bool
        """
        return (
            (filename == SUCCESS_FILENAME)
            or (".tmp" in filename)
            or filename.endswith(".crc")
            or filename.endswith("._COPYING_")
        )


class ExportHdfsDirBaseTask(ExportHdfsBaseTask):
    """ABC to export various data as a whole new HDFS directory at once."""

    def move(self, src_hdfs_dir, dst_hdfs_dir):
        self.hdfs.remove(dst_hdfs_dir, recursive=True, skip_trash=True, force=True)
        # Path without a trailing slash allows a whole directory rename.
        self.hdfs.move(os.path.normpath(src_hdfs_dir), os.path.normpath(dst_hdfs_dir))


class ExportHdfsFileBaseTask(ExportHdfsBaseTask):
    """ABC to export various data as files into a possibly existing and non-empty HDFS directory."""

    def move(self, src_hdfs_dir, dst_hdfs_dir):
        self.hdfs.mkdir(dst_hdfs_dir, parents=True, raise_if_exists=False, remove_if_exists=False)

        mv_pairs, num_files = [], 0

        for src_path in self.hdfs.listdir(src_hdfs_dir, ignore_directories=True):
            if not self.is_skipped(os.path.basename(src_path)):
                mv_pairs.append((src_path, os.path.join(dst_hdfs_dir, self.target_filename(num_files))))
                num_files += 1

        if num_files == 0:
            self.warn("There are no valid files to move from {}".format(src_hdfs_dir))
            return

        # Since the method is called within an active Spark session: it will be 'get'
        spark = SparkSession.builder.getOrCreate()

        errors = spark.sparkContext.parallelize(mv_pairs, numSlices=num_files).flatMap(self._mv).collect()

        if errors:
            raise RuntimeError("Move failed, got errors:\n{}".format(pformat(errors)))

    @abc.abstractmethod
    def target_filename(self, idx):
        """Return target filename for a given index according to some template.

        :param int idx: file index.
        :return: target filename.
        :rtype: str
        """
        raise NotImplementedError

    @staticmethod
    def _mv(pair):
        hdfs = HdfsClient()
        src, dst = pair
        try:
            hdfs.move(src, dst)
        except HDFSCliError as e:
            if not hdfs.exists(dst):
                yield e.message


class ExportFeaturesBaseTask(ExportHdfsDirBaseTask):
    """ABC for export features into HDFS directory, implements `extract_transform` method using OOP template pattern."""

    target_dt = luigi.Parameter(description="Export date")  # type: str
    features_subdir = luigi.Parameter(description="A unique HDFS sub-directory for a features set")  # type: str

    source_db = luigi.Parameter(description="Hive source database")  # type: str
    source_table = luigi.Parameter(description="Hive source table")  # type: str
    source_partitions = luigi.ListParameter(description="Selected partitions list")  # type: list

    export_columns = luigi.DictParameter(description="Output columns config")  # type: dict

    max_collection_size = luigi.IntParameter(description="Maximum number of items in one feature")  # type: int

    @property
    def target_hdfs_subdirs(self):
        return [self.features_subdir, self.target_dt]

    @property
    def features(self):
        # From luigi we receive unicode, but StructType can process only (str, int, slice) in __getitem__
        return [(to_str(f["name"]), f["expr"]) for f in self.export_columns["features"]]

    def output(self):
        return HdfsTarget(path=self.target_hdfs_dir, isdir=True)

    def requires(self):
        return HiveExternalTask(database=self.source_db, table=self.source_table, partitions=self.source_partitions)

    def extract_transform(self, sql_ctx):
        self.info("Loading table: {}".format(format_table(self.source_db, self.source_table, self.source_partitions)))

        df = sql_ctx.sql(
            select_clause(database=self.source_db, table=self.source_table, partition_dicts=self.source_partitions)
        )

        self.info("Aggregating by key and combining a feature ...")

        try:
            df = (
                self.filter_keys(df)
                .groupBy(*self.key_columns)
                .agg(*[sqlfn.expr(expr).alias(name) for name, expr in self.features])
            )
        except CapturedException as e:
            raise FatalStatusException("SQL exception: {}\n{}".format(e.desc, e.stackTrace))

        self.info("Validate feature data type ...")

        for feature_name, _ in self.features:
            feature_type = df.schema[feature_name].dataType

            if not self.validate_feature_type(feature_type):
                raise FatalStatusException(
                    "Unsupported feature `{}` data type `{}`".format(feature_name, feature_type.simpleString())
                )

        self.info("Filtering out invalid rows from {} ...".format(df.schema.simpleString()))
        return self.filter_features(df)

    @abc.abstractproperty
    def key_columns(self):
        """Return a list of :class:`pyspark.sql.Column` for groupBy operation.

        :rtype: list[:class:`pyspark.sql.Column`]
        """
        return NotImplementedError

    @staticmethod
    def validate_feature_type(feature_type):
        return (
            isinstance(feature_type, (StringType, IntegralType, FractionalType))
            or (
                isinstance(feature_type, ArrayType)
                and isinstance(feature_type.elementType, (StringType, IntegralType, FractionalType))
            )
            or (
                isinstance(feature_type, MapType)
                and isinstance(feature_type.keyType, (StringType, IntegralType))
                and isinstance(feature_type.valueType, (IntegralType, FractionalType))
            )
        )

    def filter_keys(self, df):
        self.info("Filtering out {} invalid values ...".format(self.key_columns))

        for key in self.key_columns:
            df = df.where(key.isNotNull())

        return df

    def filter_features(self, df):
        df = df.dropna()
        arr_filter = "size({col}) > 0 and size({col}) <= {maxlen} and not exists({col}, _x -> {invalid_elem})"

        for feature_name, _ in self.features:
            params = {"col": feature_name, "maxlen": self.max_collection_size}
            feature_type = df.schema[feature_name].dataType

            self.info("Filtering out `{}: {}` invalid values ...".format(feature_name, feature_type.simpleString()))

            if isinstance(feature_type, ArrayType):
                if isinstance(feature_type.elementType, (StringType, IntegralType)):
                    params["invalid_elem"] = "isnull(_x) or not is_uint32(cast(_x as string))"
                else:  # FractionalType
                    params["invalid_elem"] = "not isfinite(_x)"

                df = df.where(arr_filter.format(**params))

            elif isinstance(feature_type, MapType):
                params["col"] = "map_values({})".format(feature_name)
                params["invalid_elem"] = "not isfinite(_x)"

                df = (
                    df.withColumn(feature_name, sqlfn.expr("cast({} as map<string,float>)".format(feature_name)))
                    .where(arr_filter.format(**params))
                    .where("not exists(map_keys({}), _x -> not is_uint32(_x))".format(feature_name))
                )

            elif isinstance(feature_type, (StringType, IntegralType)):
                df = df.where("is_uint32(cast({} as string))".format(feature_name))

            else:  # FractionalType
                df = df.where("isfinite({})".format(feature_name))

        return df


class ExportFeaturesBaseApp(ControlApp):
    """ABC for export features apps, implements a search for source partitions."""

    def prepare_config(self, config):
        feature_names = [f["name"] for f in config["export_columns"]["features"]]

        if len(feature_names) != len(set(feature_names)):
            raise FatalStatusException("Feature name duplicate found, names: {}".format(pformat(feature_names)))

        max_dt = config["target_dt"]
        max_dt_diff = config.pop("max_dt_diff", None)

        source_partitions = self.partitions_finder.find(
            database=config["source_db"],
            table=config["source_table"],
            partition_conf=config.pop("source_partition_conf"),
            min_dt=None if max_dt_diff is None else add_days(max_dt, -max_dt_diff),
            max_dt=max_dt,
        )

        config["source_partitions"] = source_partitions or HiveExternalTask.MISSING
        return config
