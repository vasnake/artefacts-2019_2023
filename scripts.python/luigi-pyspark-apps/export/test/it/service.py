import os
import six
import glob
import shutil
import copy
import json
import luigi

import numpy as np

from pprint import pprint as pp
from pprint import pformat
from contextlib import contextmanager

from pyspark.sql import functions as sqlfn

from pyspark.sql import SparkSession
from luigi.target import FileSystemTarget
from pyspark.conf import SparkConf
from pyspark.sql.types import (  # noqa: F401
    MapType,
    LongType,
    ArrayType,
    FloatType,
    DoubleType,
    StringType,
    StructType,
    IntegerType,
    StructField,
)

from spark_utils import CustomUDFLibrary


def create_spark_session(session_dir, conf_update=None, hive_support=False, parallelism=None):
    print("\nSpark session builder ...")

    if not parallelism:
        parallelism = os.environ.get("PYTEST_SPARK_PARALLELISM", 1)

    conf = {
        "spark.hadoop.mapred.output.compress": "true",
        "spark.hadoop.mapred.output.compression.codec": "org.apache.hadoop.io.compress.GzipCodec",
        "spark.driver.host": "localhost",
        "spark.master": "local[{}]".format(parallelism),
        "spark.default.parallelism": parallelism,
        "spark.sql.shuffle.partitions": parallelism,
        "spark.ui.enabled": "false",
        "hive.exec.dynamic.partition": "true",
        "hive.exec.dynamic.partition.mode": "nonstrict",
        "spark.sql.warehouse.dir": os.path.join(session_dir, "spark-warehouse"),
        "spark.driver.extraJavaOptions": "-Dderby.system.home={}".format(os.path.join(session_dir, "derby")),
    }
    if "SPARK_JARS" in os.environ:
        conf["spark.jars"] = os.environ["SPARK_JARS"]
    if hive_support:  # spark.enableHiveSupport()
        conf["spark.sql.catalogImplementation"] = "hive"

    if conf_update:
        conf.update(**conf_update)

    print("\ncreate_spark_session, conf: {}".format(pformat(conf)))
    spark = SparkSession.builder.config(conf=SparkConf().setAll(conf.items())).getOrCreate()

    if "ADD_JAR" in os.environ:
        for jar in os.environ["ADD_JAR"].split(","):
            spark.sql("ADD JAR {}".format(jar))

    return spark


class TableWrapper(object):
    # TODO: fix constructor signature, unwrap parameters into named params, e.g.
    #  (database, table, schema, partition_columns, rows, df)
    def __init__(self, table, data, spark=None):
        def _fix(row, schema):
            if isinstance(row, dict):
                return [row.get(name) for name, _type in schema]
            else:  # tuple or list
                assert len(row) == len(schema)
                return row

        self.table = table
        self.partition_columns = data[table]["partitions"]
        self.schema = data[table]["schema"]
        self.rows = [_fix(row, self.schema) for row in data[table]["rows"]]
        self.df = None
        self.create_df(spark)

    def create_df(self, spark):
        if spark is not None:
            self.df = _persist(self._create_df(spark, self.rows, self.schema))

    @staticmethod
    def _create_df(spark, rows, schema):
        return spark.createDataFrame(
            spark.sparkContext.parallelize(rows),
            StructType([StructField(name, _type) for name, _type in schema]),
        )

    def set_df(self, df, set_rows=True):
        # TODO: check that df.schema == self.schema

        def _row(row):
            return [row[name] for name, _ in self.schema]

        self.rows = []
        self.df = df

        if set_rows:
            self.rows = [_row(row.asDict()) for row in self.df.collect()]

        return self


class HdfsClient(object):
    """prj.apps.utils.common.fs.HdfsClient mock."""

    @contextmanager
    def open(self, path, mode="r"):
        if mode == "r" and not self.exists(path):
            raise IOError("No such file or directory: '{}'".format(path))
        with open(path, mode) as fd:
            yield fd

    def rename(self, path, dest):
        return self.move(path, dest)

    def move(self, old_path, new_path, raise_if_exists=True, verbose=True):
        """Result should depend on `new_path` format, if new_path ends with `/`, it means that new_path is a directory
        and old_path will be moved into it. E.g. `a -> b/` and result will be `b/a`. If new_path NOT ends with a `/`, it
        will be considered as a new name for old_path. E.g. `a -> b` and result will be `b`.

        :param old_path:
        :param new_path:
        :param raise_if_exists:
        :param verbose:
        :return:
        """
        if verbose:
            print("\nHdfsClient.move, from `{}` to\n`{}`\n".format(old_path, new_path))
            if os.path.isdir(old_path):
                pp(("src content", list(self.listdir(old_path))))
            if os.path.isdir(new_path):
                pp(("dst content", list(self.listdir(new_path))))

        if raise_if_exists and os.path.exists(new_path):
            print("\nCan't move to existing target: `{}`".format(new_path))
            raise ValueError("Destination exists: %s" % new_path)
        d = os.path.dirname(new_path)
        if d and not os.path.exists(d):
            self.mkdir(d)
        try:
            os.rename(old_path, new_path)
        except OSError as e:
            msg = "\nOSError: {} {}\n".format(e.errno, e)
            print(msg)
            raise ValueError(msg)
        if verbose:
            pp(("moving done, result files", list(self.listdir(new_path))))

    def isdir(self, path):
        return os.path.isdir(path)

    def listdir(
        self,
        path,
        ignore_directories=False,
        ignore_files=False,
        include_size=False,
        include_type=False,
        include_time=False,
        recursive=False,
    ):
        # TODO: should return list of full paths
        files = []
        if os.path.exists(path):
            if os.path.isdir(path):
                files = [os.path.join(path, f) for f in os.listdir(path)]
                print("listdir, dir path: `{}`".format(pformat(files)))
            else:
                files.append(path)
                print("listdir, file path: `{}`".format(pformat(files)))
        elif "*" in path:
            files = list(glob.glob(path))
            print("listdir, glob found: `{}`".format(pformat(files)))

        for fp in files:
            if ignore_directories and os.path.isdir(fp):
                print("listdir, ignore dir: `{}`".format(fp))
            else:
                yield fp

    # def get(self, from_path, to_path):
    def get(self, path, local_destination, force=False, content_only=False):
        print("\nHdfsClient.get, from `{}` to `{}`\n".format(path, local_destination))
        if os.path.isfile(path):
            shutil.copy(path, local_destination)
        else:
            if content_only:
                # print("get, content only")
                if self.exists(local_destination) and os.path.isdir(local_destination):
                    # print("get, destination exists, it's a dir: {}".format(list(self.listdir(local_destination))))
                    if len(list(self.listdir(local_destination))) <= 1:
                        # print("get, empty destination exists, try to remove ...")
                        self.remove(local_destination)
                    else:
                        print("get, destination dir not empty")
                else:
                    print(
                        "get, destination dir not exists but file exists? {}".format(
                            self.exists(local_destination) and os.path.isfile(local_destination)
                        )
                    )
                shutil.copytree(path, local_destination)
            else:
                shutil.copytree(path, os.path.join(local_destination, os.path.basename(path)))

    def put(self, local_path, destination, force=False, content_only=False):
        self.get(local_path, destination, content_only=content_only)

    def exists(self, path):
        return os.path.exists(path)

    def touchz(self, path):
        print("touchz: `{}`".format(path))
        assert not self.exists(path)
        open(path, "w").close()

    def mkdir(self, path, parents=True, raise_if_exists=False, remove_if_exists=False):
        print("mkdir: `{}`".format(path))
        if remove_if_exists:
            self.remove(path)
        if not self.exists(path):
            os.makedirs(path)

    def remove(self, path, recursive=True, skip_trash=False, force=False):
        def remove_by_pattern(pattern):
            for f in glob.glob(pattern):
                print("removing `{}`".format(f))
                os.remove(f)

        print("\nHdfsClient.remove, removing path `{}`".format(path))
        assert any([path.startswith(x) for x in ("/tmp", "/temp", "/var", "/private/var")])
        try:
            if os.path.isdir(path):
                shutil.rmtree(path)
            elif "*" in path:
                remove_by_pattern(path)
            else:
                os.remove(path)
        except OSError as e:
            print("\nOSError: {}\n".format(e))
        print("HdfsClient.remove, done.\n")


class HadoopEnvironment(object):
    _instance = None
    hdfs = HdfsClient()

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            print("\nCreating the HadoopEnvironment object ...\n")
            cls._instance = super(HadoopEnvironment, cls).__new__(cls)
            cls._instance._new = True
        return cls._instance

    def __init__(self, temp_dir="/tmp/dmgrinder/test/hadoop/env"):
        if not self._new:
            return
        print("\nHadoop env init, temp_dir `{}` ...".format(temp_dir))
        self._new = False
        self.temp_dir = temp_dir
        self._spark = None
        self.data = {}
        self.tables = []
        self.dataframes = {}

    @property
    def spark(self):
        if self._spark:
            return self._spark
        spark = create_spark_session(self.temp_dir, hive_support=True)
        # hdfs:/data/dm/lib/brickhouse-0.7.1-SNAPSHOT.jar
        spark.sql("DROP TEMPORARY FUNCTION IF EXISTS user_dmdesc_collect")
        spark.sql("CREATE TEMPORARY FUNCTION user_dmdesc_collect AS 'brickhouse.udf.collect.CollectUDAF'")
        self._spark = spark
        return self._spark

    def custom_spark(self, conf, hive_support=True, parallelism=None):
        if self._spark is not None:
            raise RuntimeError("Spark session created already")
        spark = create_spark_session(
            self.temp_dir, conf_update=conf, hive_support=hive_support, parallelism=parallelism
        )
        self._spark = spark
        return self._spark

    def close(self):
        if self._spark:
            self._spark.stop()
            self._spark = None

    def add_data(self, data, tables):
        for table in tables:
            self.data[table] = data[table]
            if table not in self.tables:
                self.tables.append(table)
            self.dataframes[table] = TableWrapper(table, data, self.spark)
        self._setup_db([tuple(t.split(".")) for t in tables])

    def _setup_db(self, tables):
        for db, table in tables:
            self.spark.sql("create database if not exists {}".format(db))
            # org.apache.hadoop.hive.metastore.RetryingHMSHandler - ERROR - AlreadyExistsException(message:Database db_e2e_test already exists)
            # try:
            #     self.spark.sql("create database if not exists {}".format(db))
            # except Exception as e:
            #     if "Database {} already exists".format(db) not in str(e):
            #         raise
            self._create_table(db, table)

    def _create_table(self, db, table):
        name = "{}.{}".format(db, table)
        self.spark.sql("drop table if exists {}".format(name))
        wrapper = self.dataframes[name]
        self._write(wrapper, name, mode="append")

    @staticmethod
    def _write(wrapper, table_name=None, mode="append"):
        if not table_name:
            table_name = wrapper.table

        print("\nAppend to Hive: `{}`".format(table_name))
        wrapper.df.printSchema()
        wrapper.df.show(100, truncate=False)

        (
            wrapper.df.write
            .format("hive")  # .format("orc") won't work after executing `create table ... `
            .mode(mode)
            .partitionBy(*wrapper.partition_columns)
            .saveAsTable(table_name)
        )

        print("\nAppend to Hive, written {} rows.".format(wrapper.df.count()))

        wrapper.set_df(
            df=_persist(wrapper.df.sql_ctx.sql("select * from {}".format(table_name))),
            set_rows=True
        )

        return wrapper

    def write_table(self, db, table, df, mode="append", fix_schema=False):
        """write only if table exists and have the same schema."""
        name = "{}.{}".format(db, table)
        if name in self.dataframes:
            wrapper = self.dataframes[name]
            old_df = wrapper.df  # old_df is None if table is joiner_features target
            print("\nwrite_table: old_df `{}`".format(old_df))
        else:
            raise ValueError("Can't find df wrapper, table `{}` was created not using HadoopEnvironment!".format(name))

        if old_df is None:
            old_df = self.spark.sql("select * from {}".format(name))

        print("\nwrite_table: writing {} to {}".format(df.schema.simpleString(), old_df.schema.simpleString()))

        if fix_schema:
            df = df.select(*[
                sqlfn.col(column.name) if column.name in df.columns else sqlfn.lit(None).cast(column.dataType).alias(column.name)
                for column in old_df.schema.fields
            ])
            print("\nwrite_table: fixed schema {}".format(df.schema.simpleString()))

        assert len(df.schema.fields) == len(old_df.schema.fields) and all(
            a.name == b.name and a.dataType == b.dataType
            for a, b in zip(
                sorted(df.schema.fields, key=lambda f: f.name), sorted(old_df.schema.fields, key=lambda f: f.name)
            )
        )

        wrapper.set_df(df, set_rows=False)
        self.dataframes[name] = self._write(wrapper, name, mode)

    def empty_table(self, db, table):
        wrapper = self.dataframes["{}.{}".format(db, table)]
        self.write_table(db, table, wrapper.df.where("1=0"), mode="overwrite")

    def read_table(self, db, table):
        wrapper = self.dataframes["{}.{}".format(db, table)]
        return wrapper.df

    @staticmethod
    def insert_into_hive(
            df,
            database,
            table,
            max_rows_per_bucket,
            overwrite=True,
            raise_on_missing_columns=True,
            check_parameter=None,
            jar="hdfs:/lib/dm/grinder-transformers-assembly-1.7.1.jar",
    ):
        print("\ninsert_into_hive, {} into {}.{}, raise_on_missing_columns: {}".format(
            df.schema.simpleString(), database, table, raise_on_missing_columns
        ))
        he = HadoopEnvironment()
        he.write_table(database, table, df, fix_schema=(not raise_on_missing_columns))
        print("insert_into_hive, done.\n")

        show_spark_df(
            df=he.spark.sql("select * from {}.{}".format(database, table)),
            msg="Hive table `{}.{}`".format(database, table)
        )

    @staticmethod
    def read_from_hive(database, table):
        he = HadoopEnvironment()
        return he.read_table(database, table)

    @staticmethod
    def create_hive_table_ddl(database, table, fields, partition_columns, stored_as="ORC", raise_if_exists=False):
        from prj.apps.utils.common.hive import create_hive_table_ddl as _ddl
        ddl = _ddl(database, table, fields, partition_columns)

        # register new table in HE
        he = HadoopEnvironment()
        name = "{}.{}".format(database, table)
        _data = {
            "partitions": [p for p in partition_columns],
            "schema": [
                (name_type_comment[0], HiveMetastoreClient.Field.encode(name_type_comment[1]))
                for name_type_comment in fields
            ],
            "rows": []
        }
        he.dataframes[name] = TableWrapper(table=name, data={name: _data})

        print("\ncreate_hive_table_ddl: `{}`\n".format(ddl))
        return ddl

    @staticmethod
    def run_hive_cmd(hivecmd, check_return_code=True):
        he = HadoopEnvironment()
        try:
            he.spark.sql(hivecmd)
        except Exception as e:
            if check_return_code:
                raise
            else:
                print("\nrun_hime_cmd failed, error: {}\nargs: {}\n".format(e, hivecmd))
        return ""


class HdfsTarget(FileSystemTarget):
    """luigi.contrib.hdfs.HdfsTarget."""

    def __init__(self, path=None, format=None, is_tmp=False, fs=None):
        super(HdfsTarget, self).__init__(path)
        self.path = path
        self._fs = HdfsClient()

    @property
    def fs(self):
        return self._fs

    def glob_exists(self, expected_files):
        raise NotImplementedError("glob_exists not ready yet")

    def open(self, mode="r"):
        if mode not in ("r", "w"):
            raise ValueError("Unsupported open mode '%s'" % mode)
        return open(self.path, mode)

    def exists(self):
        print("\nHdfsTarget, exists({})".format(self.path))
        return self.fs.exists(self.path)


class HdfsFlagTarget(HdfsTarget):
    # from luigi.contrib.hdfs import HdfsFlagTarget
    def __init__(self, path, format=None, client=None, flag="_SUCCESS"):
        assert path[-1] == "/"
        super(HdfsFlagTarget, self).__init__(path + flag, format)
        self.path = path
        self.flag = flag

    def exists(self):
        hadoopSemaphore = self.path + self.flag
        return self.fs.exists(hadoopSemaphore)


class HiveMetastoreClient(object):
    """prj.common.hive.HiveMetastoreClient mock."""

    class Field(object):
        def __init__(self, colname, coltype):
            self.name = colname
            self.type = self.decode(coltype)

        @staticmethod
        def encode(coltype):
            mapping = {
                "string": StringType(),
                "float": FloatType(),
                "double": DoubleType(),
                "int": IntegerType(),
                "long": LongType(),
                "array<float>": ArrayType(FloatType()),
                "array<double>": ArrayType(DoubleType()),
                "array<string>": ArrayType(StringType()),
                "map<string,float>": MapType(StringType(), FloatType()),
                "map<string,double>": MapType(StringType(), DoubleType()),
                "map<int,double>": MapType(IntegerType(), DoubleType()),
                "map<string,string>": MapType(StringType(), StringType()),
            }
            return mapping[coltype.replace(" ", "").lower()]

        @staticmethod
        def decode(coltype):
            if isinstance(coltype, StringType):
                return "string"
            if isinstance(coltype, DoubleType):
                return "double"
            if isinstance(coltype, ArrayType) and isinstance(coltype.elementType, DoubleType):
                return "array<double>"
            if isinstance(coltype, MapType):
                if isinstance(coltype.keyType, StringType):
                    if isinstance(coltype.valueType, StringType):
                        return "map<string,string>"
            raise TypeError("Unknown spark col type: `{}`".format(coltype.__repr__()))

    def table_schema(self, table, database="default", comments=False):
        if comments:
            raise NotImplementedError("schema with comments not available yet")
        else:
            return [(field_schema.name, field_schema.type) for field_schema in self._get_schema(database, table)]

    def get_columns(self, table, database="default"):
        return [field_schema.name for field_schema in self._get_schema(database, table)]

    def get_partition_names(self, table, database="default"):
        name = "{}.{}".format(database, table)
        catalog = HadoopEnvironment().dataframes
        wrapper = catalog.get(name)
        if not wrapper:
            print("Unknown table `{}` in catalog {}".format(name, HadoopEnvironment().dataframes))
            return []
        return wrapper.partition_columns

    def get_partitions(self, table, database='default', filter_expr=None, check_parameter=None):
        he = HadoopEnvironment()
        fqn_table_name = "{}.{}".format(database, table)
        wrapper = he.dataframes[fqn_table_name]
        partition_names = self.get_partition_names(table, database)
        all_names = [k for k, _ in wrapper.schema]
        rows = [zip(all_names, row) for row in wrapper.rows]

        partitions = [
            tuple(
                (k, v)
                for k, v in r
                if k in partition_names
            )
            for r in rows
        ]

        print("\nHMC.get_partitions, all part. tuples: {}\n".format(pformat(partitions)))
        return [dict(r) for r in set(partitions)]

    def table_exists(self, table, database='default', partition=None):
        try:
            partitions = self.get_partitions(table, database)
        except KeyError as e:
            partitions = None

        if partition is None:
            return partitions is not None
        else:
            return partition in partitions

    def _get_schema(self, db, table):
        df = HadoopEnvironment.read_from_hive(db, table)
        return [self.Field(x.name, x.dataType) for x in df.schema.fields]


class SimpleHiveMetastoreClient(object):
    """prj.common.hive.HiveMetastoreClient mock."""

    _spark = None
    _catalog = {"db.table": {"partition_columns": ["dt", "uid_type"], "schema": [("dt", "string")]}}

    def set_spark(self, spark):
        self._spark = spark
        return self

    def set_catalog(self, catalog):
        self._catalog = catalog
        return self

    class Field(object):
        def __init__(self, colname, coltype):
            self.name = colname
            self.type = self.decode(coltype)

        @staticmethod
        def encode(coltype):
            mapping = {
                "string": StringType(),
                "float": FloatType(),
                "int": IntegerType(),
                "double": DoubleType(),
                "array<float>": ArrayType(FloatType()),
                "array<double>": ArrayType(DoubleType()),
                "array<string>": ArrayType(StringType()),
                "map<string,float>": MapType(StringType(), FloatType()),
                "map<int,double>": MapType(IntegerType(), DoubleType()),
            }
            return mapping[coltype.replace(" ", "").lower()]

        @staticmethod
        def decode(coltype):
            if isinstance(coltype, StringType):
                return "string"
            if isinstance(coltype, DoubleType):
                return "double"
            if isinstance(coltype, ArrayType) and isinstance(coltype.elementType, DoubleType):
                return "array<double>"
            raise TypeError("Unknown spark col type: `{}`".format(coltype.__repr__()))

    def table_schema(self, table, database="default", comments=False):
        print("\nHMC table_schema for {}.{}".format(database, table))
        if comments:
            raise NotImplementedError("schema with comments not available yet")
        else:
            return [(field.name, field.type) for field in self._get_schema(database, table)]

    def get_partition_names(self, table, database="default"):
        name = "{}.{}".format(database, table)
        print("\nHMC get_partition_names for `{}`".format(name))
        wrapper = self._catalog.get(name)
        if not wrapper:
            raise AttributeError("Unknown table `{}` in catalog {}".format(name, pformat(self._catalog)))
        return wrapper.get("partition_columns", [])

    def get_partitions(self, table, database='default', filter_expr=None, check_parameter=None):
        name = "{}.{}".format(database, table)
        print("\nHMC get_partitions for `{}`".format(name))
        p_names = self.get_partition_names(table, database)
        if not p_names:
            raise AttributeError("Table {} doesn't have partition columns".format(name))
        df = self._spark.sql("select distinct {} from {}".format(", ".join(p_names), name))
        # fmt: off
        partitions = [
            tuple([(n, row[n],) for n in p_names])
            for row in df.collect()
        ]
        # fmt: on
        return [dict(r) for r in set(partitions)]

    def _get_schema(self, db, table):
        df = self._spark.sql("select * from {}.{} where 1=0".format(db, table))
        return [self.Field(x.name, x.dataType) for x in df.schema.fields]


class GrinderUDFLibraryMock(GrinderUDFLibrary):
    def __init__(self, spark, jar="hdfs:/lib/dm/grinder-transformers-assembly-1.7.1.jar", log_url=None):
        super(GrinderUDFLibraryMock, self).__init__(spark, jar=None, log_url=log_url)
        if jar:
            if HdfsClient().exists(jar):
                self.info("Loading jar `{}` ...".format(jar))
                spark.sql("ADD JAR {}".format(jar))
            else:
                self.warn("Jar `{}` doesn't exist.".format(jar))


class HiveDataIO(object):

    def log(self, msg):
        print("\ntest.it.service.HiveDataIO, {}".format(msg))

    def __init__(self, database, table):
        self.database = database
        self.table = table
        self.schema = None
        self.partition_columns = None
        self.fq_table_name = "{}.{}".format(database, table)

    def drop_table(self):
        self.log("drop_table {}".format(self.fq_table_name))
        return self

    def set_schema(self, schema, partition_columns=None):
        from dmcore.utils.common import make_list
        self.log("set_schema, table {}, schema {}, partition_columns {}".format(self.fq_table_name, pformat(schema), pformat(partition_columns)))
        self.schema = schema
        self.partition_columns = make_list(partition_columns)
        return self

    def create_table(self):
        self.log("create_table {}".format(self.fq_table_name))
        return self

    def insert_data(self, lines, tmp_hdfs_dir):
        self.log("insert_data, table {}, lines {}, tmp_hdfs_dir {}".format(self.fq_table_name, pformat(lines), tmp_hdfs_dir))
        he = HadoopEnvironment()
        td = {"db": self.database, "table": self.table, "schema": self.schema, "partition_columns": self.partition_columns, "data": lines}
        data = e2e_data_to_it(**td)
        he.add_data(data, data.keys())
        return self

    def collect_data(self, partitions, tmp_hdfs_dir, sort_by=None):
        self.log("collect_data, table {}, partitions {}, sort_by {}".format(self.fq_table_name, pformat(partitions), sort_by))
        data = []
        return data


def _persist(df):
    from pyspark.storagelevel import StorageLevel
    return df.persist(StorageLevel.DISK_ONLY)
    # return df.persist(StorageLevel.MEMORY_ONLY)


def fq_table_name(database, table):
    return "{}.{}".format(database, table)


def show_spark_df(df, msg="Spark DataFrame", lines=100, truncate=False):
    """Print df as table, with metadata.

    :param df: pyspark.sql.DataFrame
    :type df: :class:`pyspark.sql.DataFrame`
    :param msg: title
    :param lines: lines to show
    :param truncate: truncate wide lines if True
    :return: pyspark.sql.DataFrame
    :rtype: :class:`pyspark.sql.DataFrame`
    """
    from pyspark.storagelevel import StorageLevel

    df.explain(extended=True)
    _df = df.persist(StorageLevel.MEMORY_ONLY)
    # _df = df.persist(StorageLevel.DISK_ONLY)
    count = _df.count()
    num_parts = _df.rdd.getNumPartitions()
    print("\n# {}\nrows: {}, partitions: {}".format(msg, count, num_parts))

    _df.printSchema()

    _df.show(n=lines, truncate=truncate)

    return _df.unpersist()


def show_pandas_df(df, debug=False):
    """Print pandas df as a nice table, spark df.show-like.

    Copy-paste from SO.
    """
    # assert isinstance(df, pd.DataFrame)
    if debug:
        print(df.to_string(index=False))
    df_columns = df.columns.tolist()
    max_len_in_lst = lambda lst: len(sorted(lst, reverse=True, key=len)[0])  # noqa: E731
    align_center = (
        lambda st, sz: "{0}{1}{0}".format(" " * (1 + (sz - len(st)) // 2), st)[:sz] if len(st) < sz else st
    )  # noqa: E731
    align_right = lambda st, sz: "{0}{1} ".format(" " * (sz - len(st) - 1), st) if len(st) < sz else st  # noqa: E731
    max_col_len = max_len_in_lst(df_columns)
    max_val_len_for_col = dict(
        [(col, max_len_in_lst(df.iloc[:, idx].astype("str"))) for idx, col in enumerate(df_columns)]
    )
    col_sizes = dict([(col, 2 + max(max_val_len_for_col.get(col, 0), max_col_len)) for col in df_columns])
    build_hline = lambda row: "+".join(["-" * col_sizes[col] for col in row]).join(["+", "+"])  # noqa: E731
    build_data = lambda row, align: "|".join(  # noqa: E731
        [align(str(val), col_sizes[df_columns[idx]]) for idx, val in enumerate(row)]
    ).join(["|", "|"])
    hline = build_hline(df_columns)
    out = [hline, build_data(df_columns, align_center), hline]
    for _, row in df.iterrows():
        out.append(build_data(row.tolist(), align_right))
    out.append(hline)
    print("\n".join(out))


def e2e_data_to_it(**hive_table_descr):
    def _norm(v, t=None):
        if isinstance(v, str) and v == "":
            return None
        elif isinstance(v, float) and np.isnan(v):
            return None
        elif isinstance(v, int) and t in ["double", "float"]:
            return float(v)
        elif isinstance(v, list):
            return [_norm(x) for x in v]
        elif isinstance(v, dict):
            return {k: _norm(x) for k, x in six.iteritems(v)}
        return v

    def _values(row, schema):
        values = [_norm(row.get(colname), coltype) for colname, coltype in schema]
        return tuple(values)

    return {
        "{}.{}".format(hive_table_descr["db"], hive_table_descr["table"]): {
            "partitions": hive_table_descr["partition_columns"],
            "schema": [(n, HiveMetastoreClient.Field.encode(t)) for n, t in hive_table_descr["schema"]],
            "rows": [_values(row, hive_table_descr["schema"]) for row in hive_table_descr["data"]],
        }
    }


def fix_control_urls_path(task_conf, temp_dir, hadoop_env):
    def _temp_path(*dirs):
        return os.path.join(temp_dir, *dirs)

    fixed_task_conf = copy.deepcopy(task_conf)

    for param_name in ("input_urls", "output_urls", "status_urls",):
        hadoop_env.hdfs.mkdir(_temp_path(param_name))
        fixed_task_conf[param_name] = [
            _temp_path(param_name, "_{}".format(i))
            for i, _ in enumerate(task_conf[param_name], 1)
        ]

    return fixed_task_conf


HIVE_METASTORE_CLIENT = HiveMetastoreClient()


def get_hive_metastore_client():
    return HIVE_METASTORE_CLIENT
