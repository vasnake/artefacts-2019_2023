import os
import shutil
import pandas as pd

from luigi.target import FileSystemTarget
from pyspark.sql.types import StructType, StructField


class HdfsTarget(FileSystemTarget):
    """luigi.contrib.hdfs.HdfsTarget"""

    def __init__(self, path=None, format=None, is_tmp=False, fs=None):
        super(HdfsTarget, self).__init__(path)
        self.path = path
        self._fs = HdfsClient()

    @property
    def fs(self):
        return self._fs

    def glob_exists(self, expected_files):
        raise NotImplementedError("glob_exists not ready yet")

    def open(self, mode='r'):
        if mode not in ('r', 'w'):
            raise ValueError("Unsupported open mode '%s'" % mode)
        return open(self.path, mode)


class HiveMetastoreClient(object):
    """dmgrinder.common.hive.HiveMetastoreClient mock"""

    class Field(object):
        def __init__(self, colname, coltype):
            self.name = colname
            self.type = self._decode(coltype)

        @staticmethod
        def _decode(coltype):
            from pyspark.sql.types import StringType, DoubleType, ArrayType
            if isinstance(coltype, StringType):
                return "string"
            if isinstance(coltype, DoubleType):
                return "double"
            if isinstance(coltype, ArrayType) and isinstance(coltype.elementType, DoubleType):
                return "array<double>"
            return coltype.__repr__()

    def table_schema(self, table, database='default', comments=False):
        if comments:
            raise NotImplementedError("schema with comments not available yet")
        else:
            return [(field_schema.name, field_schema.type)
                    for field_schema in self._get_schema(database, table)]

    def _get_schema(self, db, table):
        df = HadoopEnvironment.read_from_hive(db, table)
        return [self.Field(x.name, x.dataType) for x in df.schema.fields]


class HdfsClient(object):
    """luigi.contrib.hdfs.hadoopcli_clients.HdfsClient mock"""

    def isdir(self, path):
        return os.path.isdir(path)

    def listdir(self, path, ignore_directories=False, ignore_files=False,
                include_size=False, include_type=False, include_time=False, recursive=False):
        if os.path.isdir(path):
            for p in os.listdir(path):
                yield os.path.join(path, p)
            else:
                yield path

    # def get(self, from_path, to_path):
    def get(self, path, local_destination, force=False, content_only=False):
        print("\nHdfsClient.get, from `{}`, to `{}`\n".format(path, local_destination))
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
                    print("get, destination dir not exists but file exists? {}".format(self.exists(local_destination) and os.path.isfile(local_destination)))
                shutil.copytree(path, local_destination)
            else:
                shutil.copytree(path, os.path.join(local_destination, os.path.basename(path)))

    def put(self, local_path, destination, force=False, content_only=False):
        self.get(local_path, destination, content_only=content_only)

    def exists(self, path):
        return os.path.exists(path)

    def touchz(self, path):
        assert not self.exists(path)
        open(path, 'w').close()

    def mkdir(self, path, parents=True, raise_if_exists=False, remove_if_exists=False):
        if remove_if_exists:
            self.remove(path)
        if not self.exists(path):
            os.makedirs(path)

    def remove(self, path, recursive=True, skip_trash=False):
        print("\nHdfsClient.remove, remove path `{}`".format(path))
        assert any([path.startswith(x) for x in ("/tmp", "/temp", "/var", "/private/var")])
        try:
            if os.path.isfile(path):
                os.remove(path)
            else:
                shutil.rmtree(path)
        except OSError as e:
            print(e)
        print("HdfsClient.remove, done.\n")


class TableWrapper(object):

    def __init__(self, table, data, spark):
        self.partition_columns = data[table]["partitions"]
        self.df = None
        if spark is not None:
            self.df = spark.createDataFrame(
                spark.sparkContext.parallelize(data[table]["rows"]),
                StructType([StructField(name, _type) for name, _type in data[table]["schema"]])
            )


class HadoopEnvironment(object):
    instance = None

    def __init__(self, spark_wrapper, data, tables):
        self.spark_wrapper = spark_wrapper
        self.data = data
        self.tables = tables
        self.dataframes = {}

        spark = self.spark_wrapper.spark_session()
        if spark:
            self._setup(spark, data, tables)

    def _setup(self, spark, data, tables):
        self.dataframes = {
            name: TableWrapper(name, data, spark)
            for name in tables
        }
        self._setup_db(spark, [tuple(t.split(".")) for t in tables])

    def _setup_db(self, spark, tables):
        for db, table in tables:
            spark.sql("create database if not exists {}".format(db))
            self._create_table(spark, db, table)

    def _create_table(self, spark, db, table):
        name = "{}.{}".format(db, table)
        spark.sql("drop table if exists {}".format(name))
        wrapper = self.dataframes[name]
        self._write(wrapper, name, mode="append")

    @staticmethod
    def _write(tab_wrapper, table_name, mode="append"):
        tab_wrapper.df.write.format("orc").mode(mode).partitionBy(
            *tab_wrapper.partition_columns
        ).saveAsTable(table_name)

    def _force_delayed_setup(self):
        spark = self.spark_wrapper.spark_session()
        if not spark:
            spark = self.spark_wrapper.spark_session(force=True)
            self._setup(spark, self.data, self.tables)

    def write_table(self, db, table, df, mode="append"):
        """write only if table exists and have the same schema"""
        self._force_delayed_setup()
        name = "{}.{}".format(db, table)
        wrapper = self.dataframes[name]
        assert len(df.schema.fields) == len(wrapper.df.schema.fields) and all(
            a.name == b.name and a.dataType == b.dataType for a, b in zip(
                sorted(df.schema.fields, key=lambda f: f.name), sorted(wrapper.df.schema.fields, key=lambda f: f.name)
            ))
        wrapper.df = df.cache()
        self._write(wrapper, name, mode)

    def empty_table(self, db, table):
        self._force_delayed_setup()
        wrapper = self.dataframes["{}.{}".format(db, table)]
        self.write_table(db, table, wrapper.df.where("1=0"), mode="overwrite")

    def read_table(self, db, table):
        self._force_delayed_setup()
        wrapper = self.dataframes["{}.{}".format(db, table)]
        return wrapper.df

    @staticmethod
    def insert_into_hive(df, database, table, max_rows_per_bucket, overwrite=True):
        assert HadoopEnvironment.instance
        HadoopEnvironment.instance.write_table(database, table, df)

    @staticmethod
    def read_from_hive(database, table):
        assert HadoopEnvironment.instance
        return HadoopEnvironment.instance.read_table(database, table)


def print_csr_matrix(x, prefix):
    from scipy.sparse.csr import csr_matrix
    from scipy.sparse.csc import csc_matrix
    import pandas as pd
    assert isinstance(x, (csr_matrix, csc_matrix))
    print("\n{}, x type: {}, shape: {}, data:\n{}\nnnz: `{}`\nindices: `{}`\nindptr: `{}`\n".format(
        prefix, type(x), x.shape, x.data.tolist(),
        x.nnz, x.indices.tolist(), x.indptr.tolist()
    ))
    # print(pd.DataFrame(data=x.toarray()))


def parse_df(text):
    def values(line):
        return [x.strip() for x in line.split("|") if x.strip()]

    def convert_data_type(name, value):
        if name in {"score", "scores_raw", "scores_trf"}:
            return name, eval(value)
        return name, value

    def row(names, values):
        return dict([convert_data_type(name, value) for name, value in zip(names, values)])

    lines = text.strip().split("\n")
    return [row(values(lines[1]), values(lines[i])) for i in range(3, len(lines)-1)]


def show_pandas_df(df, debug=False):
    """Print pandas df as a nice table, spark df.show-like.
    Copy-paste from SO.
    """
    assert isinstance(df, pd.DataFrame)
    if debug:
        print(df.to_string(index=False))
    df_columns = df.columns.tolist()
    max_len_in_lst = lambda lst: len(sorted(lst, reverse=True, key=len)[0])
    align_center = lambda st, sz: "{0}{1}{0}".format(" "*(1+(sz-len(st))//2), st)[:sz] if len(st) < sz else st
    align_right = lambda st, sz: "{0}{1} ".format(" "*(sz-len(st)-1), st) if len(st) < sz else st
    max_col_len = max_len_in_lst(df_columns)
    max_val_len_for_col = dict([(col, max_len_in_lst(df.iloc[:,idx].astype('str'))) for idx, col in enumerate(df_columns)])
    col_sizes = dict([(col, 2 + max(max_val_len_for_col.get(col, 0), max_col_len)) for col in df_columns])
    build_hline = lambda row: '+'.join(['-' * col_sizes[col] for col in row]).join(['+', '+'])
    build_data = lambda row, align: "|".join([align(str(val), col_sizes[df_columns[idx]]) for idx, val in enumerate(row)]).join(['|', '|'])
    hline = build_hline(df_columns)
    out = [hline, build_data(df_columns, align_center), hline]
    for _, row in df.iterrows():
        out.append(build_data(row.tolist(), align_right))
    out.append(hline)
    print("\n".join(out))
