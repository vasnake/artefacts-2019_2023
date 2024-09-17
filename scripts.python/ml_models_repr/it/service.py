import os
import shutil

from pyspark.sql.types import StructType, StructField


class HdfsClient(object):
    """luigi.contrib.hdfs.hadoopcli_clients.HdfsClient mock class"""

    def get(self, from_path, to_path):
        print("\nHdfsClient.get, from `{}`, to `{}`\n".format(from_path, to_path))
        if os.path.isfile(from_path):
            shutil.copy(from_path, to_path)
        else:
            shutil.copytree(from_path, os.path.join(to_path, os.path.basename(from_path)))

    def put(self, from_path, to_path):
        self.get(from_path, to_path)

    def exists(self, path):
        return os.path.exists(path)

    def touchz(self, path):
        assert not self.exists(path)
        open(path, 'w').close()

    def mkdir(self, path):
        if not self.exists(path):
            os.makedirs(path)

    def remove(self, path, recursive=True, skip_trash=False):
        print("\nHdfsClient.remove, remove path `{}`".format(path))
        assert any([path.startswith(x) for x in ("/tmp", "/temp", "/var")])
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
        self.df = spark.createDataFrame(
            spark.sparkContext.parallelize(data[table]["rows"]),
            StructType([StructField(name, _type) for name, _type in data[table]["schema"]])
        )


class HadoopEnvironment(object):
    instance = None

    def __init__(self, spark_session, data, tables):
        self.spark_session = spark_session
        self.dataframes = {
            name: TableWrapper(name, data, self.spark_session)
            for name in tables
        }
        self._setup_db([tuple(t.split(".")) for t in tables])

    def _setup_db(self, tables):
        for db, table in tables:
            self.spark_session.sql("create database if not exists {}".format(db))
            self._create_table(db, table)

    def _create_table(self, db, table):
        name = "{}.{}".format(db, table)
        self.spark_session.sql("drop table if exists {}".format(name))
        wrapper = self.dataframes[name]
        self._write(wrapper, name)

    @staticmethod
    def _write(tab_wrapper, table_name):
        tab_wrapper.df.write.format("orc").mode("append").partitionBy(
            *tab_wrapper.partition_columns
        ).saveAsTable(table_name)

    def write_table(self, db, table, df):
        # only if table exists and have the same schema
        name = "{}.{}".format(db, table)
        wrapper = self.dataframes[name]
        assert len(df.schema.fields) == len(wrapper.df.schema.fields) and all(
            a.name == b.name and a.dataType == b.dataType for a, b in zip(
                sorted(df.schema.fields, key=lambda f: f.name), sorted(wrapper.df.schema.fields, key=lambda f: f.name)
            ))
        wrapper.df = df
        self._write(wrapper, name)

    def read_table(self, db, table):
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
