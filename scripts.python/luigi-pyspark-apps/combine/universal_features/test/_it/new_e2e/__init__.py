"""Mock objects"""

# flake8: noqa

import os
import glob
import shutil

from pprint import pprint as pp
from pprint import pformat
from contextlib import contextmanager

from pyspark.sql.types import (  # noqa: F401
    MapType,
    ArrayType,
    FloatType,
    DoubleType,
    StringType,
    StructType,
    IntegerType,
    StructField,
)


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


class HiveMetastoreClient(object):
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

    def get_partitions(self, table, database="default"):
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


HIVE_METASTORE_CLIENT = HiveMetastoreClient()


def get_hive_metastore_client():
    return HIVE_METASTORE_CLIENT
