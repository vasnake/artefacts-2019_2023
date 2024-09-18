from pprint import pformat
from operator import itemgetter

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

from prj.logs import LoggingMixin
from prjcore.utils.common import make_list
from prj.apps.utils.common.hive import select_clause


class TableOperation(object):
    def __init__(self, database, table):
        if not (database and table):
            raise AttributeError("You have to set all parameters: database and table")
        self.database = database
        self.table = table


class DropTable(TableOperation):
    def __init__(self, database, table):
        super(DropTable, self).__init__(database, table)

    def __repr__(self):
        return "DropTable('{}', '{}')".format(self.database, self.table)


class CreateTable(TableOperation):
    def __init__(self, database, table, schema, partition_columns):
        super(CreateTable, self).__init__(database, table)
        if not (schema and partition_columns):
            raise AttributeError("Set all parameters: schema, partition_columns")
        self.schema = schema
        self.partition_columns = partition_columns

    def __repr__(self):
        return "CreateTable('{}', '{}', {}, {})".format(self.database, self.table, self.schema, self.partition_columns)


class InsertData(TableOperation):
    def __init__(self, database, table, schema, partition_columns, rows):
        super(InsertData, self).__init__(database, table)
        if not (schema and partition_columns and rows):
            raise AttributeError("Set all parameters: schema, partition_columns, rows")
        self.schema = schema
        self.partition_columns = partition_columns
        self.rows = rows

    def __repr__(self):
        return "InsertData('{}', '{}', {}, {}, {})".format(
            self.database, self.table, self.schema, self.partition_columns, self.rows
        )


class CollectData(TableOperation):
    def __init__(self, database, table, partitions, sort_by):
        super(CollectData, self).__init__(database, table)
        self.partitions = partitions
        self.sort_by = sort_by

    def __repr__(self):
        return "CollectData('{}', '{}', {}, {})".format(self.database, self.table, self.partitions, self.sort_by)


class HiveDataIOSpark(LoggingMixin):
    def __init__(self, database, table):
        self.database = database
        self.table = table
        self.schema = []
        self.partition_columns = []
        self.rows = []
        self.operations = []

    def set_schema(self, schema, partition_columns=None):
        self.schema = schema
        self.partition_columns = make_list(partition_columns)
        return self

    def create_table(self):
        self.info(
            "Create table: database={}, table={}, schema={}, partition_columns={} ...".format(
                self.database, self.table, self.schema, self.partition_columns
            )
        )
        self.operations.append(CreateTable(self.database, self.table, self.schema, self.partition_columns))
        self.info("creating table {}.{} scheduled".format(self.database, self.table))
        return self

    def drop_table(self):
        self.info("Drop table: database={}, table={} ...".format(self.database, self.table))
        self.operations.append(DropTable(self.database, self.table))
        self.info("dropping table {}.{} scheduled".format(self.database, self.table))
        return self

    def insert_data(self, rows, tmp_hdfs_dir=None):
        self.rows = rows
        self.info("Insert data: database={}, table={}, {} rows ...".format(self.database, self.table, len(rows)))
        self.operations.append(InsertData(self.database, self.table, self.schema, self.partition_columns, rows))
        self.info("inserting data into table {}.{} scheduled".format(self.database, self.table))
        return self

    def collect_data(self, partitions, tmp_hdfs_dir=None, sort_by=None):
        self.info(
            "Collect data: database={}, table={}, partitions={}, sort_by={} ...".format(
                self.database, self.table, partitions, sort_by
            )
        )
        self.operations.append(CollectData(self.database, self.table, partitions, sort_by))
        self.info("collecting data from table {}.{} scheduled".format(self.database, self.table))
        return self

    def commit(self, spark):
        while self.operations:
            op = self.operations.pop(0)
            self.info("Committing {} ...".format(op.__repr__()[:250]))
            self._commit(op, spark)
            self.info("operation {} done.".format(op.__repr__()[:250]))
        return self

    def _commit(self, operation, spark):

        if isinstance(operation, DropTable):
            name = "{}.{}".format(operation.database, operation.table)
            spark.sql("DROP TABLE IF EXISTS {}".format(name)).collect()
            return self

        if isinstance(operation, CreateTable):
            name = "{}.{}".format(operation.database, operation.table)
            spark.sql("CREATE DATABASE IF NOT EXISTS {}".format(operation.database)).collect()
            # fmt: off
            df = self._create_df([(n, self._hive_to_spark_coltype(t), ) for n, t in operation.schema], [], spark,)
            # fmt: on
            self._write(name, df, operation.partition_columns, mode="overwrite")
            return self

        if isinstance(operation, InsertData):
            name = "{}.{}".format(operation.database, operation.table)
            # fmt: off
            df = self._create_df(
                [(n, self._hive_to_spark_coltype(t), ) for n, t in operation.schema],
                operation.rows,
                spark
            )
            # fmt: on
            df.write.insertInto("{}.{}".format(operation.database, operation.table), overwrite=True)
            # self._write(name, df, operation.partition_columns, mode="append")
            return self

        if isinstance(operation, CollectData):
            df = spark.sql(select_clause(operation.database, operation.table, partition_dicts=operation.partitions))
            self.rows = [row.asDict() for row in df.collect()]
            if operation.sort_by:
                self.rows = sorted(self.rows, key=itemgetter(*operation.sort_by), reverse=False)
            return self

        raise NotImplementedError("Operation {}".format(operation.__repr__()))

    def _write(self, table_name, df, partition_columns, mode="append"):
        self.debug(
            "df.saveAsTable: {}, mode {}, partition_columns {}".format(table_name, mode, pformat(partition_columns))
        )
        df.write.format("orc").mode(mode).partitionBy(*partition_columns).saveAsTable(table_name)

    @staticmethod
    def _create_df(schema, rows, spark):
        rows = [{n: r.get(n) for n, _ in schema} for r in rows]
        return spark.createDataFrame(
            # spark.sparkContext.parallelize(rows),
            rows,
            StructType([StructField(name, _type) for name, _type in schema]),
        )

    @staticmethod
    def _hive_to_spark_coltype(coltype):
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
