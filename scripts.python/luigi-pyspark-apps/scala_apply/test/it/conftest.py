import os
import shutil
import pytest
import tempfile

from pyspark.sql import SparkSession

from .data import TABLES
from .service import HadoopEnvironment, HdfsClient, HiveMetastoreClient, HdfsTarget

__all__ = ["session_temp_dir", "lazy_spark"]


class Spark(object):
    parallelism = 1

    def __init__(self, lazy=True, session_dir="/tmp/spark_session_files"):
        self._spark = None
        self.lazy = lazy
        self.session_dir = session_dir
        if not lazy:
            self._spark = self._setup(self.parallelism, self.session_dir)

    def spark_session(self, force=False):
        if not self._spark:
            if not self.lazy or force:
                self._spark = self._setup(self.parallelism, self.session_dir)
        return self._spark

    @staticmethod
    def _setup(parallelism, session_dir):
        conf = {
            "spark.sql.shuffle.partitions": parallelism,
            "spark.default.parallelism": parallelism,
            "spark.driver.host": "localhost",
            "spark.ui.enabled": "false",
            "hive.exec.dynamic.partition.mode": "nonstrict",
            "hive.exec.dynamic.partition": "true",
            "spark.sql.warehouse.dir": os.path.join(session_dir, "spark-warehouse"),
            "spark.driver.extraJavaOptions": "-Dderby.system.home={}".format(os.path.join(session_dir, "derby"))
        }
        if "SPARK_JARS" in os.environ:
            conf["spark.jars"] = os.environ["SPARK_JARS"]

        spark = SparkSession.builder.master("local[{}]".format(parallelism))
        for k, v in conf.items():
            spark = spark.config(k, v)
        spark = spark.getOrCreate()

        return spark

    def stop(self):
        if self._spark:
            self._spark.stop()


@pytest.fixture(scope="session")
def session_temp_dir(request):
    temp_dir = tempfile.mkdtemp()
    print("\nsession_temp_dir fixture, created dir `{}`\n".format(temp_dir))
    yield temp_dir
    shutil.rmtree(temp_dir)


@pytest.fixture(scope="session")
def lazy_spark(request, session_temp_dir):
    spark_wrapper = Spark(lazy=True, session_dir=session_temp_dir)
    if hasattr(request, "cls"):
        request.cls.spark_wrapper = spark_wrapper
    yield spark_wrapper
    spark_wrapper.stop()


@pytest.fixture(scope="session", autouse=True)
def mocks_setup(monkeysession, lazy_spark):
    HadoopEnvironment.instance = HadoopEnvironment(
        lazy_spark,
        TABLES,
        ["local_db.features_table", "local_db.audience_table", "local_db.features_e2e"]
    )
    monkeysession.setattr("luigi.contrib.hdfs.target.hdfs_clients.get_autoconfig_client", HdfsClient)
    monkeysession.setattr("prj.adapters.apply.HdfsClient", HdfsClient)
    monkeysession.setattr("prj.apps.utils.control.client.HdfsTarget", HdfsTarget)
    monkeysession.setattr("prj.apps.utils.control.client.get_autoconfig_client", HdfsClient)
    monkeysession.setattr("prj.apps.utils.luigi_helpers.get_autoconfig_client", HdfsClient)
    monkeysession.setattr("prj.apps.apply.app.insert_into_hive", HadoopEnvironment.insert_into_hive)
    monkeysession.setattr("prj.apps.apply.app.HiveMetastoreClient", HiveMetastoreClient)
    monkeysession.setattr("prj.apps.learn.test.e2e.test_app.TestLearn.hdfs", HdfsClient())
    monkeysession.setattr("prj.apps.learn.app.HdfsClient", HdfsClient)
    monkeysession.setattr("prj.apps.learn.test.e2e.test_app.HdfsClient", HdfsClient)


@pytest.fixture(scope="function", autouse=True)
def mocks_setup_fun(monkeypatch, mocker):
    monkeypatch.setattr("prj.apps.utils.control.client.DebugControlClient.post", mocker.Mock(return_value=None))
