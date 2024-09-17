import os
import shutil
import pytest
import tempfile

from pyspark.sql import SparkSession

from .data import TABLES
from .service import HadoopEnvironment, HdfsClient


@pytest.fixture(scope="session")
def session_temp_dir(request):
    temp_dir = tempfile.mkdtemp()
    print("\nsession_temp_dir fixture, created dir `{}`\n".format(temp_dir))
    yield temp_dir
    shutil.rmtree(temp_dir)


@pytest.fixture(scope="session")
def local_spark(request):
    tmp_local_dir = tempfile.mkdtemp()
    request.addfinalizer(lambda: shutil.rmtree(tmp_local_dir))

    conf = {
        "spark.driver.host": "localhost",
        "spark.ui.enabled": "false",
        "spark.sql.shuffle.partitions": 2,
        "hive.exec.dynamic.partition.mode": "nonstrict",
        "hive.exec.dynamic.partition": "true",
        "spark.sql.warehouse.dir": os.path.join(tmp_local_dir, "spark-warehouse"),
        "spark.driver.extraJavaOptions": "-Dderby.system.home={}".format(os.path.join(tmp_local_dir, "derby"))
    }
    if "SPARK_JARS" in os.environ:
        conf["spark.jars"] = os.environ["SPARK_JARS"]

    spark = SparkSession.builder.master("local[2]")
    for k, v in conf.items():
        spark = spark.config(k, v)

    spark = spark.getOrCreate()
    if hasattr(request, "cls"):
        request.cls.spark = spark

    request.addfinalizer(lambda: spark.stop())
    return spark


@pytest.fixture(scope="session", autouse=True)
def mocks_setup(monkeysession, local_spark):
    HadoopEnvironment.instance = HadoopEnvironment(
        local_spark,
        TABLES,
        ["local_db.features_table", "local_db.audience_table"]
    )

    monkeysession.setattr("prj.apps.apply.app.insert_into_hive", HadoopEnvironment.insert_into_hive)
    monkeysession.setattr("luigi.contrib.hdfs.target.hdfs_clients.get_autoconfig_client", HdfsClient)
    monkeysession.setattr("prj.adapters.dataset.get_autoconfig_client", HdfsClient)
    monkeysession.setattr("prj.adapters.apply.get_autoconfig_client", HdfsClient)
    monkeysession.setattr("prj.apps.utils.luigi_helpers.get_autoconfig_client", HdfsClient)
    monkeysession.setattr("prj.apps.utils.control.client.get_autoconfig_client", HdfsClient)
    monkeysession.setattr("prj.apps.learn.test.e2e.test_app.TestLearn.hdfs", HdfsClient())
    monkeysession.setattr("prj.apps.learn.app.get_autoconfig_client", HdfsClient)
