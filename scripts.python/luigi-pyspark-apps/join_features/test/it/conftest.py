# flake8: noqa
# fmt: off

import shutil
import tempfile

import pytest

from prj.apps.export.test.it.service import (
    HdfsClient,
    HdfsTarget,
    HadoopEnvironment,
    HiveMetastoreClient,
    GrinderUDFLibraryMock,
)

__all__ = ["session_temp_dir", "hadoop_env", "mocks_session", "mocks_function"]


@pytest.fixture(scope="session")
def session_temp_dir(request):
    print("\nSession temp dir ...")
    temp_dir = tempfile.mkdtemp()
    print("session_temp_dir fixture, created dir `{}`\n".format(temp_dir))
    yield temp_dir
    print("\nSession temp dir, remove `{}` ...".format(temp_dir))
    shutil.rmtree(temp_dir)


@pytest.fixture(scope="session")
def hadoop_env(request, session_temp_dir):
    print("\nSession hadoop env ...")
    he = HadoopEnvironment(session_temp_dir)
    print("\nhadoop_env fixture, created env `{}`\n".format(he.__repr__()))
    yield he
    print("\nSession hadoop env, closing ...")
    he.close()


@pytest.fixture(scope="session", autouse=True)
def mocks_session(monkeysession, hadoop_env):
    print("\nSession mocks ...")

    monkeysession.setattr("prj.apps.utils.control.luigix.task.CustomUDFLibrary", CustomUDFLibraryMock)
    monkeysession.setattr("prj.apps.joiner.features.app.CustomUDFLibrary", CustomUDFLibraryMock)

    monkeysession.setattr("prj.apps.utils.control.luigix.task.HdfsClient", HdfsClient)
    monkeysession.setattr("prj.apps.utils.common.fs.hdfs.HdfsClient", HdfsClient)
    monkeysession.setattr("prj.apps.utils.common.fs.HdfsClient", HdfsClient)
    monkeysession.setattr("prj.apps.utils.common.spark.HdfsClient", HdfsClient)
    monkeysession.setattr("prj.apps.utils.testing.tools.HdfsClient", HdfsClient)

    monkeysession.setattr("prj.common.hive.HiveMetastoreClient", HiveMetastoreClient)
    monkeysession.setattr("prj.apps.utils.common.hive.HiveMetastoreClient", HiveMetastoreClient)
    monkeysession.setattr("prj.apps.utils.common.hive.FindPartitionsEngine.hive_client", HiveMetastoreClient())
    monkeysession.setattr("prj.apps.utils.common.luigix.target.HiveGenericTarget.hive_client", HiveMetastoreClient())
    monkeysession.setattr("prj.apps.joiner.features.app.HiveMetastoreClient", HiveMetastoreClient)

    monkeysession.setattr("prj.apps.utils.common.spark.insert_into_hive", HadoopEnvironment.insert_into_hive)
    monkeysession.setattr("prj.apps.joiner.features.app.insert_into_hive", HadoopEnvironment.insert_into_hive)

    monkeysession.setattr("luigi.contrib.hive.run_hive_cmd", HadoopEnvironment.run_hive_cmd)
    monkeysession.setattr("prj.apps.joiner.features.app.run_hive_cmd", HadoopEnvironment.run_hive_cmd)

    monkeysession.setattr("prj.apps.joiner.features.app.create_hive_table_ddl", HadoopEnvironment.create_hive_table_ddl)

    print("session mocks done.")
    return


@pytest.fixture(scope="function", autouse=True)
def mocks_function(monkeypatch, mocker):
    print("\nFunction mocks ...")
    monkeypatch.setattr("prj.apps.utils.control.client.DebugControlClient.post", mocker.Mock(return_value=None))
    print("function mocks done.")