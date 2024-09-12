import shutil
import tempfile

import pytest

from prj.apps.export.test.it.service import (  # noqa: F401
    HdfsClient,
    HdfsTarget,
    HadoopEnvironment,
    HiveMetastoreClient,
    CustomUDFLibraryMock
)

__all__ = ["session_temp_dir", "hadoop_env", "mocks_session", "mocks_function"]


@pytest.fixture(scope="session")
def session_temp_dir(request):
    temp_dir = tempfile.mkdtemp()
    print("\nsession_temp_dir fixture, created dir `{}`\n".format(temp_dir))
    yield temp_dir
    shutil.rmtree(temp_dir)


@pytest.fixture(scope="session")
def hadoop_env(request, session_temp_dir):
    he = HadoopEnvironment(session_temp_dir)
    print("\nhadoop_env fixture, created env `{}`\n".format(he.__repr__()))
    yield he
    he.close()


@pytest.fixture(scope="session", autouse=True)
def mocks_session(monkeysession, hadoop_env):
    monkeysession.setattr("prj.apps.utils.control.luigix.task.CustomUDFLibrary", CustomUDFLibraryMock)
    monkeysession.setattr("prj.apps.utils.control.luigix.task.HdfsClient", HdfsClient)
    monkeysession.setattr("prj.apps.utils.common.fs.hdfs.HdfsClient", HdfsClient)
    monkeysession.setattr("prj.apps.utils.common.fs.HdfsClient", HdfsClient)
    monkeysession.setattr("prj.apps.utils.common.spark.HdfsClient", HdfsClient)

    monkeysession.setattr("prj.apps.export.base.HdfsClient", HdfsClient)
    monkeysession.setattr("prj.apps.export.base.ExportHdfsBaseTask.hdfs", HdfsClient())

    monkeysession.setattr("prj.apps.utils.common.hive.HiveMetastoreClient", HiveMetastoreClient)
    monkeysession.setattr("prj.apps.utils.common.hive.FindPartitionsEngine.hive_client", HiveMetastoreClient())

    monkeysession.setattr("prj.apps.export.audience.base.HiveMetastoreClient", HiveMetastoreClient)
    monkeysession.setattr("prj.apps.export.audience.base.ExportAudienceBaseApp.hive_client", HiveMetastoreClient())

    return


@pytest.fixture(scope="function", autouse=True)
def mocks_function(monkeypatch, mocker):
    monkeypatch.setattr("prj.apps.utils.control.client.DebugControlClient.post", mocker.Mock(return_value=None))
