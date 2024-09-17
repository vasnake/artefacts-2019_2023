import os

from luigi.contrib.hdfs import get_autoconfig_client, HDFSCliError

from prj.logs import LoggingMixin


class AbstractStorage(LoggingMixin):

    def get(self, from_path, to_path):
        """Download remote content to local dir.
        Return two lists, successfully downloaded and failed names respectively.

        Local directory must exists and should be empty. If local file already exists warning will be logged.

        Remote content could be a directory or file. In case of directory, its listdir contents will be downloaded to
        local dir.
        """
        raise NotImplementedError("get files from remote path to local path")

    def put(self, from_path, to_path):
        """Upload local content to remote dir.
        Return two lists, successfully uploaded and failed names respectively.

        Remote directory must exists and should be empty. If file already exits warning will be logged.

        Local content could be a directory or file. In case of directory, its listdir contents will be uploaded to
        remote directory.
        """
        raise NotImplementedError("put files from local path to remote path")

    def remove_mkdir(self, path, recursive=True, skip_trash=True):
        """Sequence of remove-if-exists and mkdir"""
        raise NotImplementedError("remove dir if exists then create")


class HdfsStorage(AbstractStorage):

    def __init__(self, hdfs_client=None):
        if hdfs_client is None:
            self._hdfs = get_autoconfig_client()
        else:
            self._hdfs = hdfs_client  # luigi.contrib.hdfs.abstract_client.HdfsFileSystem

    def remove_mkdir(self, path, recursive=True, skip_trash=True):
        if self._hdfs.exists(path):
            self._hdfs.remove(path, recursive=recursive, skip_trash=skip_trash)
        self._hdfs.mkdir(path)

    def get(self, from_path, to_path):
        if not (os.path.exists(to_path) and os.path.isdir(to_path)):
            raise ValueError("Destination path must be an existing directory")

        success, error = [], []
        for p in self._hdfs.listdir(from_path):
            if self._get(p, to_path):
                success.append(p)
            else:
                error.append(p)

        return success, error

    def put(self, from_path, to_path):
        success, error = [], []
        if os.path.isdir(from_path):
            for p in os.listdir(from_path):
                if self._put(os.path.join(from_path, p), to_path):
                    success.append(p)
                else:
                    error.append(p)
        else:
            if self._put(from_path, to_path):
                success.append(from_path)
            else:
                error.append(from_path)

        return success, error

    def _get(self, _from, _to):
        try:
            self.debug("download from `{}` to '{}'".format(_from, _to))
            self._hdfs.get(_from, _to)
            return True
        except HDFSCliError as e:
            self.warn("Download error: {}".format(e))
            return False

    def _put(self, _from, _to):
        try:
            self.debug("upload from `{}` to '{}'".format(_from, _to))
            self._hdfs.put(_from, _to)
            return True
        except HDFSCliError as e:
            self.warn("Upload error: {}".format(e))
            return False
