# Experiments with exceptions as Control signals

# flake8: noqa

import os
import dill
import copy
import json
import luigi

from pprint import pformat

import six
import numpy as np
import pandas as pd
import pytest
import pandas.testing as pdt

from prj.apps.utils.control.client.status import (
    FatalStatusException,
    MissingDepsStatusException,
)

from pyspark.sql.utils import (AnalysisException, CapturedException,)


class TestSignals(object):

    def test_dump_load(self, session_temp_dir):
        signal_file_path = os.path.join(session_temp_dir, "signal.dill")
        exception = AnalysisException(desc="Some stupid error", stackTrace="foo bar")
        exception_str = "{}: {}".format(type(exception), str(exception))  # repr is undefined for CapturedException
        # exception_str = "{}".format(exception)

        print("\nOriginal exception: `{}`".format(exception_str))

        signal = FatalStatusException(exception_str)
        print("\nSignal repr: `{}`, str: `{}`".format(repr(signal), str(signal)))

        with open(signal_file_path, "w") as f:
            dill.dump(signal, f)

        with open(signal_file_path, "r") as f:
            _signal = dill.load(f)

        print("\nLoaded signal repr: `{}`, str: `{}`".format(repr(_signal), str(_signal)))


class ControlClient(object):
    def send_results(self, url, data, overwrite=True):
        print("ControlClient.send_results,\n\turl `{}`,\n\tdata `{}`".format(url, data))


class TestReportRunsExceptions(object):
    """
    ControlClient.send_results,
        url `/tmp/grinder/joiner/features/42/output1`,
        data `{'type': 'exception', 'name': 42, 'value': None}`

    ControlClient.send_results,
        url `/tmp/grinder/joiner/features/42/output2`,
        data `{'type': 'exception', 'name': 42, 'value': 'prj.apps.utils.control.client.exception.FatalStatusException'}`

    ControlClient.send_results,
        url `/tmp/grinder/joiner/features/42/output3`,
        data `{'type': 'exception', 'name': 42, 'value': 'prj.apps.utils.control.client.exception.FailedStatusException'}`
    """

    def test_report_runs_exception(self):
        from pyspark.sql.utils import AnalysisException
        print("\n`exceptions` are: None, Exception class, Exception instance, iterable of any from above\n")
        try:
            # happy path

            # exceptions = None
            # exceptions = FatalStatusException  # class
            # exceptions = FailedStatusException("foo")  # instance
            # exceptions = AnalysisException
            # raise FailedStatusException("bar")
            # raise AnalysisException("WTF", "stacktrace")
            # exceptions = [None, FatalStatusException]
            # exceptions = (FatalStatusException, FailedStatusException("baz"))
            # exceptions = {FailedStatusException("foo"), None}
            # exceptions = [None, FailedStatusException("bar")]

            # wrong args
            # exceptions = ControlClient
            # exceptions = 33
            # exceptions = u"some error"
            # exceptions = self
            # exceptions = [None]  # wrong size
            # exceptions = [None, ControlClient]
            # exceptions = [33, None]
            # exceptions = ["error", None]
            exceptions = [None, self]
        except Exception as exceptions:
            print("oops, {}".format(exceptions))
        finally:
            report_runs_exception(self, exceptions)

    def debug(self, message):
        print("DEBUG TestReportRunsExceptions, {}".format(message))

    def info(self, message):
        print("INFO TestReportRunsExceptions, {}".format(message))

    def warn(self, message):
        print("WARN TestReportRunsExceptions, {}".format(message))

    @property
    def task_id(self):
        return 42

    @property
    def output_urls(self):
        return (
            "/tmp/grinder/joiner/features/{}/output1".format(self.task_id),
            "/tmp/grinder/joiner/features/{}/output2".format(self.task_id),
        )

    @property
    def control_client(self):
        return ControlClient()


def report_runs_exception(self, exceptions, log=True):
    # exceptions are: None, Exception class, Exception instance, iterable of any from above

    import traceback
    import inspect
    from dmcore.utils.common import is_iterable
    from prj.adapters.utils import get_location

    self.debug("report_runs_exception, task {} reported errors {} {}".format(
        self, type(exceptions), repr(exceptions))
    )

    def _check_type(exception):
        return (
                exception is None
                or (inspect.isclass(exception) and issubclass(exception, BaseException))
                or isinstance(exception, BaseException)
        )

    if exceptions is None:
        self.debug("Error is None")
        if log:
            self.info("Task {} reported no errors".format(self.task_id))
        exceptions = [None] * len(self.output_urls)

    elif inspect.isclass(exceptions):
        self.debug("Error passed as class")
        if issubclass(exceptions, BaseException):
            if log:
                self.warn(
                    "Task {} reported failure: {}".format(self.task_id, repr(exceptions))
                )
            exceptions = [get_location(exceptions)] * len(self.output_urls)
        else:
            raise ValueError("Unknown `exceptions` class, expect subclass of Exception, got {}".format(
                repr(exceptions))
            )

    elif isinstance(exceptions, BaseException):
        self.debug("Error passed as Exception instance")
        if log:
            self.warn(
                "Task {} reported failure: {}, traceback: {}".format(
                    self.task_id, repr(exceptions), traceback.format_exc()
                )
            )
        exceptions = [get_location(exceptions)] * len(self.output_urls)

    elif is_iterable(exceptions):
        # N.B. generator drain here
        # N.B. FatalStatusException is iterable
        if all(_check_type(e) for e in exceptions):
            if log:
                self.warn("Task {} reported collection of failures: {}".format(
                    self.task_id, [repr(e) for e in exceptions])
                )
            exceptions = [e if e is None else get_location(e) for e in exceptions]
        else:
            raise ValueError(
                "Unknown `exceptions` type, expect iterable of: "
                "None or Exception class or Exception instance, got `{}`, value `{}`".format(
                    type(exceptions), repr(exceptions)
                )
            )

    else:
        raise ValueError(
            "Unknown `exceptions` type, expect None or Exception or iterable, got `{}`, value `{}`".format(
                type(exceptions), repr(exceptions)
            )
        )

    if len(exceptions) != len(self.output_urls):
        raise ValueError(
            "`exceptions` amount differs from output_urls: {} vs {}".format(
                len(exceptions), len(self.output_urls)
            )
        )

    for exception, url in zip(exceptions, self.output_urls):
        data = {"type": "exception", "name": str(self.task_id), "value": exception}
        self.debug("Sending {} to: {}".format(data, url))
        self.control_client.send_results(url, data, overwrite=True)
