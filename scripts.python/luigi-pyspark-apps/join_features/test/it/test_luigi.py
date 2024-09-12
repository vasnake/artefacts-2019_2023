# Experiments with Luigi task life-cicle

# flake8: noqa

import os
import dill
import copy
import json
import luigi
import logging

from pprint import pformat

import six
import numpy as np
import pandas as pd
import pytest
import pandas.testing as pdt

from dmcore.utils.common import is_iterable

from prj.apps.utils.control.client.status import STATUS

from prj.apps.utils.control.client.status import (
    FatalStatusException,
    MissingDepsStatusException,
)

SUBTASK_COUNT = 1


class MissingDepsStatus(object):
    def __init__(self, message):
        self.message = message


class SomeMeta(type):
    def some_meta_method(self):
        print("SomeMeta.some_meta_method, {}".format(self))


class SomeBase(object):
    def some_base_method(self):
        print("SomeBase.some_base_method, {}".format(self))


class SomeApp(six.with_metaclass(SomeMeta, SomeBase)):
    pass


class ControlClient(object):
    def send_results(self, url, data, overwrite=True):
        print("ControlClient.send_results,\n\turl `{}`,\n\tdata `{}`".format(url, data))

    def get_results(self, url):
        return TestLuigiWrapper.storage.get(url, [])


def compute_status(client, output_urls, luigi_run_result, logger=None):
    # from prj.apps.utils.control.luigix.meta import ControlTaskMeta
    # return ControlTaskMeta._compute_task_statuses(client, output_urls, luigi_run_result, logger)
    # SomeWrapperTask, __init__, task_id `/var/folders/fw/fdtn3vwd53x__f6xk4fkhzb80000gp/T/tmp73S5Vx`
    worker = luigi_run_result.worker
    task_history = worker._add_task_history

    for(task, status, ext) in task_history:
        print("task: `{}`, status: `{}`, external: `{}`".format(task.task_id, status, ext))
        # task: `/var/folders/fw/fdtn3vwd53x__f6xk4fkhzb80000gp/T/tmpBQqhJK`, status: `PENDING`, external: `True`
        # task: `/var/folders/fw/fdtn3vwd53x__f6xk4fkhzb80000gp/T/tmpBQqhJK/0`, status: `PENDING`, external: `True`
        # task: `LocalFsExternalTask__var_folders_fw__6ed7d18c64`, status: `PENDING`, external: `False`

    successful_tasks = {task for (task, status, ext) in task_history if status == 'DONE'}
    for task in successful_tasks:
        print("SUCCESS: task: `{}`".format(task.task_id))
        # task: `/var/folders/fw/fdtn3vwd53x__f6xk4fkhzb80000gp/T/tmp5Rzvy6`, status: `PENDING`, external: `True`
        # task: `/var/folders/fw/fdtn3vwd53x__f6xk4fkhzb80000gp/T/tmp5Rzvy6/0`, status: `PENDING`, external: `True`
        # task: `LocalFsExternalTask__var_folders_fw__99e68be7a4`, status: `DONE`, external: `False`
        # task: `/var/folders/fw/fdtn3vwd53x__f6xk4fkhzb80000gp/T/tmp5Rzvy6/0`, status: `DONE`, external: `None`
        # task: `/var/folders/fw/fdtn3vwd53x__f6xk4fkhzb80000gp/T/tmp5Rzvy6`, status: `DONE`, external: `None`
        # SUCCESS: task: `/var/folders/fw/fdtn3vwd53x__f6xk4fkhzb80000gp/T/tmp5Rzvy6/0`
        # SUCCESS: task: `LocalFsExternalTask__var_folders_fw__99e68be7a4`
        # SUCCESS: task: `/var/folders/fw/fdtn3vwd53x__f6xk4fkhzb80000gp/T/tmp5Rzvy6`
    assert len(successful_tasks) > 0

    summary = luigi.execution_summary._summary_dict(worker)
    for k, v in six.iteritems(summary):
        print("{}: {}".format(k, v))
        # still_pending_ext: set([])
        # upstream_failure: set([])
        # upstream_missing_dependency: set([])
        # failed: set([])
        # completed: set([WorkUnderWrapperTask(kvs={"base_path": "/var/folders/fw/fdtn3vwd53x__f6xk4fkhzb80000gp/T/tmpCxuoGQ", "idx": 0}), SomeWrapperTask(base_path=/var/folders/fw/fdtn3vwd53x__f6xk4fkhzb80000gp/T/tmpCxuoGQ)])
        # ever_failed: set([])
        # still_pending_not_ext: set([])
        # scheduling_error: set([])
        # upstream_run_by_other_worker: set([])
        # not_run: set([])
        # already_done: set([LocalFsExternalTask(path=/var/folders/fw/fdtn3vwd53x__f6xk4fkhzb80000gp/T/tmpCxuoGQ/0/input)])
        # run_by_other_worker: set([])
        # upstream_scheduling_error: set([])
    successful_tasks = summary["completed"].union(summary["already_done"])
    for task in successful_tasks:
        print("SUCCESS: task: `{}`".format(task.task_id))
        # SUCCESS: task: `LocalFsExternalTask__var_folders_fw__4b57fbdb5a`
        # SUCCESS: task: `/var/folders/fw/fdtn3vwd53x__f6xk4fkhzb80000gp/T/tmp3TrbZe/0`
        # SUCCESS: task: `/var/folders/fw/fdtn3vwd53x__f6xk4fkhzb80000gp/T/tmp3TrbZe`
    assert len(successful_tasks) > 0


class TestLuigiWrapper(object):
    """Wrapper task and sub-task communication experiments."""

    storage = {
        "/tmp/grinder/joiner/features/42/output1": [
            {
                "type": "exception", "name": "some_task_id",
                "value": ["pyspark.sql.utils.AnalysisException", None]
            }
        ],
        "/tmp/grinder/joiner/features/42/output2": [
            {
                "type": "exception", "name": "some_task_id",
                "value": ["prj.apps.utils.control.client.exception.FatalStatusException", None, "pyspark.sql.utils.AnalysisException"]
            }
        ],
        "/tmp/grinder/joiner/features/42/output3": [
            {
                "type": "exception", "name": "some_task_id_2",
                "value": [None, None]
            }
        ]
    }

    @property
    def output_urls(self):
        task_id = 42
        return (
            "/tmp/grinder/joiner/features/{}/output1".format(task_id),
            "/tmp/grinder/joiner/features/{}/output2".format(task_id),
            "/tmp/grinder/joiner/features/{}/output3".format(task_id),
        )

    def test_wrapper(self, session_temp_dir):

        def iter_check():
            try:
                raise FatalStatusException("Invalid task output")
            except BaseException as err:
                # FatalStatusException('Invalid task output',) is ITERABLE!
                if is_iterable(err):
                    print("is_iterable, {} is ITERABLE!".format(repr(err)))
                else:
                    print("is_iterable, {} is NOT iterable!".format(repr(err)))

                try:
                    iterator = iter(err)
                except TypeError:
                    print("iter, {} is NOT iterable!".format(repr(err)))
                else:
                    print("iter, {} is ITERABLE!".format(repr(err)))

        def meta_check():
            app = SomeApp()
            app.some_meta_method()
            app.some_base_method()

        def ser_des_check():
            # prj.adapters.utils.get_location(obj) => path
            # prj.adapters.utils.locate_cls(path) => cls
            # prj.adapters.utils.construct(dict) => obj
            # prj.adapters.utils.deconstruct(obj) => dict

            from prj.adapters.utils import get_location, locate_cls, construct, deconstruct
            from prj.apps.utils.control.client.status import MissingDepsStatusException
            from prj.apps.utils.common import unfreeze_json_param

            print(construct({"foo": "bar", "baz": 42}))
            print(construct([{"foo": "bar"}]))
            print(construct(None))
            luigi_param = luigi.ListParameter().parse("""["1", "2"]""")
            luigi_param = luigi.DictParameter().parse("""{"1":2}""")
            print(construct(unfreeze_json_param(luigi_param)))
            print(construct(luigi_param))
            print(unfreeze_json_param(None))
            print(unfreeze_json_param([1, 2]))
            print(unfreeze_json_param({1: 2}))
            partitions = MissingDepsStatusException("something missing")
            partitions = MissingDepsStatusException()
            dumpable = deconstruct(partitions)
            print(json.dumps(dumpable))
            print(json.dumps(partitions, default=lambda x: json.dumps(deconstruct(x))))
            reconstructed = construct(dumpable)
            print(reconstructed)

            # partitions = MissingDepsStatus("oops")
            # print(json.dumps(partitions, default=lambda x: json.dumps(deconstruct(x))))
            # dumpable = deconstruct(partitions)
            # print(json.dumps(dumpable))
            # reconstructed = construct(dumpable)

        # ser_des_check()
        # meta_check()
        # iter_check()
        # assert 0, "break"

        self._create_input(session_temp_dir)
        # self._create_output(session_temp_dir)

        print("\n\ncall luigi.interface.build ...")

        # patch luigi/interface/_schedule_and_run
        # from .luigi_interface import _schedule_and_run_patched
        # luigi.interface._schedule_and_run = _schedule_and_run_patched

        luigi_run_result = luigi.interface.build(
            tasks=[SomeWrapperTask(base_path=session_temp_dir)],
            detailed_summary=True,
            local_scheduler=True,
            workers=1,
        )

        print("\n\nluigi DAG result, type: {}, repr: {}".format(type(luigi_run_result), pformat(luigi_run_result)))
        # statuses = compute_status(self.control_client, self.output_urls, luigi_run_result, logger=self)
        # assert statuses == [STATUS.FAILED, STATUS.FATAL, STATUS.SUCCESS]

    def _create_input(self, base_dir):
        self._create_target(base_dir, "input")

    def _create_output(self, base_dir):
        self._create_target(base_dir, "output")

    def _create_target(self, base_dir, target):
        for i in range(SUBTASK_COUNT):
            try:
                os.makedirs(os.path.join(base_dir, str(i)))
            except OSError as e:
                print(e)
            with open(os.path.join(base_dir, str(i), target), "wb") as f:
                dill.dump(self, f)

    @property
    def control_client(self):
        return ControlClient()

    def debug(self, message):
        print("DEBUG TestLuigiWrapper, {}".format(message))

    def info(self, message):
        print("INFO TestLuigiWrapper, {}".format(message))

    def warn(self, message):
        print("WARN TestLuigiWrapper, {}".format(message))


class SomeWrapperTask(luigi.WrapperTask):

    base_path = luigi.Parameter(description="root dir for task files")  # type: str

    def __init__(self, *args, **kwargs):
        self.log("__init__, args: {}, kwargs {}".format(pformat(args), pformat(kwargs)))
        super(SomeWrapperTask, self).__init__(*args, **kwargs)
        self.log("__init__, after super.__init__")
        self.task_id = self.base_path
        self._register_callbacks()
        self.log("__init__, task_id `{}`".format(self.task_id))

    def _register_callbacks(self):
        self.log("_register_callbacks ...")

        # exception in `WorkUnderWrapperTask.run` method
        def _failure_callback(*args, **kwargs):
            self.log("FAILURE event,\n\targs: {};\n\tkwargs: {}".format(pformat(args), pformat(kwargs)))
            task, exception = args
            self.log("failed task `{}` with exception `{}`".format(task, exception))

        WorkUnderWrapperTask.event_handler(luigi.Event.FAILURE)(_failure_callback)

        # missing dependency in `WorkUnderWrapperTask`
        def _missing_deps_callback(*args, **kwargs):
            self.log("DEPENDENCY_MISSING event,\n\targs: {};\n\tkwargs: {}".format(pformat(args), pformat(kwargs)))
            self.log("external dependency tasks:\n\t{}".format("\n\t".join("`{}`".format(task) for task in args)))

        luigi.Task.event_handler(luigi.Event.DEPENDENCY_MISSING)(_missing_deps_callback)

        # exception in `WorkUnderWrapperTask.requires`
        def _requires_err_callback(*args, **kwargs):
            self.log("BROKEN_TASK event,\n\targs: {};\n\tkwargs: {}".format(pformat(args), pformat(kwargs)))
            task, exception = args
            self.log("failed task `{}` with exception `{}`".format(task, exception))

        WorkUnderWrapperTask.event_handler(luigi.Event.BROKEN_TASK)(_requires_err_callback)

        # experiments with custom callbacks

        def _done_callback(*args, **kwargs):
            # luigi/interface
            # def _schedule_and_run(tasks, worker_scheduler_factory=None, override_defaults=None):
            # added two lines before exit from func
            # for task in tasks:
            #     task.trigger_event("custom.events.DONE", task, luigi_run_result)
            self.log("custom.events.DONE event,\n\targs: {};\n\tkwargs: {}".format(pformat(args), pformat(kwargs)))

            task, luigi_run_result = args
            if task.task_id == self.task_id:
                self.log("I'm done, {}".format(self))
                exit_method = getattr(task, "exit", None)
                if exit_method is not None:
                    exit_method(task, luigi_run_result=luigi_run_result)

        SomeWrapperTask.event_handler("custom.events.DONE")(_done_callback)

    def exit(self, *args, **kwargs):
        self.log("exit, args {}, kwargs {}".format(pformat(args), pformat(kwargs)))

    def requires(self):
        self.log("requires, ...")
        res = super(SomeWrapperTask, self).requires()
        self.log("requires, after super, building list ...")
        res += [WorkUnderWrapperTask(kvs={"idx": i, "base_path": self.base_path}) for i in range(SUBTASK_COUNT)]
        self.log("requires, final list: {}".format(pformat(res)))
        return res

    def complete(self):
        self.log("complete, ...")
        res = super(SomeWrapperTask, self).complete()
        self.log("complete, after super, res: `{}`".format(pformat(res)))
        return res

    def output(self):
        self.log("output, ...")
        res = super(SomeWrapperTask, self).output()
        self.log("output, after super, res: `{}`".format(pformat(res)))
        return res

    def deps(self):
        self.log("deps, ...")
        res = super(SomeWrapperTask, self).deps()
        self.log("deps, after super, res: `{}`".format(pformat(res)))
        return res

    def run(self):
        self.log("run, ...")
        res = super(SomeWrapperTask, self).run()
        self.log("run, after super, res: {}".format(pformat(res)))
        return res

    def on_failure(self, exception):
        self.log("on_failure, exception: {}".format(pformat(exception)))
        res = super(SomeWrapperTask, self).on_failure(exception)
        self.log("on_failure, after super, exception {}, res {}".format(pformat(exception), pformat(res)))
        return res

    def on_success(self):
        self.log("on_success, ...")
        res = super(SomeWrapperTask, self).on_success()
        self.log("on_success, after super, res: `{}`".format(pformat(res)))
        return res

    def log(self, msg):
        print("\nSomeWrapperTask, {}".format(msg))


class WorkUnderWrapperTask(luigi.Task):
    kvs = luigi.DictParameter(default={})

    def __init__(self, *args, **kwargs):
        self.log("__init__, args: {}, kwargs {}".format(pformat(args), pformat(kwargs)))
        super(WorkUnderWrapperTask, self).__init__(*args, **kwargs)
        self.log("__init__, after super")
        self.task_id = self.path

    @property
    def path(self):
        return os.path.join(self.kvs["base_path"], str(self.kvs["idx"]))

    def requires(self):
        self.log("requires, ...")
        res = super(WorkUnderWrapperTask, self).requires()
        self.log("requires, after super, res: `{}`".format(pformat(res)))
        res += [LocalFsExternalTask(path=os.path.join(self.path, "input"))]
        self.log("requires, final res: `{}`".format(pformat(res)))

        # if self.kvs["idx"] >= 0:
        #     raise ValueError("WorkUnderWrapperTask.requires, missing_deps detected")

        return res

    def complete(self):
        self.log("complete, ...")
        res = super(WorkUnderWrapperTask, self).complete()
        self.log("complete, after super, res: `{}`".format(pformat(res)))
        return res

    def output(self):
        self.log("output, ...")
        res = super(WorkUnderWrapperTask, self).output()
        self.log("output, after super, res: `{}`".format(pformat(res)))
        res += [luigi.LocalTarget(os.path.join(self.path, "output"))]
        self.log("output, final res: `{}`".format(pformat(res)))
        return res

    def deps(self):
        self.log("deps, ...")
        res = super(WorkUnderWrapperTask, self).deps()
        self.log("deps, after super, res: `{}`".format(pformat(res)))
        return res

    def run(self):
        self.log("run ...")
        res = super(WorkUnderWrapperTask, self).run()
        self.log("run, after super, res: `{}`".format(pformat(res)))

        # if self.kvs["idx"] >= 0:
        #     raise ValueError("WorkUnderWrapperTask.run problems")

        with open(os.path.join(self.path, "output"), "wb") as f:
            dill.dump(self, f)

        self.log("run, result: {}".format(pformat(res)))
        return res

    def on_failure(self, exception):
        self.log("on_failure, exception `{}`".format(pformat(exception)))
        res = super(WorkUnderWrapperTask, self).on_failure(exception)
        self.log("on_failure, after super,res `{}`".format(pformat(res)))
        return res

    def on_success(self):
        self.log("on_success, ...")
        res = super(WorkUnderWrapperTask, self).on_success()
        self.log("on_success, after super, res: `{}`".format(pformat(res)))
        return res

    def log(self, msg):
        print("\nWorkUnderWrapperTask, {}".format(msg))


class LocalFsExternalTask(luigi.ExternalTask):
    """
    complete (call output, call exists on it)
    output (return target)
    """

    path = luigi.Parameter(description="Target local file system path")  # type: str

    def output(self):
        self.log("output, luigi.LocalTarget path: {}".format(self.path))
        return luigi.LocalTarget(self.path)

    def complete(self):
        self.log("complete, checking for local path: {}".format(self.path))
        return super(LocalFsExternalTask, self).complete()

    def log(self, msg):
        print("\nLocalFsExternalTask, {}".format(msg))


# @luigi.Task.event_handler(luigi.Event.FAILURE)
# def task_failure(*args, **kwargs):
#     print("\n\nFAILURE event\nargs: {};\nkwargs: {}".format(pformat(args), pformat(kwargs)))


# @luigi.Task.event_handler(luigi.Event.DEPENDENCY_MISSING)
# def task_missing_deps(*args, **kwargs):
#     print("\n\nDEPENDENCY_MISSING event, args {}, kwargs {}".format(pformat(args), pformat(kwargs)))


# @luigi.Task.event_handler(luigi.Event.BROKEN_TASK)
# def task_broken(*args, **kwargs):
#     print("\n\nBROKEN_TASK event, args {}, kwargs {}".format(pformat(args), pformat(kwargs)))


@luigi.Task.event_handler(luigi.Event.TIMEOUT)
def task_timeout(*args, **kwargs):
    print("\n\nTIMEOUT event, args {}, kwargs {}".format(pformat(args), pformat(kwargs)))


@luigi.Task.event_handler(luigi.Event.PROCESS_FAILURE)
def task_process_fail(*args, **kwargs):
    print("\n\nPROCESS_FAILURE event, args {}, kwargs {}".format(pformat(args), pformat(kwargs)))

# /Users/v.fedulov/.local/share/virtualenvs/dmgrinder-n1eTiDyB/lib/python2.7/site-packages/

# luigi/interface.py
# success &= worker.add(t, env_params.parallel_scheduling, env_params.parallel_scheduling_processes)
# success &= worker.run()

# luigi/worker.py
# def add(self, task, multiprocess=False, processes=0):

# def _add(self, task, is_complete):

# luigi/task.py
# def trigger_event(self, event, *args, **kwargs):
