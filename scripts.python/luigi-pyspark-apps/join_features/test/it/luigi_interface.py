import signal
import logging
from luigi import lock

from luigi.setup_logging import InterfaceLogging
from luigi.interface import _WorkerSchedulerFactory, core, PidLockAlreadyTakenExit
from luigi.execution_summary import LuigiRunResult


def _schedule_and_run_patched(tasks, worker_scheduler_factory=None, override_defaults=None):
    """
    :param tasks:
    :param worker_scheduler_factory:
    :param override_defaults:
    :return: True if all tasks and their dependencies were successfully run (or already completed);
             False if any error occurred. It will return a detailed response of type LuigiRunResult
             instead of a boolean if detailed_summary=True.
    """

    if worker_scheduler_factory is None:
        worker_scheduler_factory = _WorkerSchedulerFactory()
    if override_defaults is None:
        override_defaults = {}
    env_params = core(**override_defaults)

    InterfaceLogging.setup(env_params)

    kill_signal = signal.SIGUSR1 if env_params.take_lock else None
    if (not env_params.no_lock and
            not(lock.acquire_for(env_params.lock_pid_dir, env_params.lock_size, kill_signal))):
        raise PidLockAlreadyTakenExit()

    if env_params.local_scheduler:
        sch = worker_scheduler_factory.create_local_scheduler()
    else:
        if env_params.scheduler_url != '':
            url = env_params.scheduler_url
        else:
            url = 'http://{host}:{port:d}/'.format(
                host=env_params.scheduler_host,
                port=env_params.scheduler_port,
            )
        sch = worker_scheduler_factory.create_remote_scheduler(url=url)

    worker = worker_scheduler_factory.create_worker(
        scheduler=sch, worker_processes=env_params.workers, assistant=env_params.assistant)

    success = True
    logger = logging.getLogger('luigi-interface')
    with worker:
        for t in tasks:
            success &= worker.add(t, env_params.parallel_scheduling, env_params.parallel_scheduling_processes)
        logger.info('Done scheduling tasks')
        success &= worker.run()
    luigi_run_result = LuigiRunResult(worker, success)
    logger.info(luigi_run_result.summary_text)

    # PATCH: added only two next lines
    for task in tasks:
        task.trigger_event("custom.events.DONE", task, luigi_run_result)

    return luigi_run_result
