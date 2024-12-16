"""Executor for the requests."""
import concurrent.futures
import contextlib
import enum
import functools
import multiprocessing
import os
import queue as queue_lib
import signal
import sys
import time
import traceback
import typing
from typing import Any, Callable, List, Optional, Union

import psutil

from sky import global_user_state
from sky import models
from sky import sky_logging
from sky import skypilot_config
from sky.api import constants as api_constants
from sky.api.requests import payloads
from sky.api.requests import requests
from sky.api.requests.queues import mp_queue
from sky.skylet import constants
from sky.utils import common
from sky.utils import common_utils
from sky.utils import timeline
from sky.utils import ux_utils

if typing.TYPE_CHECKING:
    import redis
    from redis import exceptions as redis_exceptions
else:
    redis = None
    redis_exceptions = None
    try:
        import redis
        from redis import exceptions as redis_exceptions
    except ImportError:
        pass

# pylint: disable=ungrouped-imports
if sys.version_info >= (3, 10):
    from typing import ParamSpec
else:
    from typing_extensions import ParamSpec

P = ParamSpec('P')

logger = sky_logging.init_logger(__name__)

# On macOS, the default start method for multiprocessing is 'fork', which
# can cause issues with certain types of resources, including those used in
# the QueueManager in mp_queue.py.
# The 'spawn' start method is generally more compatible across different
# platforms, including macOS.
multiprocessing.set_start_method('spawn', force=True)

# Constants based on profiling the peak memory usage of
# various sky commands. See `tests/load_test/` for details.
# Max memory consumption for each request.
_PER_BLOCKING_REQUEST_MEM_GB = 0.25
_PER_NON_BLOCKING_REQUEST_MEM_GB = 0.15
# To control the number of blocking workers.
_CPU_MULTIPLIER_FOR_BLOCKING_WORKERS = 2
_MAX_BLOCKING_WORKERS_LOCAL = 4
# Percentage of memory for blocking requests
# from the memory reserved for SkyPilot.
# This is to reserve some memory for non-blocking requests.
_MAX_MEM_PERCENT_FOR_BLOCKING = 0.6


class QueueBackend(enum.Enum):
    REDIS = 'redis'
    MULTIPROCESSING = 'multiprocessing'


def get_queue_backend() -> QueueBackend:
    if redis is None:
        return QueueBackend.MULTIPROCESSING
    try:
        assert redis is not None, 'Redis is not installed'
        queue = redis.Redis(host='localhost',
                            port=46581,
                            db=0,
                            socket_timeout=0.1)
        queue.ping()
        return QueueBackend.REDIS
    # pylint: disable=broad-except
    except (redis_exceptions.ConnectionError
            if redis_exceptions is not None else Exception):
        return QueueBackend.MULTIPROCESSING


class RequestQueue:
    """The queue for the requests, either redis or multiprocessing."""

    def __init__(self,
                 schedule_type: requests.ScheduleType,
                 backend: Optional[QueueBackend] = None):
        self.name = schedule_type.value
        self.backend = backend
        self.queue: Union[queue_lib.Queue, 'redis.Redis']
        if backend == QueueBackend.MULTIPROCESSING:
            self.queue = mp_queue.get_queue(self.name)
        else:
            assert redis is not None, 'Redis is not installed'
            self.queue = redis.Redis(host='localhost',
                                     port=46581,
                                     db=0,
                                     socket_timeout=0.1)

    def put(self, obj: Any):
        if self.backend == QueueBackend.REDIS:
            assert isinstance(self.queue, redis.Redis), 'Redis is not installed'
            self.queue.lpush(self.name, obj)
        else:
            self.queue.put(obj)  # type: ignore

    def get(self):
        if self.backend == QueueBackend.REDIS:
            assert isinstance(self.queue, redis.Redis), 'Redis is not installed'
            return self.queue.rpop(self.name)
        else:
            try:
                return self.queue.get(block=False)
            except queue_lib.Empty:
                return None

    def __len__(self):
        # TODO(zhwu): we should autoscale based on the queue length.
        if isinstance(self.queue, redis.Redis):
            return self.queue.llen(self.name)
        else:
            return self.queue.qsize()


queue_backend = get_queue_backend()


@functools.lru_cache(maxsize=None)
def _get_queue(schedule_type: requests.ScheduleType) -> RequestQueue:
    return RequestQueue(schedule_type, backend=queue_backend)


@contextlib.contextmanager
def override_request_env_and_config(request_body: payloads.RequestBody):
    """Override the environment and SkyPilot config for a request."""
    original_env = os.environ.copy()
    os.environ.update(request_body.env_vars)
    user = models.User(id=request_body.env_vars[constants.USER_ID_ENV_VAR],
                       name=request_body.env_vars[constants.USER_ENV_VAR])
    global_user_state.add_user(user)
    common_utils.set_current_command(request_body.entrypoint_command)
    # Force color to be enabled.
    os.environ['CLICOLOR_FORCE'] = '1'
    common.reload()
    try:
        with skypilot_config.override_skypilot_config(
                request_body.override_skypilot_config):
            yield
    finally:
        # We need to call the save_timeline() since atexit will not be
        # triggered as multiple requests can be sharing the same process.
        timeline.save_timeline()
        # Restore the original environment variables, so that a new request
        # won't be affected by the previous request, e.g. SKYPILOT_DEBUG
        # setting, etc. This is necessary as our executor is reusing the
        # same process for multiple requests.
        os.environ.clear()
        os.environ.update(original_env)


def _wrapper(request_id: str, ignore_return_value: bool) -> None:
    """Wrapper for a request task."""

    def sigterm_handler(signum, frame):
        raise KeyboardInterrupt

    signal.signal(signal.SIGTERM, sigterm_handler)

    def redirect_output(file):
        """Redirect stdout and stderr to the log file."""
        fd = file.fileno()  # Get the file descriptor from the file object
        # Store copies of the original stdout and stderr file descriptors
        original_stdout = os.dup(sys.stdout.fileno())
        original_stderr = os.dup(sys.stderr.fileno())

        # Copy this fd to stdout and stderr
        os.dup2(fd, sys.stdout.fileno())
        os.dup2(fd, sys.stderr.fileno())
        return original_stdout, original_stderr

    def restore_output(original_stdout, original_stderr):
        """Restore stdout and stderr to their original file descriptors. """
        os.dup2(original_stdout, sys.stdout.fileno())
        os.dup2(original_stderr, sys.stderr.fileno())

        # Close the duplicate file descriptors
        os.close(original_stdout)
        os.close(original_stderr)

    pid = multiprocessing.current_process().pid
    logger.info(f'Running request {request_id} with pid {pid}')
    with requests.update_request(request_id) as request_task:
        assert request_task is not None, request_id
        log_path = request_task.log_path
        request_task.pid = pid
        request_task.status = requests.RequestStatus.RUNNING
        func = request_task.entrypoint
        request_body = request_task.request_body

    with log_path.open('w', encoding='utf-8') as f:
        # Store copies of the original stdout and stderr file descriptors
        original_stdout, original_stderr = redirect_output(f)
        # Redirect the stdout/stderr before overriding the environment and
        # config, as there can be some logs during override that needs to be
        # captured in the log file.
        try:
            with override_request_env_and_config(request_body):
                return_value = func(**request_body.to_kwargs())
        except KeyboardInterrupt:
            logger.info(f'Request {request_id} aborted by user')
            restore_output(original_stdout, original_stderr)
            return
        except (Exception, SystemExit) as e:  # pylint: disable=broad-except
            with ux_utils.enable_traceback():
                stacktrace = traceback.format_exc()
            setattr(e, 'stacktrace', stacktrace)
            with requests.update_request(request_id) as request_task:
                assert request_task is not None, request_id
                request_task.status = requests.RequestStatus.FAILED
                request_task.set_error(e)
            restore_output(original_stdout, original_stderr)
            logger.info(f'Request {request_id} failed due to '
                        f'{common_utils.format_exception(e)}')
            return
        else:
            with requests.update_request(request_id) as request_task:
                assert request_task is not None, request_id
                request_task.status = requests.RequestStatus.SUCCEEDED
                if not ignore_return_value:
                    request_task.set_return_value(return_value)
            restore_output(original_stdout, original_stderr)
            logger.info(f'Request {request_id} finished')


def schedule_request(
        request_id: str,
        request_name: str,
        request_body: payloads.RequestBody,
        func: Callable[P, Any],
        ignore_return_value: bool = False,
        schedule_type: requests.ScheduleType = requests.ScheduleType.BLOCKING,
        is_skypilot_system: bool = False):
    """Enqueue a request to the request queue."""
    user_id = request_body.env_vars[constants.USER_ID_ENV_VAR]
    if is_skypilot_system:
        user_id = api_constants.SKYPILOT_SYSTEM_USER_ID
        global_user_state.add_user(models.User(id=user_id, name=user_id))
    request = requests.Request(request_id=request_id,
                               name=api_constants.REQUEST_NAME_PREFIX +
                               request_name,
                               entrypoint=func,
                               request_body=request_body,
                               status=requests.RequestStatus.PENDING,
                               created_at=time.time(),
                               schedule_type=schedule_type,
                               user_id=user_id)

    if not requests.create_if_not_exists(request):
        logger.debug(f'Request {request_id} already exists.')
        return

    request.log_path.touch()
    input_tuple = (request_id, ignore_return_value)
    _get_queue(schedule_type).put(input_tuple)


def request_worker(worker_id: int, schedule_type: requests.ScheduleType,
                   max_parallel_size: int):
    """Worker for the requests.

    Args:
        worker_id: The ID of the worker.
        schedule_type: determines the type of queue the worker will use to
            retrieve jobs.
        max_parallel_size: The maximum number of parallel jobs this worker can
            run.
    """
    logger.info(f'Request worker {worker_id} -- started with pid '
                f'{multiprocessing.current_process().pid}')
    queue = _get_queue(schedule_type)
    # Use concurrent.futures.ProcessPoolExecutor instead of multiprocessing.Pool
    # because the former is more efficient with the support of lazy creation of
    # worker processes.
    # We use executor instead of individual multiprocessing.Process to avoid
    # the overhead of forking a new process for each request, which can be about
    # 1s delay.
    with concurrent.futures.ProcessPoolExecutor(
            max_workers=max_parallel_size) as executor:
        while True:
            request = queue.get()
            if request is None:
                time.sleep(0.1)
                continue
            request_id, ignore_return_value = request
            request = requests.get_request(request_id)
            if request.status == requests.RequestStatus.ABORTED:
                continue
            logger.info(
                f'Request worker {worker_id} -- submitted request: {request_id}'
            )
            # Start additional process to run the request, so that it can be
            # aborted when requested by a user.
            # TODO(zhwu): since the executor is reusing the request process,
            # multiple requests can share the same process pid, which may cause
            # issues with SkyPilot core functions if they rely on the exit of
            # the process, such as subprocess_daemon.py.
            future = executor.submit(_wrapper, request_id, ignore_return_value)

            if schedule_type == requests.ScheduleType.BLOCKING:
                # Wait for the request to finish.
                try:
                    future.result(timeout=None)
                except Exception as e:  # pylint: disable=broad-except
                    logger.error(
                        f'Request worker {worker_id} -- request {request_id} '
                        f'failed: {e}')
                logger.info(
                    f'Request worker {worker_id} -- request {request_id} '
                    'finished')
            else:
                # Non-blocking requests are handled by the non-blocking worker.
                logger.info(
                    f'Request worker {worker_id} -- request {request_id} '
                    'submitted')


def start(deploy: bool) -> List[multiprocessing.Process]:
    """Start the request workers."""
    # Determine the job capacity of the workers based on the system resources.
    cpu_count: int = psutil.cpu_count()
    total_mem_size_gb: float = psutil.virtual_memory().total / (1024**3)
    mem_size_gb = max(0, total_mem_size_gb - api_constants.MIN_AVAIL_MEM_GB)
    parallel_for_blocking = _max_parallel_size_for_blocking(
        cpu_count, mem_size_gb)
    if not deploy:
        parallel_for_blocking = min(parallel_for_blocking,
                                    _MAX_BLOCKING_WORKERS_LOCAL)
    max_parallel_for_non_blocking = _max_parallel_size_for_non_blocking(
        mem_size_gb, parallel_for_blocking)
    logger.info(
        f'SkyPilot server will start {parallel_for_blocking} workers for '
        f'blocking requests and will allow at max '
        f'{max_parallel_for_non_blocking} non-blocking requests in parallel.')

    # Setup the queues.
    if queue_backend == QueueBackend.MULTIPROCESSING:
        logger.info('Creating shared request queues')
        queue_server = multiprocessing.Process(
            target=mp_queue.start_queue_manager,
            args=([
                schedule_type.value for schedule_type in requests.ScheduleType
            ],))
        queue_server.start()
    logger.info('Request queues created')

    # Wait for the queues to be created. This is necessary to avoid request
    # workers to be refused by the connection to the queue.
    time.sleep(2)

    workers = []
    for worker_id in range(parallel_for_blocking):
        worker = multiprocessing.Process(target=request_worker,
                                         args=(worker_id,
                                               requests.ScheduleType.BLOCKING,
                                               1))
        logger.info(f'Starting request worker: {worker_id}')
        worker.start()
        workers.append(worker)

    # Start a non-blocking worker.
    worker = multiprocessing.Process(target=request_worker,
                                     args=(-1,
                                           requests.ScheduleType.NON_BLOCKING,
                                           max_parallel_for_non_blocking))
    logger.info('Starting non-blocking request worker')
    worker.start()
    workers.append(worker)
    return workers


@functools.lru_cache(maxsize=1)
def _max_parallel_size_for_blocking(cpu_count: int, mem_size_gb: float) -> int:
    """Max parallelism for blocking requests."""
    cpu_based_max_parallel = cpu_count * _CPU_MULTIPLIER_FOR_BLOCKING_WORKERS
    mem_based_max_parallel = int(mem_size_gb * _MAX_MEM_PERCENT_FOR_BLOCKING /
                                 _PER_BLOCKING_REQUEST_MEM_GB)
    n = max(1, min(cpu_based_max_parallel, mem_based_max_parallel))
    return n


@functools.lru_cache(maxsize=1)
def _max_parallel_size_for_non_blocking(mem_size_gb: float,
                                        parallel_size_for_blocking: int) -> int:
    """Max parallelism for non-blocking requests."""
    available_mem = mem_size_gb - (parallel_size_for_blocking *
                                   _PER_BLOCKING_REQUEST_MEM_GB)
    n = max(1, int(available_mem / _PER_NON_BLOCKING_REQUEST_MEM_GB))
    return n
