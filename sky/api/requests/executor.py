"""Executor for the requests."""
import enum
import multiprocessing
import os
import queue as queue_lib
import sys
import time
import traceback
from typing import Any, Callable, List, Optional, Union

import redis
from redis import exceptions as redis_exceptions

from sky import sky_logging
from sky.api.requests import payloads
from sky.api.requests import requests
from sky.usage import usage_lib
from sky.utils import common
from sky.utils import ux_utils

# pylint: disable=ungrouped-imports
if sys.version_info >= (3, 10):
    from typing import ParamSpec
else:
    from typing_extensions import ParamSpec

P = ParamSpec('P')

logger = sky_logging.init_logger(__name__)


class ScheduleType(enum.Enum):
    QUEUE = 'queue'
    # Directly execute the request in a different process.
    DIRECT = 'direct'

    @classmethod
    def active_queues(cls) -> List['ScheduleType']:
        return [cls.QUEUE]


class _QueueBackend(enum.Enum):
    REDIS = 'redis'
    MULTIPROCESSING = 'multiprocessing'


def get_queue_backend() -> _QueueBackend:
    try:
        queue = redis.Redis(host='localhost',
                            port=46581,
                            db=0,
                            socket_timeout=0.1)
        queue.ping()
        return _QueueBackend.REDIS
    except redis_exceptions.ConnectionError:
        return _QueueBackend.MULTIPROCESSING


class RequestQueue:

    def __init__(self, name: str, queue_type: Optional[_QueueBackend] = None):
        self.name = name
        self.queue: Union[multiprocessing.Queue, redis.Redis]
        if queue_type == _QueueBackend.MULTIPROCESSING:
            self.queue = multiprocessing.Queue()
        else:
            self.queue = redis.Redis(host='localhost',
                                     port=46581,
                                     db=0,
                                     socket_timeout=0.1)

    def put(self, object: Any):
        if isinstance(self.queue, redis.Redis):
            self.queue.lpush(self.name, object)
        else:
            self.queue.put(object)

    def get(self):
        if isinstance(self.queue, redis.Redis):
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


_queue_backend = get_queue_backend()
queues = {}
for queue_type in ScheduleType.active_queues():
    queues[queue_type] = RequestQueue(queue_type.value,
                                      queue_type=_queue_backend)


def _wrapper(request_id: str, ignore_return_value: bool):
    """Wrapper for a request task."""

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
    logger.info(f'Running task {request_id} with pid {pid}')
    with requests.update_rest_task(request_id) as request_task:
        assert request_task is not None, request_id
        log_path = request_task.log_path
        request_task.pid = pid
        request_task.status = requests.RequestStatus.RUNNING
        func = request_task.entrypoint
        request_body = request_task.request_body

    with log_path.open('w', encoding='utf-8') as f:
        # Store copies of the original stdout and stderr file descriptors
        original_stdout, original_stderr = redirect_output(f)
        try:
            os.environ.update(request_body.env_vars)
            # Force color to be enabled.
            os.environ['CLICOLOR_FORCE'] = '1'
            common.reload()
            from sky import skypilot_config
            logger.debug(f'skypilot_config: {skypilot_config._dict}')
            return_value = func(**request_body.to_kwargs())
        except Exception as e:  # pylint: disable=broad-except
            with ux_utils.enable_traceback():
                stacktrace = traceback.format_exc()
            setattr(e, 'stacktrace', stacktrace)
            usage_lib.store_exception(e)
            with requests.update_rest_task(request_id) as request_task:
                assert request_task is not None, request_id
                request_task.status = requests.RequestStatus.FAILED
                request_task.set_error(e)
            restore_output(original_stdout, original_stderr)
            logger.info(f'Task {request_id} failed due to {e}')
            return None
        else:
            with requests.update_rest_task(request_id) as request_task:
                assert request_task is not None, request_id
                request_task.status = requests.RequestStatus.SUCCEEDED
                if not ignore_return_value:
                    request_task.set_return_value(return_value)
            restore_output(original_stdout, original_stderr)
            logger.info(f'Task {request_id} finished')
        return return_value


def schedule_request(request_id: str,
                     request_name: str,
                     request_body: payloads.RequestBody,
                     func: Callable[P, Any],
                     ignore_return_value: bool = False,
                     schedule_type: ScheduleType = ScheduleType.QUEUE):
    """Enqueue a request to the request queue."""
    request = requests.Request(request_id=request_id,
                               name=request_name,
                               entrypoint=func,
                               request_body=request_body,
                               status=requests.RequestStatus.PENDING)

    if not requests.create_if_not_exists(request):
        logger.debug(f'Request {request_id} already exists.')
        return

    request.log_path.touch()
    input_tuple = (request_id, ignore_return_value)
    # Enqueue the request to the Redis list.
    if schedule_type == ScheduleType.DIRECT:
        multiprocessing.Process(target=_wrapper,
                                args=(request_id, ignore_return_value)).start()
    else:
        queues[schedule_type].put(input_tuple)


def request_worker(worker_id: int):
    """Worker for the requests."""
    logger.info(f'Request worker {worker_id} -- started with pid '
                f'{multiprocessing.current_process().pid}')
    while True:
        for queue_type in ScheduleType.active_queues():
            request = queues[queue_type].get()
            if request is not None:
                break
        if request is None:
            time.sleep(.1)
            continue
        request_id, ignore_return_value = request
        request = requests.get_request(request_id)
        if request.status == requests.RequestStatus.ABORTED:
            continue
        logger.info(
            f'Request worker {worker_id} -- running request: {request_id}')
        # Start additional process to run the request, so that it can be aborted
        # when requested by a user.
        process = multiprocessing.Process(target=_wrapper,
                                          args=(request_id,
                                                ignore_return_value))
        process.start()

        # Wait for the request to finish.
        try:
            process.join()
        except Exception as e:
            logger.error(
                f'Request worker {worker_id} -- request {request_id} failed: '
                f'{e}')
        logger.info(
            f'Request worker {worker_id} -- request {request_id} finished')


def start_request_queue_workers(
        num_queue_workers: int = 1) -> List[multiprocessing.Process]:
    """Start the request workers."""
    workers = []
    for worker_id in range(num_queue_workers):
        worker = multiprocessing.Process(target=request_worker,
                                         args=(worker_id,))
        logger.info(f'Starting request worker: {worker_id}')
        worker.start()
        workers.append(worker)
    return workers