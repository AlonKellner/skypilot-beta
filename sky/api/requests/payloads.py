"""Payloads for the Sky API requests."""
import functools
import getpass
import json
import os
import tempfile
from typing import Any, Dict, List, Optional, Tuple, Union

import pydantic

from sky import serve
from sky import sky_logging
from sky import skypilot_config
from sky.api import common
from sky.skylet import constants
from sky.utils import common as common_lib
from sky.utils import common_utils
from sky.utils import registry

logger = sky_logging.init_logger(__name__)


@functools.lru_cache()
def request_body_env_vars() -> dict:
    env_vars = {}
    for env_var in os.environ:
        if env_var.startswith(constants.SKYPILOT_ENV_VAR_PREFIX):
            env_vars[env_var] = os.environ[env_var]
    env_vars[constants.USER_ID_ENV_VAR] = os.getenv(
        constants.USER_ID_ENV_VAR, common_utils.get_user_hash())
    env_vars[constants.USER_ENV_VAR] = os.getenv(constants.USER_ENV_VAR,
                                                 getpass.getuser())
    # Remove the path to config file, as the config content is included in the
    # request body and will be merged with the config on the server side.
    env_vars.pop(skypilot_config.ENV_VAR_SKYPILOT_CONFIG, None)
    return env_vars


def get_override_skypilot_config_from_client() -> Dict[str, Any]:
    """Returns the override configs from the client."""
    config = skypilot_config.to_dict()
    # Remove the API server config, as we should not specify the SkyPilot
    # server endpoint on the server side. This avoids the warning below.
    config.pop_nested(('api_server',), default_value=None)
    ignored_key_values = {}
    for nested_key in constants.SKIPPED_CLIENT_OVERRIDE_KEYS:
        value = config.pop_nested(nested_key, default_value=None)
        if value is not None:
            ignored_key_values['.'.join(nested_key)] = value
    if ignored_key_values:
        logger.debug(f'The following keys ({json.dumps(ignored_key_values)}) '
                     'are specified in the client SkyPilot config at '
                     f'{skypilot_config.loaded_config_path()!r}. '
                     'This will be ignored. If you want to specify it, '
                     'please modify it on server side or contact your '
                     'administrator.')
    return config


class RequestBody(pydantic.BaseModel):
    """The request body for the SkyPilot API."""
    env_vars: Dict[str, str] = {}
    entrypoint_command: str = ''
    override_skypilot_config: Optional[Dict[str, Any]] = {}

    def __init__(self, **data):
        data['env_vars'] = data.get('env_vars', request_body_env_vars())
        data['entrypoint_command'] = data.get(
            'entrypoint_command', common_utils.get_pretty_entry_point())
        data['override_skypilot_config'] = data.get(
            'override_skypilot_config',
            get_override_skypilot_config_from_client())
        super().__init__(**data)

    def to_kwargs(self) -> Dict[str, Any]:
        """Convert the request body to a kwargs dictionary on API server.

        This converts the request body into kwargs for the underlying SkyPilot
        backend's function.
        """
        kwargs = self.model_dump()
        kwargs.pop('env_vars')
        kwargs.pop('entrypoint_command')
        kwargs.pop('override_skypilot_config')
        return kwargs


class CheckBody(RequestBody):
    """The request body for the check endpoint."""
    clouds: Optional[Tuple[str]]
    verbose: bool


class ValidateBody(RequestBody):
    """The request body for the validate endpoint."""
    dag: str


class OptimizeBody(RequestBody):
    """The request body for the optimize endpoint."""
    dag: str
    minimize: common_lib.OptimizeTarget = common_lib.OptimizeTarget.COST

    def to_kwargs(self) -> Dict[str, Any]:
        # Import here to avoid requirement of the whole SkyPilot dependency on
        # local clients.
        # pylint: disable=import-outside-toplevel
        from sky.utils import dag_utils

        kwargs = super().to_kwargs()

        with tempfile.NamedTemporaryFile(mode='w') as f:
            f.write(self.dag)
            f.flush()
            dag = dag_utils.load_chain_dag_from_yaml(f.name)
            # We should not validate the dag here, as the file mounts are not
            # processed yet, but we need to validate the resources during the
            # optimization to make sure the resources are available.
        kwargs['dag'] = dag
        return kwargs


class LaunchBody(RequestBody):
    """The request body for the launch endpoint."""
    task: str
    cluster_name: str
    retry_until_up: bool = False
    idle_minutes_to_autostop: Optional[int] = None
    dryrun: bool = False
    down: bool = False
    backend: Optional[str] = None
    optimize_target: common_lib.OptimizeTarget = common_lib.OptimizeTarget.COST
    no_setup: bool = False
    clone_disk_from: Optional[str] = None
    fast: bool = False
    # Internal only:
    # pylint: disable=invalid-name
    quiet_optimizer: bool = False
    is_launched_by_jobs_controller: bool = False
    is_launched_by_sky_serve_controller: bool = False
    disable_controller_check: bool = False

    def to_kwargs(self) -> Dict[str, Any]:

        kwargs = super().to_kwargs()
        dag = common.process_mounts_in_task(self.task,
                                            self.env_vars,
                                            workdir_only=False)

        backend_cls = registry.BACKEND_REGISTRY.from_str(self.backend)
        backend = backend_cls() if backend_cls is not None else None
        kwargs['task'] = dag
        kwargs['backend'] = backend
        kwargs['_quiet_optimizer'] = kwargs.pop('quiet_optimizer')
        kwargs['_is_launched_by_jobs_controller'] = kwargs.pop(
            'is_launched_by_jobs_controller')
        kwargs['_is_launched_by_sky_serve_controller'] = kwargs.pop(
            'is_launched_by_sky_serve_controller')
        kwargs['_disable_controller_check'] = kwargs.pop(
            'disable_controller_check')
        return kwargs


class ExecBody(RequestBody):
    """The request body for the exec endpoint."""
    task: str
    cluster_name: str
    dryrun: bool = False
    down: bool = False
    backend: Optional[str] = None

    def to_kwargs(self) -> Dict[str, Any]:

        kwargs = super().to_kwargs()
        dag = common.process_mounts_in_task(self.task,
                                            self.env_vars,
                                            workdir_only=True)
        backend_cls = registry.BACKEND_REGISTRY.from_str(self.backend)
        backend = backend_cls() if backend_cls is not None else None
        kwargs['task'] = dag
        kwargs['backend'] = backend
        return kwargs


class StopOrDownBody(RequestBody):
    cluster_name: str
    purge: bool = False


class StatusBody(RequestBody):
    """The request body for the status endpoint."""
    cluster_names: Optional[List[str]] = None
    refresh: common_lib.StatusRefreshMode = common_lib.StatusRefreshMode.NONE
    all_users: bool = True


class StartBody(RequestBody):
    """The request body for the start endpoint."""
    cluster_name: str
    idle_minutes_to_autostop: Optional[int] = None
    retry_until_up: bool = False
    down: bool = False
    force: bool = False


class AutostopBody(RequestBody):
    """The request body for the autostop endpoint."""
    cluster_name: str
    idle_minutes: int
    down: bool = False


class QueueBody(RequestBody):
    """The request body for the queue endpoint."""
    cluster_name: str
    skip_finished: bool = False
    all_users: bool = False


class CancelBody(RequestBody):
    """The request body for the cancel endpoint."""
    cluster_name: str
    job_ids: Optional[List[int]]
    all: bool = False
    all_users: bool = False
    # Internal only:
    try_cancel_if_cluster_is_init: bool = False


class ClusterNameBody(RequestBody):
    """Cluster node."""
    cluster_name: str


class ClusterJobBody(RequestBody):
    """The request body for the cluster job endpoint."""
    cluster_name: str
    job_id: Optional[int]
    follow: bool = True


class ClusterJobsBody(RequestBody):
    """The request body for the cluster jobs endpoint."""
    cluster_name: str
    job_ids: Optional[List[str]]


class ClusterJobsDownloadLogsBody(RequestBody):
    """The request body for the cluster jobs download logs endpoint."""
    cluster_name: str
    job_ids: Optional[List[str]]
    local_dir: str = constants.SKY_LOGS_DIRECTORY


class DownloadBody(RequestBody):
    """The request body for the download endpoint."""
    folder_paths: List[str]


class StorageBody(RequestBody):
    """The request body for the storage endpoint."""
    name: str


class EndpointBody(RequestBody):
    """The request body for the endpoint."""
    cluster: str
    port: Optional[Union[int, str]] = None


class JobStatusBody(RequestBody):
    """The request body for the job status endpoint."""
    cluster_name: str
    job_ids: Optional[List[int]]


class JobsLaunchBody(RequestBody):
    """The request body for the jobs launch endpoint."""
    task: str
    name: Optional[str]
    retry_until_up: bool
    fast: bool = False

    def to_kwargs(self) -> Dict[str, Any]:
        kwargs = super().to_kwargs()
        kwargs['task'] = common.process_mounts_in_task(self.task,
                                                       self.env_vars,
                                                       workdir_only=False)
        return kwargs


class JobsQueueBody(RequestBody):
    """The request body for the jobs queue endpoint."""
    refresh: bool = False
    skip_finished: bool = False


class JobsCancelBody(RequestBody):
    """The request body for the jobs cancel endpoint."""
    name: Optional[str]
    job_ids: Optional[List[int]]
    all: bool = False


class JobsLogsBody(RequestBody):
    """The request body for the jobs logs endpoint."""
    name: Optional[str] = None
    job_id: Optional[int] = None
    follow: bool = True
    controller: bool = False
    refresh: bool = False


class RequestIdBody(pydantic.BaseModel):
    """The request body for the API request endpoint."""
    request_id: Optional[str] = None
    all: bool = False


class ServeUpBody(RequestBody):
    """The request body for the serve up endpoint."""
    task: str
    service_name: str

    def to_kwargs(self) -> Dict[str, Any]:
        kwargs = super().to_kwargs()
        dag = common.process_mounts_in_task(self.task,
                                            self.env_vars,
                                            workdir_only=False)
        assert len(
            dag.tasks) == 1, ('Must only specify one task in the DAG for '
                              'a service.', dag)
        kwargs['task'] = dag.tasks[0]
        return kwargs


class ServeUpdateBody(RequestBody):
    """The request body for the serve update endpoint."""
    task: str
    service_name: str
    mode: serve.UpdateMode

    def to_kwargs(self) -> Dict[str, Any]:
        kwargs = super().to_kwargs()
        dag = common.process_mounts_in_task(self.task,
                                            self.env_vars,
                                            workdir_only=False)
        assert len(
            dag.tasks) == 1, ('Must only specify one task in the DAG for '
                              'a service.', dag)
        kwargs['task'] = dag.tasks[0]
        return kwargs


class ServeDownBody(RequestBody):
    """The request body for the serve down endpoint."""
    service_names: Optional[Union[str, List[str]]]
    all: bool = False
    purge: bool = False


class ServeLogsBody(RequestBody):
    """The request body for the serve logs endpoint."""
    service_name: str
    target: Union[str, serve.ServiceComponent]
    replica_id: Optional[int] = None
    follow: bool = True


class ServeStatusBody(RequestBody):
    """The request body for the serve status endpoint."""
    service_names: Optional[Union[str, List[str]]]


class RealtimeGpuAvailabilityRequestBody(RequestBody):
    """The request body for the realtime GPU availability endpoint."""
    context: Optional[str]
    name_filter: Optional[str]
    quantity_filter: Optional[int]


class KubernetesNodeInfoRequestBody(RequestBody):
    """The request body for the kubernetes node info endpoint."""
    context: Optional[str] = None


class ListAcceleratorsBody(RequestBody):
    """The request body for the list accelerators endpoint."""
    gpus_only: bool = True
    name_filter: Optional[str] = None
    region_filter: Optional[str] = None
    quantity_filter: Optional[int] = None
    clouds: Optional[Union[List[str], str]] = None
    all_regions: bool = False
    require_price: bool = True
    case_sensitive: bool = True


class ListAcceleratorCountsBody(RequestBody):
    """The request body for the list accelerator counts endpoint."""
    gpus_only: bool = True
    name_filter: Optional[str] = None
    region_filter: Optional[str] = None
    quantity_filter: Optional[int] = None
    clouds: Optional[Union[List[str], str]] = None


class LocalUpBody(RequestBody):
    """The request body for the local up endpoint."""
    gpus: bool = True


class ServeTerminateReplicaBody(RequestBody):
    """The request body for the serve terminate replica endpoint."""
    service_name: str
    replica_id: int
    purge: bool = False


class KillRequestProcessesBody(RequestBody):
    """The request body for the kill request processes endpoint."""
    request_ids: List[str]


class StreamBody(pydantic.BaseModel):
    """The request body for the stream endpoint."""
    request_id: Optional[str] = None
    log_path: Optional[str] = None
    tail: Optional[int] = None
    plain_logs: bool = True
