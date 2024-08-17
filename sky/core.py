"""SDK functions for cluster/job management."""
import getpass
import os
import shlex
import subprocess
import typing
from typing import Any, Dict, List, Optional, Union

import colorama

from sky import backends
from sky import check as sky_check
from sky import clouds
from sky import dag
from sky import data
from sky import exceptions
from sky import global_user_state
from sky import sky_logging
from sky import task
from sky.backends import backend_utils
from sky.clouds import service_catalog
from sky.provision.kubernetes import utils as kubernetes_utils
from sky.skylet import constants
from sky.skylet import job_lib
from sky.skylet import log_lib
from sky.usage import usage_lib
from sky.utils import common
from sky.utils import common_utils
from sky.utils import controller_utils
from sky.utils import log_utils
from sky.utils import rich_utils
from sky.utils import status_lib
from sky.utils import subprocess_utils
from sky.utils import ux_utils

if typing.TYPE_CHECKING:
    from sky import resources as resources_lib

logger = sky_logging.init_logger(__name__)

# ======================
# = Cluster Management =
# ======================


@usage_lib.entrypoint
def status(
    cluster_names: Optional[Union[str, List[str]]] = None,
    refresh: common.StatusRefreshMode = common.StatusRefreshMode.NONE
) -> List[Dict[str, Any]]:
    # NOTE(dev): Keep the docstring consistent between the Python API and CLI.
    """Get cluster statuses.

    If cluster_names is given, return those clusters. Otherwise, return all
    clusters.

    Each returned value has the following fields:

    .. code-block:: python

        {
            'name': (str) cluster name,
            'launched_at': (int) timestamp of last launch on this cluster,
            'handle': (ResourceHandle) an internal handle to the cluster,
            'last_use': (str) the last command/entrypoint that affected this
              cluster,
            'status': (sky.ClusterStatus) cluster status,
            'autostop': (int) idle time before autostop,
            'to_down': (bool) whether autodown is used instead of autostop,
            'metadata': (dict) metadata of the cluster,
        }

    Each cluster can have one of the following statuses:

    - ``INIT``: The cluster may be live or down. It can happen in the following
      cases:

      - Ongoing provisioning or runtime setup. (A ``sky.launch()`` has started
        but has not completed.)
      - Or, the cluster is in an abnormal state, e.g., some cluster nodes are
        down, or the SkyPilot runtime is unhealthy. (To recover the cluster,
        try ``sky launch`` again on it.)

    - ``UP``: Provisioning and runtime setup have succeeded and the cluster is
      live.  (The most recent ``sky.launch()`` has completed successfully.)

    - ``STOPPED``: The cluster is stopped and the storage is persisted. Use
      ``sky.start()`` to restart the cluster.

    Autostop column:

    - The autostop column indicates how long the cluster will be autostopped
      after minutes of idling (no jobs running). If ``to_down`` is True, the
      cluster will be autodowned, rather than autostopped.

    Getting up-to-date cluster statuses:

    - In normal cases where clusters are entirely managed by SkyPilot (i.e., no
      manual operations in cloud consoles) and no autostopping is used, the
      table returned by this command will accurately reflect the cluster
      statuses.

    - In cases where the clusters are changed outside of SkyPilot (e.g., manual
      operations in cloud consoles; unmanaged spot clusters getting preempted)
      or for autostop-enabled clusters, use ``refresh=True`` to query the
      latest cluster statuses from the cloud providers.

    Args:
        cluster_names: a list of cluster names to query. If not
            provided, all clusters will be queried.
        refresh: whether to query the latest cluster statuses from the cloud
            provider(s).

    Returns:
        A list of dicts, with each dict containing the information of a
        cluster. If a cluster is found to be terminated or not found, it will
        be omitted from the returned list.
    """
    clusters = backend_utils.get_clusters(include_controller=True,
                                          refresh=refresh,
                                          cluster_names=cluster_names)
    # Extract additional fields from resource handle to cluster records so that
    # dashboard can use the fields to render the UI.
    for record in clusters:
        handle = record['handle']
        if handle is None:
            continue
        record['accelerators'] = handle.launched_resources.accelerators
        record['cloud'] = str(handle.launched_resources.cloud)
    return clusters


def endpoints(cluster_name: str,
              port: Optional[Union[int, str]] = None) -> Dict[int, str]:
    """Gets the endpoint for a given cluster and port number (endpoint).

    Args:
        cluster: The name of the cluster.
        port: The port number to get the endpoint for. If None, endpoints
            for all ports are returned..

    Returns: A dictionary of port numbers to endpoints. If endpoint is None,
        the dictionary will contain all ports:endpoints exposed on the cluster.

    Raises:
        ValueError: if the cluster is not UP or the endpoint is not exposed.
        RuntimeError: if the cluster has no ports to be exposed or no endpoints
            are exposed yet.
    """
    with rich_utils.safe_status('[bold cyan]Fetching endpoints for cluster '
                                f'{cluster_name}...[/]'):
        return backend_utils.get_endpoints(cluster=cluster_name, port=port)


@usage_lib.entrypoint
def cost_report() -> List[Dict[str, Any]]:
    # NOTE(dev): Keep the docstring consistent between the Python API and CLI.
    """Get all cluster cost reports, including those that have been downed.

    Each returned value has the following fields:

    .. code-block:: python

        {
            'name': (str) cluster name,
            'launched_at': (int) timestamp of last launch on this cluster,
            'duration': (int) total seconds that cluster was up and running,
            'last_use': (str) the last command/entrypoint that affected this
            'num_nodes': (int) number of nodes launched for cluster,
            'resources': (resources.Resources) type of resource launched,
            'cluster_hash': (str) unique hash identifying cluster,
            'usage_intervals': (List[Tuple[int, int]]) cluster usage times,
            'total_cost': (float) cost given resources and usage intervals,
        }

    The estimated cost column indicates price for the cluster based on the type
    of resources being used and the duration of use up until the call to
    status. This means if the cluster is UP, successive calls to report will
    show increasing price. The estimated cost is calculated based on the local
    cache of the cluster status, and may not be accurate for the cluster with
    autostop/use_spot set or terminated/stopped on the cloud console.

    Returns:
        A list of dicts, with each dict containing the cost information of a
        cluster.
    """
    cluster_reports = global_user_state.get_clusters_from_history()

    def get_total_cost(cluster_report: dict) -> float:
        duration = cluster_report['duration']
        launched_nodes = cluster_report['num_nodes']
        launched_resources = cluster_report['resources']

        cost = (launched_resources.get_cost(duration) * launched_nodes)
        return cost

    for cluster_report in cluster_reports:
        cluster_report['total_cost'] = get_total_cost(cluster_report)
        cluster_report['cloud'] = str(cluster_report['resources'].cloud)
        cluster_report['accelerators'] = cluster_report['resources'].accelerators
        

    return cluster_reports


def _start(
    cluster_name: str,
    idle_minutes_to_autostop: Optional[int] = None,
    retry_until_up: bool = False,
    down: bool = False,  # pylint: disable=redefined-outer-name
    force: bool = False,
) -> backends.CloudVmRayResourceHandle:

    cluster_status, handle = backend_utils.refresh_cluster_status_handle(
        cluster_name)
    if handle is None:
        raise ValueError(f'Cluster {cluster_name!r} does not exist.')
    if not force and cluster_status == status_lib.ClusterStatus.UP:
        sky_logging.print(f'Cluster {cluster_name!r} is already up.')
        return handle
    assert force or cluster_status in (
        status_lib.ClusterStatus.INIT,
        status_lib.ClusterStatus.STOPPED), cluster_status

    backend = backend_utils.get_backend_from_handle(handle)
    if not isinstance(backend, backends.CloudVmRayBackend):
        raise exceptions.NotSupportedError(
            f'Starting cluster {cluster_name!r} with backend {backend.NAME} '
            'is not supported.')

    if controller_utils.Controllers.from_name(cluster_name) is not None:
        if down:
            raise ValueError('Using autodown (rather than autostop) is not '
                             'supported for SkyPilot controllers. Pass '
                             '`down=False` or omit it instead.')
        if idle_minutes_to_autostop is not None:
            raise ValueError(
                'Passing a custom autostop setting is currently not '
                'supported when starting SkyPilot controllers. To '
                'fix: omit the `idle_minutes_to_autostop` argument to use the '
                f'default autostop settings (got: {idle_minutes_to_autostop}).')
        idle_minutes_to_autostop = (
            constants.CONTROLLER_IDLE_MINUTES_TO_AUTOSTOP)

    usage_lib.record_cluster_name_for_current_operation(cluster_name)

    with dag.Dag():
        dummy_task = task.Task().set_resources(handle.launched_resources)
        dummy_task.num_nodes = handle.launched_nodes
    handle = backend.provision(dummy_task,
                               to_provision=handle.launched_resources,
                               dryrun=False,
                               stream_logs=True,
                               cluster_name=cluster_name,
                               retry_until_up=retry_until_up)
    storage_mounts = backend.get_storage_mounts_metadata(handle.cluster_name)
    # Passing all_file_mounts as None ensures the local source set in Storage
    # to not redundantly sync source to the bucket.
    backend.sync_file_mounts(handle=handle,
                             all_file_mounts=None,
                             storage_mounts=storage_mounts)
    if idle_minutes_to_autostop is not None:
        backend.set_autostop(handle, idle_minutes_to_autostop, down=down)
    return handle


@usage_lib.entrypoint
def start(
    cluster_name: str,
    idle_minutes_to_autostop: Optional[int] = None,
    retry_until_up: bool = False,
    down: bool = False,  # pylint: disable=redefined-outer-name
    force: bool = False,
) -> backends.CloudVmRayResourceHandle:
    # NOTE(dev): Keep the docstring consistent between the Python API and CLI.
    """Restart a cluster.

    If a cluster is previously stopped (status is STOPPED) or failed in
    provisioning/runtime installation (status is INIT), this function will
    attempt to start the cluster.  In the latter case, provisioning and runtime
    installation will be retried.

    Auto-failover provisioning is not used when restarting a stopped
    cluster. It will be started on the same cloud, region, and zone that were
    chosen before.

    If a cluster is already in the UP status, this function has no effect.

    Args:
        cluster_name: name of the cluster to start.
        idle_minutes_to_autostop: automatically stop the cluster after this
            many minute of idleness, i.e., no running or pending jobs in the
            cluster's job queue. Idleness gets reset whenever setting-up/
            running/pending jobs are found in the job queue. Setting this
            flag is equivalent to running
            ``sky.launch(..., detach_run=True, ...)`` and then
            ``sky.autostop(idle_minutes=<minutes>)``. If not set, the
            cluster will not be autostopped.
        retry_until_up: whether to retry launching the cluster until it is
            up.
        down: Autodown the cluster: tear down the cluster after specified
            minutes of idle time after all jobs finish (successfully or
            abnormally). Requires ``idle_minutes_to_autostop`` to be set.
        force: whether to force start the cluster even if it is already up.
            Useful for upgrading SkyPilot runtime.

    Raises:
        ValueError: argument values are invalid: (1) the specified cluster does
          not exist; (2) if ``down`` is set to True but
          ``idle_minutes_to_autostop`` is None; (3) if the specified cluster is
          the managed jobs controller, and either ``idle_minutes_to_autostop``
          is not None or ``down`` is True (omit them to use the default
          autostop settings).
        sky.exceptions.NotSupportedError: if the cluster to restart was
          launched using a non-default backend that does not support this
          operation.
        sky.exceptions.ClusterOwnerIdentitiesMismatchError: if the cluster to
            restart was launched by a different user.
    """
    if down and idle_minutes_to_autostop is None:
        raise ValueError(
            '`idle_minutes_to_autostop` must be set if `down` is True.')
    return _start(cluster_name,
                  idle_minutes_to_autostop,
                  retry_until_up,
                  down,
                  force=force)


def _stop_not_supported_message(resources: 'resources_lib.Resources') -> str:
    if resources.use_spot:
        message = ('Stopping spot instances is currently not supported on '
                   f'{resources.cloud}')
    else:
        message = f'Stopping is currently not supported for {resources}'
    return message


@usage_lib.entrypoint
def stop(cluster_name: str, purge: bool = False) -> None:
    # NOTE(dev): Keep the docstring consistent between the Python API and CLI.
    """Stop a cluster.

    Data on attached disks is not lost when a cluster is stopped.  Billing for
    the instances will stop, while the disks will still be charged.  Those
    disks will be reattached when restarting the cluster.

    Currently, spot instance clusters cannot be stopped (except for GCP, which
    does allow disk contents to be preserved when stopping spot VMs).

    Args:
        cluster_name: name of the cluster to stop.
        purge: (Advanced) Forcefully mark the cluster as stopped in SkyPilot's
            cluster table, even if the actual cluster stop operation failed on
            the cloud. WARNING: This flag should only be set sparingly in
            certain manual troubleshooting scenarios; with it set, it is the
            user's responsibility to ensure there are no leaked instances and
            related resources.

    Raises:
        ValueError: the specified cluster does not exist.
        RuntimeError: failed to stop the cluster.
        sky.exceptions.NotSupportedError: if the specified cluster is a spot
          cluster, or a TPU VM Pod cluster, or the managed jobs controller.
    """
    if controller_utils.Controllers.from_name(cluster_name) is not None:
        raise exceptions.NotSupportedError(
            f'Stopping SkyPilot controller {cluster_name!r} '
            f'is not supported.')
    handle = global_user_state.get_handle_from_cluster_name(cluster_name)
    if handle is None:
        raise ValueError(f'Cluster {cluster_name!r} does not exist.')

    backend = backend_utils.get_backend_from_handle(handle)

    if isinstance(backend, backends.CloudVmRayBackend):
        assert isinstance(handle, backends.CloudVmRayResourceHandle), handle
        # Check cloud supports stopping instances
        cloud = handle.launched_resources.cloud
        assert cloud is not None, handle
        try:
            cloud.check_features_are_supported(
                handle.launched_resources,
                {clouds.CloudImplementationFeatures.STOP})
        except exceptions.NotSupportedError as e:
            raise exceptions.NotSupportedError(
                f'{colorama.Fore.YELLOW}Stopping cluster '
                f'{cluster_name!r}... skipped.{colorama.Style.RESET_ALL}\n'
                f'  {_stop_not_supported_message(handle.launched_resources)}.\n'
                '  To terminate the cluster instead, run: '
                f'{colorama.Style.BRIGHT}sky down {cluster_name}') from e

    usage_lib.record_cluster_name_for_current_operation(cluster_name)
    backend.teardown(handle, terminate=False, purge=purge)


@usage_lib.entrypoint
def down(cluster_name: str, purge: bool = False) -> None:
    # NOTE(dev): Keep the docstring consistent between the Python API and CLI.
    """Tear down a cluster.

    Tearing down a cluster will delete all associated resources (all billing
    stops), and any data on the attached disks will be lost.  Accelerators
    (e.g., TPUs) that are part of the cluster will be deleted too.

    Args:
        cluster_name: name of the cluster to down.
        purge: (Advanced) Forcefully remove the cluster from SkyPilot's cluster
            table, even if the actual cluster termination failed on the cloud.
            WARNING: This flag should only be set sparingly in certain manual
            troubleshooting scenarios; with it set, it is the user's
            responsibility to ensure there are no leaked instances and related
            resources.

    Raises:
        ValueError: the specified cluster does not exist.
        RuntimeError: failed to tear down the cluster.
        sky.exceptions.NotSupportedError: the specified cluster is the managed
          jobs controller.
    """
    handle = global_user_state.get_handle_from_cluster_name(cluster_name)
    if handle is None:
        raise ValueError(f'Cluster {cluster_name!r} does not exist.')

    usage_lib.record_cluster_name_for_current_operation(cluster_name)
    backend = backend_utils.get_backend_from_handle(handle)
    backend.teardown(handle, terminate=True, purge=purge)


@usage_lib.entrypoint
def autostop(
        cluster_name: str,
        idle_minutes: int,
        down: bool = False,  # pylint: disable=redefined-outer-name
) -> None:
    # NOTE(dev): Keep the docstring consistent between the Python API and CLI.
    """Schedule an autostop/autodown for a cluster.

    Autostop/autodown will automatically stop or teardown a cluster when it
    becomes idle for a specified duration.  Idleness means there are no
    in-progress (pending/running) jobs in a cluster's job queue.

    Idleness time of a cluster is reset to zero, whenever:

    - A job is submitted (``sky.launch()`` or ``sky.exec()``).

    - The cluster has restarted.

    - An autostop is set when there is no active setting. (Namely, either
      there's never any autostop setting set, or the previous autostop setting
      was canceled.) This is useful for restarting the autostop timer.

    Example: say a cluster without any autostop set has been idle for 1 hour,
    then an autostop of 30 minutes is set. The cluster will not be immediately
    autostopped. Instead, the idleness timer only starts counting after the
    autostop setting was set.

    When multiple autostop settings are specified for the same cluster, the
    last setting takes precedence.

    Args:
        cluster_name: name of the cluster.
        idle_minutes: the number of minutes of idleness (no pending/running
          jobs) after which the cluster will be stopped automatically. Setting
          to a negative number cancels any autostop/autodown setting.
        down: if true, use autodown (tear down the cluster; non-restartable),
          rather than autostop (restartable).

    Raises:
        ValueError: if the cluster does not exist.
        sky.exceptions.ClusterNotUpError: if the cluster is not UP.
        sky.exceptions.NotSupportedError: if the cluster is not based on
          CloudVmRayBackend or the cluster is TPU VM Pod.
        sky.exceptions.ClusterOwnerIdentityMismatchError: if the current user is
          not the same as the user who created the cluster.
        sky.exceptions.CloudUserIdentityError: if we fail to get the current
          user identity.
    """
    is_cancel = idle_minutes < 0
    verb = 'Cancelling' if is_cancel else 'Scheduling'
    option_str = 'down' if down else 'stop'
    if is_cancel:
        option_str = '{stop,down}'
    operation = f'{verb} auto{option_str}'
    if controller_utils.Controllers.from_name(cluster_name) is not None:
        raise exceptions.NotSupportedError(
            f'{operation} SkyPilot controller {cluster_name!r} '
            f'is not supported.')
    handle = backend_utils.check_cluster_available(
        cluster_name,
        operation=operation,
    )
    backend = backend_utils.get_backend_from_handle(handle)

    # Check cloud supports stopping spot instances
    cloud = handle.launched_resources.cloud
    assert cloud is not None, handle

    if not isinstance(backend, backends.CloudVmRayBackend):
        raise exceptions.NotSupportedError(
            f'{operation} cluster {cluster_name!r} with backend '
            f'{backend.__class__.__name__!r} is not supported.')
    # Check autostop is implemented for cloud
    cloud = handle.launched_resources.cloud
    if not down and not is_cancel:
        try:
            cloud.check_features_are_supported(
                handle.launched_resources,
                {clouds.CloudImplementationFeatures.STOP})
        except exceptions.NotSupportedError as e:
            raise exceptions.NotSupportedError(
                f'{colorama.Fore.YELLOW}Scheduling autostop on cluster '
                f'{cluster_name!r}...skipped.{colorama.Style.RESET_ALL}\n'
                f'  {_stop_not_supported_message(handle.launched_resources)}.'
            ) from e

    # Check if autodown is required and supported
    if not is_cancel:
        try:
            cloud.check_features_are_supported(
                handle.launched_resources,
                {clouds.CloudImplementationFeatures.AUTO_TERMINATE})
        except exceptions.NotSupportedError as e:
            raise exceptions.NotSupportedError(
                f'{colorama.Fore.YELLOW}{operation} on cluster '
                f'{cluster_name!r}...skipped.{colorama.Style.RESET_ALL}\n'
                f'  Auto{option_str} is not supported on {cloud!r} - '
                f'see reason above.') from e

    usage_lib.record_cluster_name_for_current_operation(cluster_name)
    backend.set_autostop(handle, idle_minutes, down)


# ==================
# = Job Management =
# ==================


@usage_lib.entrypoint
def queue(cluster_name: str,
          skip_finished: bool = False,
          all_users: bool = False) -> List[dict]:
    # NOTE(dev): Keep the docstring consistent between the Python API and CLI.
    """Get the job queue of a cluster.

    Please refer to the sky.cli.queue for the document.

    Returns:
        List[dict]:
        [
            {
                'job_id': (int) job id,
                'job_name': (str) job name,
                'username': (str) username,
                'submitted_at': (int) timestamp of submitted,
                'start_at': (int) timestamp of started,
                'end_at': (int) timestamp of ended,
                'resources': (str) resources,
                'status': (job_lib.JobStatus) job status,
                'log_path': (str) log path,
            }
        ]
    raises:
        ValueError: if the cluster does not exist.
        sky.exceptions.ClusterNotUpError: if the cluster is not UP.
        sky.exceptions.NotSupportedError: if the cluster is not based on
          CloudVmRayBackend.
        sky.exceptions.ClusterOwnerIdentityMismatchError: if the current user is
          not the same as the user who created the cluster.
        sky.exceptions.CloudUserIdentityError: if we fail to get the current
          user identity.
        exceptions.CommandError: if failed to get the job queue with ssh.
    """
    all_jobs = not skip_finished
    username: Optional[str] = common_utils.get_user_hash()
    if all_users:
        username = None
    code = job_lib.JobLibCodeGen.get_job_queue(username, all_jobs)

    handle = backend_utils.check_cluster_available(
        cluster_name,
        operation='getting the job queue',
    )
    backend = backend_utils.get_backend_from_handle(handle)

    returncode, jobs_payload, stderr = backend.run_on_head(handle,
                                                           code,
                                                           require_outputs=True,
                                                           separate_stderr=True)
    subprocess_utils.handle_returncode(
        returncode,
        command=code,
        error_msg=f'Failed to get job queue on cluster {cluster_name}.',
        stderr=f'{jobs_payload + stderr}',
        stream_logs=True)
    jobs = job_lib.load_job_queue(jobs_payload)
    return jobs


@usage_lib.entrypoint
# pylint: disable=redefined-builtin
def cancel(
    cluster_name: str,
    all: bool = False,
    job_ids: Optional[List[int]] = None,
    # pylint: disable=invalid-name
    _try_cancel_if_cluster_is_init: bool = False,
) -> None:
    # NOTE(dev): Keep the docstring consistent between the Python API and CLI.
    """Cancel jobs on a cluster.

    Please refer to the sky.cli.cancel for the document.

    When `all` is False and `job_ids` is None, cancel the latest running job.

    Additional arguments:
        _try_cancel_if_cluster_is_init: (bool) whether to try cancelling the job
            even if the cluster is not UP, but the head node is still alive.
            This is used by the jobs controller to cancel the job when the
            worker node is preempted in the spot cluster.

    Raises:
        ValueError: if arguments are invalid, or the cluster does not exist.
        sky.exceptions.ClusterNotUpError: if the cluster is not UP.
        sky.exceptions.NotSupportedError: if the specified cluster is a
          controller that does not support this operation.
        sky.exceptions.ClusterOwnerIdentityMismatchError: if the current user is
          not the same as the user who created the cluster.
        sky.exceptions.CloudUserIdentityError: if we fail to get the current
          user identity.
    """
    controller_utils.check_cluster_name_not_controller(
        cluster_name, operation_str='Cancelling jobs')

    if all and job_ids:
        raise ValueError('Cannot specify both `all` and `job_ids`. To cancel '
                         'all jobs, set `job_ids` to None.')

    # Check the status of the cluster.
    handle = None
    try:
        handle = backend_utils.check_cluster_available(
            cluster_name,
            operation='cancelling jobs',
        )
    except exceptions.ClusterNotUpError as e:
        if not _try_cancel_if_cluster_is_init:
            raise
        assert (e.handle is None or
                isinstance(e.handle, backends.CloudVmRayResourceHandle)), e
        if (e.handle is None or e.handle.head_ip is None):
            raise
        handle = e.handle
        # Even if the cluster is not UP, we can still try to cancel the job if
        # the head node is still alive. This is useful when a spot cluster's
        # worker node is preempted, but we can still cancel the job on the head
        # node.

    assert handle is not None, (
        f'handle for cluster {cluster_name!r} should not be None')

    backend = backend_utils.get_backend_from_handle(handle)

    if all:
        sky_logging.print(f'{colorama.Fore.YELLOW}'
                          f'Cancelling all jobs on cluster {cluster_name!r}...'
                          f'{colorama.Style.RESET_ALL}')
    elif job_ids is None:
        # all = False, job_ids is None => cancel the latest running job.
        sky_logging.print(
            f'{colorama.Fore.YELLOW}'
            f'Cancelling latest running job on cluster {cluster_name!r}...'
            f'{colorama.Style.RESET_ALL}')
    elif len(job_ids):
        # all = False, len(job_ids) > 0 => cancel the specified jobs.
        jobs_str = ', '.join(map(str, job_ids))
        sky_logging.print(
            f'{colorama.Fore.YELLOW}'
            f'Cancelling jobs ({jobs_str}) on cluster {cluster_name!r}...'
            f'{colorama.Style.RESET_ALL}')
    else:
        # all = False, len(job_ids) == 0 => no jobs to cancel.
        return

    backend.cancel_jobs(handle, job_ids, all)


@usage_lib.entrypoint
def tail_logs(cluster_name: str,
              job_id: Optional[int],
              follow: bool = True) -> None:
    # NOTE(dev): Keep the docstring consistent between the Python API and CLI.
    """Tail the logs of a job.

    Please refer to the sky.cli.tail_logs for the document.

    Raises:
        ValueError: arguments are invalid or the cluster is not supported or
          the cluster does not exist.
        sky.exceptions.ClusterNotUpError: if the cluster is not UP.
        sky.exceptions.NotSupportedError: if the cluster is not based on
          CloudVmRayBackend.
        sky.exceptions.ClusterOwnerIdentityMismatchError: if the current user is
          not the same as the user who created the cluster.
        sky.exceptions.CloudUserIdentityError: if we fail to get the current
          user identity.
    """
    # Check the status of the cluster.
    handle = backend_utils.check_cluster_available(
        cluster_name,
        operation='tailing logs',
    )
    backend = backend_utils.get_backend_from_handle(handle)

    job_str = f'job {job_id}'
    if job_id is None:
        job_str = 'the last job'
    sky_logging.print(
        f'{colorama.Fore.YELLOW}'
        f'Tailing logs of {job_str} on cluster {cluster_name!r}...'
        f'{colorama.Style.RESET_ALL}')

    usage_lib.record_cluster_name_for_current_operation(cluster_name)
    backend.tail_logs(handle, job_id, follow=follow)


@usage_lib.entrypoint
def download_logs(
        cluster_name: str,
        job_ids: Optional[List[str]],
        local_dir: str = constants.SKY_LOGS_DIRECTORY) -> Dict[str, str]:
    # NOTE(dev): Keep the docstring consistent between the Python API and CLI.
    """Download the logs of jobs.

    Args:
        cluster_name: (str) name of the cluster.
        job_ids: (List[str]) job ids.
    Returns:
        Dict[str, str]: a mapping of job_id to local log path.
    Raises:
        ValueError: if the cluster does not exist.
        sky.exceptions.ClusterNotUpError: if the cluster is not UP.
        sky.exceptions.NotSupportedError: if the cluster is not based on
          CloudVmRayBackend.
        sky.exceptions.ClusterOwnerIdentityMismatchError: if the current user is
          not the same as the user who created the cluster.
        sky.exceptions.CloudUserIdentityError: if we fail to get the current
          user identity.
    """
    # Check the status of the cluster.
    handle = backend_utils.check_cluster_available(
        cluster_name,
        operation='downloading logs',
    )
    backend = backend_utils.get_backend_from_handle(handle)
    assert isinstance(backend, backends.CloudVmRayBackend), backend

    if job_ids is not None and len(job_ids) == 0:
        return {}

    usage_lib.record_cluster_name_for_current_operation(cluster_name)
    sky_logging.print(f'{colorama.Fore.YELLOW}'
                      'Syncing down logs to local...'
                      f'{colorama.Style.RESET_ALL}')
    local_log_dirs = backend.sync_down_logs(handle, job_ids, local_dir)
    return local_log_dirs


@usage_lib.entrypoint
def job_status(cluster_name: str,
               job_ids: Optional[List[int]],
               stream_logs: bool = False
              ) -> Dict[Optional[int], Optional[job_lib.JobStatus]]:
    # NOTE(dev): Keep the docstring consistent between the Python API and CLI.
    """Get the status of jobs.

    Args:
        cluster_name: (str) name of the cluster.
        job_ids: (List[str]) job ids. If None, get the status of the last job.
    Returns:
        Dict[Optional[str], Optional[job_lib.JobStatus]]: A mapping of job_id to
        job statuses. The status will be None if the job does not exist.
        If job_ids is None and there is no job on the cluster, it will return
        {None: None}.
    Raises:
        ValueError: if the cluster does not exist.
        sky.exceptions.ClusterNotUpError: if the cluster is not UP.
        sky.exceptions.NotSupportedError: if the cluster is not based on
          CloudVmRayBackend.
        sky.exceptions.ClusterOwnerIdentityMismatchError: if the current user is
          not the same as the user who created the cluster.
        sky.exceptions.CloudUserIdentityError: if we fail to get the current
          user identity.
    """
    # Check the status of the cluster.
    handle = backend_utils.check_cluster_available(
        cluster_name,
        operation='getting job status',
    )
    backend = backend_utils.get_backend_from_handle(handle)
    if not isinstance(backend, backends.CloudVmRayBackend):
        raise exceptions.NotSupportedError(
            f'Getting job status is not supported for cluster {cluster_name!r} '
            f'of type {backend.__class__.__name__!r}.')
    assert isinstance(handle, backends.CloudVmRayResourceHandle), handle

    if job_ids is not None and len(job_ids) == 0:
        return {}

    sky_logging.print(f'{colorama.Fore.YELLOW}'
                      'Getting job status...'
                      f'{colorama.Style.RESET_ALL}')

    usage_lib.record_cluster_name_for_current_operation(cluster_name)
    statuses = backend.get_job_status(handle, job_ids, stream_logs=stream_logs)
    return statuses


# ======================
# = Storage Management =
# ======================
@usage_lib.entrypoint
def storage_ls() -> List[Dict[str, Any]]:
    # NOTE(dev): Keep the docstring consistent between the Python API and CLI.
    """Get the storages.

    Returns:
        [
            {
                'name': str,
                'launched_at': int timestamp of creation,
                'store': List[sky.StoreType],
                'last_use': int timestamp of last use,
                'status': sky.StorageStatus,
            }
        ]
    """
    storages = global_user_state.get_storage()
    for storage in storages:
        storage['store'] = list(storage.pop('handle').sky_stores.keys())
    return storages


@usage_lib.entrypoint
def storage_delete(name: str) -> None:
    # NOTE(dev): Keep the docstring consistent between the Python API and CLI.
    """Delete a storage.

    Raises:
        ValueError: If the storage does not exist.
    """
    # TODO(zhwu): check the storage owner matches the current user
    handle = global_user_state.get_handle_from_storage_name(name)
    if handle is None:
        raise ValueError(f'Storage name {name!r} not found.')
    else:
        storage_object = data.Storage(name=handle.storage_name,
                                      source=handle.source,
                                      sync_on_reconstruction=False)
        storage_object.delete()


# ===================
# = Catalog Observe =
# ===================
@usage_lib.entrypoint
def enabled_clouds() -> List[clouds.Cloud]:
    return global_user_state.get_cached_enabled_clouds()


@usage_lib.entrypoint
def realtime_gpu_availability(
    name_filter: Optional[str] = None,
    quantity_filter: Optional[int] = None
) -> List[common.RealtimeGpuAvailability]:

    counts, capacity, available = service_catalog.list_accelerator_realtime(
        gpus_only=True,
        clouds='kubernetes',
        name_filter=name_filter,
        region_filter=None,
        quantity_filter=quantity_filter,
        case_sensitive=False)
    assert (set(counts.keys()) == set(capacity.keys()) == set(
        available.keys())), (f'Keys of counts ({list(counts.keys())}), '
                             f'capacity ({list(capacity.keys())}), '
                             f'and available ({list(available.keys())}) '
                             'must be same.')
    if len(counts) == 0:
        err_msg = 'No GPUs found in Kubernetes cluster. '
        debug_msg = 'To further debug, run: sky check '
        if name_filter is not None:
            gpu_info_msg = f' {name_filter!r}'
            if quantity_filter is not None:
                gpu_info_msg += (' with requested quantity'
                                 f' {quantity_filter}')
            err_msg = (f'Resources{gpu_info_msg} not found '
                       'in Kubernetes cluster. ')
            debug_msg = ('To show available accelerators on kubernetes,'
                         ' run: sky show-gpus --cloud kubernetes ')
        full_err_msg = (err_msg + kubernetes_utils.NO_GPU_HELP_MESSAGE +
                        debug_msg)
        raise ValueError(full_err_msg)

    realtime_gpu_availability_list: List[common.RealtimeGpuAvailability] = []

    for gpu, _ in sorted(counts.items()):
        realtime_gpu_availability_list.append(
            common.RealtimeGpuAvailability(
                gpu,
                counts.pop(gpu),
                capacity[gpu],
                available[gpu],
            ))
    return realtime_gpu_availability_list


@usage_lib.entrypoint
def local_up(gpus: bool = False) -> None:
    cluster_created = False

    # Check if GPUs are available on the host
    local_gpus_available = backend_utils.check_local_gpus()
    gpus = gpus and local_gpus_available

    # Check if ~/.kube/config exists:
    if os.path.exists(os.path.expanduser('~/.kube/config')):
        curr_context = kubernetes_utils.get_current_kube_config_context_name()
        skypilot_context = 'kind-skypilot'
        if curr_context is not None and curr_context != skypilot_context:
            sky_logging.print(
                f'Current context in kube config: {curr_context}'
                '\nWill automatically switch to kind-skypilot after the local '
                'cluster is created.')
    message_str = 'Creating local cluster{}...'
    message_str = message_str.format((' with GPU support (this may take up '
                                      'to 15 minutes)') if gpus else '')
    path_to_package = os.path.dirname(__file__)
    up_script_path = os.path.join(path_to_package, 'utils/kubernetes',
                                  'create_cluster.sh')

    # Get directory of script and run it from there
    cwd = os.path.dirname(os.path.abspath(up_script_path))
    run_command = up_script_path + ' --gpus' if gpus else up_script_path
    run_command = shlex.split(run_command)

    # Setup logging paths
    run_timestamp = backend_utils.get_run_timestamp()
    log_path = os.path.join(constants.SKY_LOGS_DIRECTORY, run_timestamp,
                            'local_up.log')
    tail_cmd = 'tail -n100 -f ' + log_path

    sky_logging.print(message_str)
    style = colorama.Style
    sky_logging.print('To view detailed progress: '
                      f'{style.BRIGHT}{tail_cmd}{style.RESET_ALL}')

    returncode, _, stderr = log_lib.run_with_log(
        cmd=run_command,
        log_path=log_path,
        require_outputs=True,
        stream_logs=False,
        line_processor=log_utils.SkyLocalUpLineProcessor(),
        cwd=cwd)

    # Kind always writes to stderr even if it succeeds.
    # If the failure happens after the cluster is created, we need
    # to strip all stderr of "No kind clusters found.", which is
    # printed when querying with kind get clusters.
    stderr = stderr.replace('No kind clusters found.\n', '')

    if returncode == 0:
        cluster_created = True
    elif returncode == 100:
        sky_logging.print(
            f'{colorama.Fore.GREEN}Local cluster already '
            f'exists.{style.RESET_ALL}\n'
            'If you want to delete it instead, run: sky local down')
    else:
        with ux_utils.print_exception_no_traceback():
            raise RuntimeError(
                'Failed to create local cluster. '
                f'Full log: {log_path}'
                f'\nError: {style.BRIGHT}{stderr}{style.RESET_ALL}')
    # Run sky check
    with rich_utils.safe_status('[bold cyan]Running sky check...'):
        sky_check.check(clouds=['kubernetes'], quiet=True)
    if cluster_created:
        # Prepare completion message which shows CPU and GPU count
        # Get number of CPUs
        p = subprocess_utils.run(
            'kubectl get nodes -o jsonpath=\'{.items[0].status.capacity.cpu}\'',
            capture_output=True)
        num_cpus = int(p.stdout.decode('utf-8'))

        # GPU count/type parsing
        gpu_message = ''
        gpu_hint = ''
        if gpus:
            # Get GPU model by querying the node labels
            label_name_escaped = 'skypilot.co/accelerator'.replace('.', '\\.')
            gpu_type_cmd = f'kubectl get node skypilot-control-plane -o jsonpath=\"{{.metadata.labels[\'{label_name_escaped}\']}}\"'  # pylint: disable=line-too-long
            try:
                # Run the command and capture the output
                gpu_count_output = subprocess.check_output(gpu_type_cmd,
                                                           shell=True,
                                                           text=True)
                gpu_type_str = gpu_count_output.strip() + ' '
            except subprocess.CalledProcessError as e:
                output = str(e.output.decode('utf-8'))
                logger.warning(f'Failed to get GPU type: {output}')
                gpu_type_str = ''

            # Get number of GPUs (sum of nvidia.com/gpu resources)
            gpu_count_command = 'kubectl get nodes -o=jsonpath=\'{range .items[*]}{.status.allocatable.nvidia\\.com/gpu}{\"\\n\"}{end}\' | awk \'{sum += $1} END {print sum}\''  # pylint: disable=line-too-long
            try:
                # Run the command and capture the output
                gpu_count_output = subprocess.check_output(gpu_count_command,
                                                           shell=True,
                                                           text=True)
                gpu_count = gpu_count_output.strip(
                )  # Remove any extra whitespace
                gpu_message = f' and {gpu_count} {gpu_type_str}GPUs'
            except subprocess.CalledProcessError as e:
                output = str(e.output.decode('utf-8'))
                logger.warning(f'Failed to get GPU count: {output}')
                gpu_message = f' with {gpu_type_str}GPU support'

            gpu_hint = (
                '\nHint: To see the list of GPUs in the cluster, '
                'run \'sky show-gpus --cloud kubernetes\'') if gpus else ''

        if num_cpus < 2:
            sky_logging.print('Warning: Local cluster has less than 2 CPUs. '
                              'This may cause issues with running tasks.')
        sky_logging.print(
            f'\n{colorama.Fore.GREEN}Local Kubernetes cluster created '
            'successfully with '
            f'{num_cpus} CPUs{gpu_message}.{style.RESET_ALL}\n`sky launch` can '
            'now run tasks locally.'
            '\nHint: To change the number of CPUs, change your docker '
            'runtime settings. See https://kind.sigs.k8s.io/docs/user/quick-start/#settings-for-docker-desktop for more info.'  # pylint: disable=line-too-long
            f'{gpu_hint}')


def local_down() -> None:
    cluster_removed = False

    path_to_package = os.path.dirname(__file__)
    down_script_path = os.path.join(path_to_package, 'utils/kubernetes',
                                    'delete_cluster.sh')

    cwd = os.path.dirname(os.path.abspath(down_script_path))
    run_command = shlex.split(down_script_path)

    # Setup logging paths
    run_timestamp = backend_utils.get_run_timestamp()
    log_path = os.path.join(constants.SKY_LOGS_DIRECTORY, run_timestamp,
                            'local_down.log')
    tail_cmd = 'tail -n100 -f ' + log_path

    with rich_utils.safe_status('[bold cyan]Removing local cluster...'):
        style = colorama.Style
        sky_logging.print(f'To view detailed progress: '
                          f'{style.BRIGHT}{tail_cmd}{style.RESET_ALL}')
        returncode, stdout, stderr = log_lib.run_with_log(cmd=run_command,
                                                          log_path=log_path,
                                                          require_outputs=True,
                                                          stream_logs=False,
                                                          cwd=cwd)
        stderr = stderr.replace('No kind clusters found.\n', '')

        if returncode == 0:
            cluster_removed = True
        elif returncode == 100:
            sky_logging.print('\nLocal cluster does not exist.')
        else:
            with ux_utils.print_exception_no_traceback():
                raise RuntimeError(
                    'Failed to create local cluster. '
                    f'Stdout: {stdout}'
                    f'\nError: {style.BRIGHT}{stderr}{style.RESET_ALL}')
    if cluster_removed:
        # Run sky check
        with rich_utils.safe_status('[bold cyan]Running sky check...'):
            sky_check.check(clouds=['kubernetes'], quiet=True)
        sky_logging.print(
            f'{colorama.Fore.GREEN}Local cluster removed.{style.RESET_ALL}')
