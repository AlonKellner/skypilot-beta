"""Common enumerators and classes."""

import enum
import importlib

from sky import sky_logging
from sky.utils import common_utils


SKY_SERVE_CONTROLLER_PREFIX: str = 'sky-serve-controller-'
JOB_CONTROLLER_PREFIX: str = 'sky-jobs-controller-'
# Add user hash so that two users don't have the same controller VM on
# shared-account clouds such as GCP.
SKY_SERVE_CONTROLLER_NAME: str = (
    f'{SKY_SERVE_CONTROLLER_PREFIX}{common_utils.get_user_hash()}')
JOB_CONTROLLER_NAME: str = (
    f'{JOB_CONTROLLER_PREFIX}{common_utils.get_user_hash()}')


class StatusRefreshMode(enum.Enum):
    """The mode of refreshing the status of a cluster."""

    NONE = 'NONE'
    # Automatically refresh when needed, e.g., autostop is set or the cluster
    # is a spot instance.
    AUTO = 'AUTO'
    FORCE = 'FORCE'


# Constants: minimize what target?
class OptimizeTarget(enum.Enum):
    COST = 0
    TIME = 1


def reload():
    # When a user request is sent to api server, it changes the user hash in the
    # env vars, but since controller_utils is imported before the env vars are
    # set, it doesn't get updated. So we need to reload it here.
    # pylint: disable=import-outside-toplevel
    from sky import skypilot_config
    from sky.utils import controller_utils
    global SKY_SERVE_CONTROLLER_NAME
    global JOB_CONTROLLER_NAME
    SKY_SERVE_CONTROLLER_NAME = (
        f'{SKY_SERVE_CONTROLLER_PREFIX}{common_utils.get_user_hash()}')
    JOB_CONTROLLER_NAME = (
        f'{JOB_CONTROLLER_PREFIX}{common_utils.get_user_hash()}')
    importlib.reload(controller_utils)
    importlib.reload(skypilot_config)

    # Make sure the logger takes the new environment variables. This is
    # necessary because the logger is initialized before the environment
    # variables are set, such as SKYPILOT_DEBUG.
    sky_logging.reload_logger()
