"""Accelerator registry."""
import typing
from typing import Optional

from sky.clouds import service_catalog
from sky.utils import ux_utils

if typing.TYPE_CHECKING:
    from sky import clouds

# Canonical names of all accelerators (except TPUs) supported by SkyPilot.
# NOTE: Must include accelerators supported for local clusters.
#
# 1. What if a name is in this list, but not in any catalog?
#
# The name will be canonicalized, but the accelerator will not be supported.
# Optimizer will print an error message.
#
# 2. What if a name is not in this list, but in a catalog?
#
# The list is simply an optimization to short-circuit the search in the catalog.
# If the name is not found in the list, it will be searched in the catalog
# with its case being ignored. If a match is found, the name will be
# canonicalized to that in the catalog. Note that this lookup can be an
# expensive operation, as it requires reading the catalog or making external
# API calls (such as for Kubernetes). Thus it is desirable to keep this list
# up-to-date with commonly used accelerators.

# 3. (For SkyPilot dev) What to do if I want to add a new accelerator?
#
# Append its case-sensitive canonical name to this list. The name must match
# `AcceleratorName` in the service catalog.

_accelerator_df = service_catalog.common.read_catalog('common/accelerators.csv')

# List of non-GPU accelerators that are supported by our backend for job queue
# scheduling.
_SCHEDULABLE_NON_GPU_ACCELERATORS = [
    'tpu',
    'inferentia',
    'trainium',
]


def is_schedulable_non_gpu_accelerator(accelerator_name: str) -> bool:
    """Returns if this accelerator is a 'schedulable' non-GPU accelerator."""
    for name in _SCHEDULABLE_NON_GPU_ACCELERATORS:
        if name in accelerator_name.lower():
            return True
    return False


def canonicalize_accelerator_name(accelerator: str,
                                  cloud: Optional['clouds.Cloud']) -> str:
    """Returns the canonical accelerator name."""
    cloud_str = None
    if cloud is not None:
        cloud_str = str(cloud)

    # TPU names are always lowercase.
    if accelerator.lower().startswith('tpu-'):
        return accelerator.lower()

    # Common case: do not read the catalog files.
    df = _accelerator_df[_accelerator_df['AcceleratorName'].str.contains(
        accelerator, case=False, regex=True)]
    names = []
    for name, clouds in df[['AcceleratorName', 'Clouds']].values:
        if accelerator.lower() == name.lower():
            return name
        if cloud_str is None or cloud_str in clouds:
            names.append(name)
    if len(names) == 0:
        return accelerator
    if len(names) == 1:
        return names[0]
    assert len(names) > 1, names
    with ux_utils.print_exception_no_traceback():
        raise ValueError(f'Accelerator name {accelerator!r} is ambiguous. '
                         f'Please choose one of {names}.')
