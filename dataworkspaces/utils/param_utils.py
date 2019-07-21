# Copyright 2018,2019 by MPI-SWS and Data-ken Research. Licensed under Apache 2.0. See LICENSE.txt.
"""
Definition of configuration parameters
"""
import socket
from typing import Dict, Callable, Any, Optional

from dataworkspaces.utils.snapshot_utils import \
    validate_template
from dataworkspaces.errors import ConfigurationError
from dataworkspaces.utils.regexp_utils import HOSTNAME_RE


class ParamNotFoundError(ConfigurationError):
    pass

class ParamValidationError(ConfigurationError):
    pass

ValidationFnType = Callable[[Any], None]
class ParamDef:
    def __init__(self, name:str, default_value:Any, help:str,
                 validation_fn:Optional[ValidationFnType]=None):
        self.name = name
        self.default_value = default_value
        self.validation_fn = validation_fn
        if validation_fn:
            validation_fn(default_value)

    def validate(self, value:Any)->None:
        if self.validation_fn:
            try:
                self.validation_fn(value)
            except Exception as e:
                raise ParamValidationError("Error in validation of configuration parameter '%s'"%
                                           self.name) from e


PARAM_DEFS = {} # type: Dict[str, ParamDef]
LOCAL_PARAM_DEFS = {} # type: Dict[str, ParamDef]


def define_param(name:str, default_value:Any, help:str,
                 validation_fn:Optional[ValidationFnType]=None) -> str:
    global PARAM_DEFS
    assert name not in PARAM_DEFS
    assert name not in LOCAL_PARAM_DEFS # don't want duplicates across local and global
    PARAM_DEFS[name] = ParamDef(name, default_value, help, validation_fn)
    return name

RESULTS_DIR_TEMPLATE=define_param(
    'results.dir_template',
    "snapshots/{HOSTNAME}-{TAG}",
    "Template describing where results files will be moved during snapshot",
    validate_template
)

def validate_move_exclude_files(value):
    if not isinstance(value, list) and not isinstance(value, tuple):
        raise ParamValidationError("Move excluded files parameter must be a list or a tuple")
    for v in value:
        if not isinstance(v, str):
            raise ParamValidationError("Move excluded files parameter elements must be strings")

RESULTS_MOVE_EXCLUDE_FILES=define_param(
    'results.move_exclude_files',
    ['README.txt', 'README.rst', 'README.md'],
    "List of files to exclude when moving results to a subdirectory during snapshot.",
    validate_move_exclude_files
)

def get_global_param_defaults():
    """Return a mapping of all default values of global params for use
    in generating the initial config file
    """
    return {n:d.default_value for (n, d) in PARAM_DEFS.items()}


#########################################################
#                  Local Params                         #
#########################################################
# These are parameters local to the current install, and not
# tracked through the workspace.

def define_local_param(name, default_value, help, validation_fn=None):
    global LOCAL_PARAM_DEFS
    assert name not in LOCAL_PARAM_DEFS
    assert name not in PARAM_DEFS # don't want duplicates across local and global
    LOCAL_PARAM_DEFS[name] = ParamDef(name, default_value, help, validation_fn)
    return name


def get_local_param_defaults(hostname=None):
    """Return a mapping of all default values for use in generating
    the initial config file. The hostname is usually provided explicitly
    by the user
    """
    defaults = {param:LOCAL_PARAM_DEFS[param].default_value
                for param in LOCAL_PARAM_DEFS.keys()}
    if hostname is not None:
        defaults[HOSTNAME] = hostname
    return defaults


def validate_hostname(name):
    if not HOSTNAME_RE.match(name):
        raise ParamValidationError("'%s' is not a valid hostname: must start with a letter and only contain letters, numbers, '-', '_', and '.'" % name)

DEFAULT_HOSTNAME=socket.gethostname().split('.')[0]
HOSTNAME=define_local_param(
    'hostname',
    DEFAULT_HOSTNAME,
    help="Hostname to identify this machine in snapshots.",
    validation_fn=validate_hostname
)
