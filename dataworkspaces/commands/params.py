# Copyright 2018,2019 by MPI-SWS and Data-ken Research. Licensed under Apache 2.0. See LICENSE.txt.
"""
Definition of configuration parameters
"""
import re
import json
import socket
from os.path import join
from dataworkspaces.resources.snapshot_utils import \
    validate_template
from dataworkspaces.errors import ConfigurationError
from dataworkspaces.utils.regexp_utils import HOSTNAME_RE

PARAM_DEFS = {}
LOCAL_PARAM_DEFS = {}

class ParamDef:
    def __init__(self, name, default_value, help, validation_fn=None):
        self.name = name
        self.default_value = default_value
        self.validation_fn = validation_fn
        if validation_fn:
            validation_fn(default_value)

    def validate(self, value):
        if self.validation_fn:
            try:
                self.validation_fn(value)
            except Exception as e:
                raise ConfigurationError("Error in validation of configuration parameter '%s'"%
                                         self.name) from e

def define_param(name, default_value, help, validation_fn=None):
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
        raise ConfigurationError("Move excluded files parameter must be a list or a tuple")
    for v in value:
        if not isinstance(v, str):
            raise ConfigurationError("Move excluded files parameter elements must be strings")

RESULTS_MOVE_EXCLUDE_FILES=define_param(
    'results.move_exclude_files',
    ['README.txt', 'README.rst', 'README.md'],
    "List of files to exclude when moving results to a subdirectory during snapshot.",
    validate_move_exclude_files
)


def get_config_param_value(config_data, param_name):
    assert param_name in PARAM_DEFS
    if 'global_params' not in config_data:
        return PARAM_DEFS[param_name].default_value
    params = config_data['global_params']
    return params.get(param_name, PARAM_DEFS[param_name].default_value)

def get_all_defaults():
    """Return a mapping of all default values for use in generating
    the initial config file.
    """
    return {param:PARAM_DEFS[param].default_value for param in PARAM_DEFS.keys()}

#########################################################
#                  Local Params                         #
#########################################################
# These are parameters local to the current install, and not
# tracked through git. The local params file also contains per-resource params
# as well.

def define_local_param(name, default_value, help, validation_fn=None):
    global LOCAL_PARAM_DEFS
    assert name not in LOCAL_PARAM_DEFS
    assert name not in PARAM_DEFS # don't want duplicates across local and global
    LOCAL_PARAM_DEFS[name] = ParamDef(name, default_value, help, validation_fn)
    return name

def get_local_param_value(local_config, param_name):
    assert param_name in LOCAL_PARAM_DEFS
    return local_config.get(param_name, LOCAL_PARAM_DEFS[param_name].default_value)

def get_local_param_from_file(workspace_dir, param_name):
    with open(get_local_params_file_path(workspace_dir), 'r') as f:
        local_config = json.load(f)
        return get_local_param_value(local_config, param_name)

def get_local_defaults(hostname=None):
    """Return a mapping of all default values for use in generating
    the initial config file. The hostname is usually provided explicitly
    by the user
    """
    defaults = {param:LOCAL_PARAM_DEFS[param].default_value
                for param in LOCAL_PARAM_DEFS.keys()}
    if hostname is not None:
        defaults[HOSTNAME] = hostname
    return defaults

def get_local_params_file_path(workspace_dir):
    return join(workspace_dir, '.dataworkspace/local_params.json')

def validate_hostname(name):
    if not HOSTNAME_RE.match(name):
        raise ConfigurationError("'%s' is not a valid hostname: must start with a letter and only contain letters, numbers, '-', '_', and '.'" % name)

DEFAULT_HOSTNAME=socket.gethostname().split('.')[0]
HOSTNAME=define_local_param(
    'hostname',
    DEFAULT_HOSTNAME,
    help="Hostname to identify this machine in snapshots.",
    validation_fn=validate_hostname
)
