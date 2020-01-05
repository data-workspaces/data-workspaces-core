# Copyright 2018,2019 by MPI-SWS and Data-ken Research. Licensed under Apache 2.0. See LICENSE.txt.
"""
Definition of configuration parameters
"""
import socket
from typing import Dict, Callable, Any, Optional, Tuple, cast
assert Dict
assert Callable
from os.path import isabs, expanduser, abspath, join
import click
import json

from dataworkspaces.utils.snapshot_utils import \
    validate_template
from dataworkspaces.utils.file_utils import LocalPathType, get_subpath_from_absolute
from dataworkspaces.errors import ConfigurationError
from dataworkspaces.utils.regexp_utils import HOSTNAME_RE


class ParamNotFoundError(ConfigurationError):
    pass

class ParamValidationError(ConfigurationError):
    pass

class ParamParseError(ConfigurationError):
    pass

ParseFnType = Callable[[str], Any]
ValidationFnType = Callable[[Any], None]
class ParamDef:
    def __init__(self, name:str, default_value:Any, help:str,
                 parse_fn:Optional[ParseFnType]=None,
                 validation_fn:Optional[ValidationFnType]=None):
        self.name = name
        self.default_value = default_value
        self.help = help
        self.parse_fn = parse_fn
        self.validation_fn = validation_fn
        if validation_fn:
            validation_fn(default_value)

    def parse(self, str_value:str) -> Any:
        if self.parse_fn:
            try:
                return self.parse_fn(str_value)
            except Exception as e:
                raise ParamParseError("Unable to parse parameter %s value %s"%
                                      (self.name, repr(str_value))) from e
        else:
            return str_value

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
                 parse_fn:Optional[ParseFnType]=None,
                 validation_fn:Optional[ValidationFnType]=None) -> str:
    global PARAM_DEFS
    assert name not in PARAM_DEFS
    assert name not in LOCAL_PARAM_DEFS # don't want duplicates across local and global
    PARAM_DEFS[name] = ParamDef(name, default_value, help, parse_fn, validation_fn)
    return name

RESULTS_DIR_TEMPLATE=define_param(
    'results.dir_template',
    "snapshots/{HOSTNAME}-{TAG}",
    "Template describing where results files will be moved during snapshot",
    None,
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
    lambda s:json.loads(s),
    validate_move_exclude_files
)

def validate_scratch_directory(value):
    if value is None:
        return
    if not isinstance(value, str):
        raise ParamValidationError("Directory path should be a string, not %s" % type(value))
    if isabs(value):
        raise ParamValidationError("Scratch directory path should be relative to the workspace")

SCRATCH_DIRECTORY=define_param(
    "scratch_directory",
    None,
    "Directory where scratch files are stored (checkpoints, temporary data, etc.). "+
    "If this is set, it is relative to the workspace directory.",
    None,
    validate_scratch_directory
)

def get_global_param_defaults():
    """Return a mapping of all default values of global params for use
    in generating the initial config file
    """
    return {n:d.default_value for (n, d) in PARAM_DEFS.items()
            if d.default_value is not None}


#########################################################
#                  Local Params                         #
#########################################################
# These are parameters local to the current install, and not
# tracked through the workspace.

def define_local_param(name:str, default_value:Optional[Any],
                       help:str, parse_fn:Optional[ParseFnType]=None,
                       validation_fn:Optional[ValidationFnType]=None):
    global LOCAL_PARAM_DEFS
    assert name not in LOCAL_PARAM_DEFS
    assert name not in PARAM_DEFS # don't want duplicates across local and global
    LOCAL_PARAM_DEFS[name] = ParamDef(name, default_value, help, parse_fn, validation_fn)
    return name


def get_local_param_defaults(hostname=None):
    """Return a mapping of all default values for use in generating
    the initial config file. The hostname is usually provided explicitly
    by the user
    """
    defaults = {param:defn.default_value
                for (param, defn) in LOCAL_PARAM_DEFS.items()
                if defn.default_value is not None}
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
    parse_fn=None,
    validation_fn=validate_hostname
)

def validate_local_scratch_directory(value):
    if value is None:
        return
    if not isinstance(value, str):
        raise ParamValidationError("Directory path should be a string, not %s" % type(value))
    if not isabs(value):
        raise ParamValidationError("Local scratch directory path should be absolute")

LOCAL_SCRATCH_DIRECTORY=define_local_param(
    "local_scratch_directory",
    None,
    "Directory where scratch files are stored (checkpoints, temporary data, etc.). "+
    "If this is set, it is absolute and only specific to the local machine.",
    None,
    validate_local_scratch_directory
)

def init_scratch_directory(scratch_dir:str, workspace_dir:str,
                           global_params:Dict[str,Any], local_params:Dict[str,Any]) \
                           -> Tuple[str,Optional[str]]:
    """Given the user-provided or default scratch directory, set the SCRATCH_DIRECTORY
    and LOCAL_SCRATCH_DIRECTORY parameters accordingly. One only will be set, with preference
    to the global parameter, which is relative to the workspace. Returns a tuple of the absolute
    and the gitignore entry (if any) for the scratch_directory
    """
    abs_scratch_dir = abspath(expanduser(scratch_dir)) if not isabs(scratch_dir) else scratch_dir
    scratch_dir_gitignore = None # type: Optional[str]
    if abs_scratch_dir.startswith(workspace_dir):
        rel_scratch_dir = get_subpath_from_absolute(workspace_dir, abs_scratch_dir)
        global_params[SCRATCH_DIRECTORY] = rel_scratch_dir # always store a relative directory
        # scratch dir gitignore should start with / to indicate that this should only
        # match the exact path relative to the git repo root.
        if rel_scratch_dir is None:
            raise ConfigurationError("Scratch directory cannot be equal to workspace directory. "+
                                     "It should either be a subdirectory or completely outside it.")
        if rel_scratch_dir.startswith('./'):
            scratch_dir_gitignore = rel_scratch_dir[1:]
        else:
            scratch_dir_gitignore = '/' + rel_scratch_dir
    else:
        local_params[LOCAL_SCRATCH_DIRECTORY] = abs_scratch_dir
    return (abs_scratch_dir, scratch_dir_gitignore)

def clone_scratch_directory(workspace_dir:str, global_params:Dict[str,Any],
                            local_params:Dict[str,Any],
                            batch:bool=False) -> str:
    """Set the scratch directory parameters for a cloned copy of the workspace,
    updating local_params if neded.
    Returns the absolute path for the scratch directory on this system.
    """
    if SCRATCH_DIRECTORY in global_params:
        return join(workspace_dir, global_params[SCRATCH_DIRECTORY])
    elif not batch:
        local_path = \
            cast(str,
                 click.prompt("Please specify a location for this workspace's scratch directory (must be outside of workspace)",
                              type=LocalPathType(exists=False, must_be_outside_of_workspace=workspace_dir)))
        local_params[LOCAL_SCRATCH_DIRECTORY] = local_path
        return local_path
    else:
        # TODO: come up with a standard way of handling this when called from the API - either by
        # letting the user provide values in advance or by having some standard defaults.
        raise ConfigurationError("Scratch directory was not within workspaces and we are running in batch mode. No way to ask user for location.")

def get_scratch_directory(workspace_dir:str, global_params:Dict[str,Any],
                          local_params:Dict[str,Any]) -> Optional[str]:
    """Given the global and local params, return the absolute path
    of the scratch directory for this workspace. If it was not specified
    in either, print a warning and return None.
    """
    if SCRATCH_DIRECTORY in global_params:
        # normalize the path to remove any "." in the path
        return abspath(join(workspace_dir, global_params[SCRATCH_DIRECTORY]))
    elif LOCAL_SCRATCH_DIRECTORY in local_params:
        return local_params[LOCAL_SCRATCH_DIRECTORY]
    else:
        click.echo("WARNING: Neither the %s nor %s parameters are set, so cannot find scratch directory. Please set one using 'dws config'."%
                   (SCRATCH_DIRECTORY, LOCAL_SCRATCH_DIRECTORY),
                   err=True)
        return None

