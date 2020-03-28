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

from dataworkspaces.utils.snapshot_utils import validate_template
from dataworkspaces.utils.file_utils import LocalPathType, get_subpath_from_absolute
from dataworkspaces.errors import ConfigurationError
from dataworkspaces.utils.regexp_utils import HOSTNAME_RE


class ParamNotFoundError(ConfigurationError):
    pass


class ParamValidationError(ConfigurationError):
    pass


class ParamParseError(ConfigurationError):
    pass


class UknownParamError(ConfigurationError):
    pass


class ParamType:
    """Base class for a defining the type of the parameter.
    Types are designed to be reusable across multiple parameters."""

    def parse(self, str_value: str) -> Any:
        """Convert the string representation into the
        actual value. If the parameter's type is a string, parsing must
        be itempotent - if parse is called multiple times, the same
        parsed value is returned.

        This function is used when parsing from the command line and potentally
        when parsing from JSON.
        """
        return str_value

    def validate(self, value: Any) -> None:
        """Check the value and throw a ParamValidationError if value
        is not of this type. Does not need to handle optional values,
        this is handled by the ParamDef class."""
        pass

    def to_json(self, value: Any) -> Any:
        """Convert the parameter value to a JSON-serializable value.
        Usually, you can just use the default, which returns the value as-is.
        In some cases, like dates, you might need a conversion function.
        """
        return value

    def __repr__(self):
        return self.__class__.__name__ + "()"

    def __str__(self):
        """Override this to return a more useful type name."""
        return self.__class__.__name__


class BoolType(ParamType):
    def parse(self, str_value: str) -> bool:
        if str_value.lower() == "true":
            return True
        elif str_value.lower() == "false":
            return False
        else:
            raise ParamParseError(
                "Unable to parse boolean parameter value was '%s'" % repr(str_value)
            )

    def validate(self, value: Any) -> None:
        if not isinstance(value, bool):
            raise ParamValidationError("Parameter must be a bool, value was '%s'" % repr(value))

    def __str__(self):
        return "bool"


class StringType(ParamType):
    def validate(self, value: Any) -> None:
        if not isinstance(value, str):
            raise ParamValidationError("Value '%s' is not a string" % repr(value))

    def __str__(self):
        return "string"


class AbspathType(ParamType):
    def validate(self, value: Any) -> None:
        if not isinstance(value, str):
            raise ParamValidationError(
                "Path value '%s' should be a string, not %s" % (repr(value), type(value))
            )
        if not isabs(value):
            raise ParamValidationError("Path value '%s' is not absolute" % repr(value))

    def __str__(self):
        return "absolute_path"


class RelpathType(ParamType):
    def validate(self, value: Any) -> None:
        if not isinstance(value, str):
            raise ParamValidationError(
                "Path value '%s' should be a string, not %s" % (repr(value), type(value))
            )
        if isabs(value):
            raise ParamValidationError(
                "Path value '%s' is absolute, should be relative" % repr(value)
            )

    def __str__(self):
        return "relative_path"


class HostnameType(ParamType):
    def validate(self, name):
        if not HOSTNAME_RE.match(name):
            raise ParamValidationError(
                "'%s' is not a valid hostname: must start with a letter and only contain letters, numbers, '-', '_', and '.'"
                % name
            )

    def __str__(self):
        return "hostname"


class ParamDef:
    def __init__(self, name: str, default_value: Any, optional: bool, help: str, ptype: ParamType):
        """Define a parameter - used for both workspace config params
        and resource params.

        If optional is True, that means that the parameter can take
        a value of None. If optional is False and the default_value is None,
        then the parameter must be explicitly specified. Also, set optional to
        False if you never want to allow a value of None (user cannot override
        default value to None).
        """
        self.name = name
        self.default_value = default_value
        self.optional = optional
        self.help = help
        self.ptype = ptype
        if default_value is not None:
            # we validate our default value if it is specified
            ptype.validate(default_value)

    def parse(self, raw_value: Any) -> Any:
        """If the value is a string, parse it and return the parsed value. Otherwise,
        just return the original value.

        NOTE: if the parameter's type is a string, and a parse function is defined,
        the parse function must return the value if a parsed value is passed in again.
        """
        if isinstance(raw_value, str):
            try:
                return self.ptype.parse(raw_value)
            except Exception as e:
                raise ParamParseError(
                    "Unable to parse parameter %s value %s" % (self.name, repr(raw_value))
                ) from e
        else:
            return raw_value

    def validate(self, value: Any) -> None:
        """Validate required values and then check the types's validation method"""
        if value is None and self.optional:
            return  # if None and None is allowed, we can just skip validation
        elif value is None and self.default_value is None and not self.optional:
            raise ParamValidationError("Required parameter '%s' is None" % self.name)
        else:
            try:
                self.ptype.validate(value)
            except Exception as e:
                raise ParamValidationError(
                    "Error in validation of configuration parameter '%s'" % self.name
                ) from e

    def to_json(self, value: Any) -> Any:
        """Convert the parameter value to a JSON-serializable value, via the type's function.
        """
        return self.ptype.to_json(value)


PARAM_DEFS = {}  # type: Dict[str, ParamDef]
LOCAL_PARAM_DEFS = {}  # type: Dict[str, ParamDef]

#########################################################
#                  Global Params                        #
#########################################################
# These params are stored in the workspace's config.json
# file, which is keep in git and synced across all instances
# of the workspace.


def define_param(name: str, default_value: Any, optional: bool, help: str, ptype: ParamType) -> str:
    global PARAM_DEFS
    assert name not in PARAM_DEFS
    assert name not in LOCAL_PARAM_DEFS  # don't want duplicates across local and global
    PARAM_DEFS[name] = ParamDef(name, default_value, optional, help, ptype)
    return name


class TemplateType(ParamType):
    def validate(self, value: Any) -> None:
        validate_template(value)

    def __str__(self):
        return "template"


RESULTS_DIR_TEMPLATE = define_param(
    "results.dir_template",
    default_value="snapshots/{HOSTNAME}-{TAG}",
    optional=False,
    help="Template describing where results files will be moved during snapshot",
    ptype=TemplateType(),
)


class FileListType(ParamType):
    def parse(self, str_value: str) -> Any:
        return json.loads(str_value)

    def validate(self, value: Any) -> None:
        if not isinstance(value, list) and not isinstance(value, tuple):
            raise ParamValidationError("File list parameter must be a list or a tuple")
        for v in value:
            if not isinstance(v, str):
                raise ParamValidationError("File list parameter elements must be strings")

    def __str__(self):
        return "file_list"


RESULTS_MOVE_EXCLUDE_FILES = define_param(
    "results.move_exclude_files",
    default_value=["README.txt", "README.rst", "README.md"],
    optional=False,
    help="List of files to exclude when moving results to a subdirectory during snapshot.",
    ptype=FileListType(),
)


SCRATCH_DIRECTORY = define_param(
    "scratch_directory",
    default_value=None,
    optional=True,
    help="Directory where scratch files are stored (checkpoints, temporary data, etc.). "
    + "If this is set, it is relative to the workspace directory.",
    ptype=RelpathType(),
)


def get_global_param_defaults():
    """Return a mapping of all default values of global params for use
    in generating the initial config file
    """
    return {n: d.default_value for (n, d) in PARAM_DEFS.items() if d.default_value is not None}


#########################################################
#                  Local Params                         #
#########################################################
# These are parameters local to the current install, and not
# tracked through the workspace.


def define_local_param(
    name: str, default_value: Optional[Any], optional: bool, help: str, ptype: ParamType
):
    global LOCAL_PARAM_DEFS
    assert name not in LOCAL_PARAM_DEFS
    assert name not in PARAM_DEFS  # don't want duplicates across local and global
    LOCAL_PARAM_DEFS[name] = ParamDef(name, default_value, optional, help, ptype)
    return name


def get_local_param_defaults(hostname=None):
    """Return a mapping of all default values for use in generating
    the initial config file. The hostname is usually provided explicitly
    by the user
    """
    defaults = {
        param: defn.default_value
        for (param, defn) in LOCAL_PARAM_DEFS.items()
        if defn.default_value is not None
    }
    if hostname is not None:
        defaults[HOSTNAME] = hostname
    return defaults


DEFAULT_HOSTNAME = socket.gethostname().split(".")[0]
HOSTNAME = define_local_param(
    "hostname",
    default_value=DEFAULT_HOSTNAME,
    optional=False,
    help="Hostname to identify this machine in snapshots.",
    ptype=HostnameType(),
)

LOCAL_SCRATCH_DIRECTORY = define_local_param(
    "local_scratch_directory",
    default_value=None,
    optional=True,
    help="Directory where scratch files are stored (checkpoints, temporary data, etc.). "
    + "If this is set, it is absolute and only specific to the local machine.",
    ptype=AbspathType(),
)


def init_scratch_directory(
    scratch_dir: str,
    workspace_dir: str,
    global_params: Dict[str, Any],
    local_params: Dict[str, Any],
) -> Tuple[str, Optional[str]]:
    """Given the user-provided or default scratch directory, set the SCRATCH_DIRECTORY
    and LOCAL_SCRATCH_DIRECTORY parameters accordingly. One only will be set, with preference
    to the global parameter, which is relative to the workspace. Returns a tuple of the absolute
    and the gitignore entry (if any) for the scratch_directory
    """
    abs_scratch_dir = abspath(expanduser(scratch_dir)) if not isabs(scratch_dir) else scratch_dir
    scratch_dir_gitignore = None  # type: Optional[str]
    if abs_scratch_dir.startswith(workspace_dir):
        rel_scratch_dir = get_subpath_from_absolute(workspace_dir, abs_scratch_dir)
        global_params[SCRATCH_DIRECTORY] = rel_scratch_dir  # always store a relative directory
        # scratch dir gitignore should start with / to indicate that this should only
        # match the exact path relative to the git repo root.
        if rel_scratch_dir is None:
            raise ConfigurationError(
                "Scratch directory cannot be equal to workspace directory. "
                + "It should either be a subdirectory or completely outside it."
            )
        if rel_scratch_dir.startswith("./"):
            scratch_dir_gitignore = rel_scratch_dir[1:]
        else:
            scratch_dir_gitignore = "/" + rel_scratch_dir
    else:
        local_params[LOCAL_SCRATCH_DIRECTORY] = abs_scratch_dir
    return (abs_scratch_dir, scratch_dir_gitignore)


def clone_scratch_directory(
    workspace_dir: str,
    global_params: Dict[str, Any],
    local_params: Dict[str, Any],
    batch: bool = False,
) -> str:
    """Set the scratch directory parameters for a cloned copy of the workspace,
    updating local_params if neded.
    Returns the absolute path for the scratch directory on this system.
    """
    if SCRATCH_DIRECTORY in global_params:
        return join(workspace_dir, global_params[SCRATCH_DIRECTORY])
    elif not batch:
        local_path = cast(
            str,
            click.prompt(
                "Please specify a location for this workspace's scratch directory (must be outside of workspace)",
                type=LocalPathType(exists=False, must_be_outside_of_workspace=workspace_dir),
            ),
        )
        local_params[LOCAL_SCRATCH_DIRECTORY] = local_path
        return local_path
    else:
        # TODO: come up with a standard way of handling this when called from the API - either by
        # letting the user provide values in advance or by having some standard defaults.
        raise ConfigurationError(
            "Scratch directory was not within workspaces and we are running in batch mode. No way to ask user for location."
        )


def get_scratch_directory(
    workspace_dir: str, global_params: Dict[str, Any], local_params: Dict[str, Any]
) -> Optional[str]:
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
        click.echo(
            "WARNING: Neither the %s nor %s parameters are set, so cannot find scratch directory. Please set one using 'dws config'."
            % (SCRATCH_DIRECTORY, LOCAL_SCRATCH_DIRECTORY),
            err=True,
        )
        return None


#########################################################
#                 Resource Params                       #
#########################################################
# These are utility functions for defining and managing the
# parameters of individual resources.


class ResourceParams:
    def __init__(self):
        self.global_defs = {}  # type: Dict[str, ParamDef]
        self.local_defs = {}  # type: Dict[str, ParamDef]
        # define the globals present in all resources
        self.define(
            "resource_type",
            default_value=None,
            optional=False,
            is_global=True,
            help="Type of this resource (e.g. git, local_files, api, etc.)",
            ptype=StringType(),
        )
        self.define(
            "name",
            default_value=None,
            optional=False,
            is_global=True,
            help="Name of the resource",
            ptype=StringType(),
        )
        self.define(
            "role",
            default_value=None,
            optional=False,
            is_global=True,
            help="Resource role (source-data, intermediate-data, code, results)",
            ptype=StringType(),
        )

    def define(
        self,
        name: str,
        default_value: Any,
        optional: bool,
        help: str,
        is_global: bool,
        ptype: ParamType,
    ):
        assert name not in self.global_defs
        assert name not in self.local_defs
        defn = ParamDef(name, default_value, optional, help, ptype)
        if is_global:
            self.global_defs[name] = defn
        else:
            self.local_defs[name] = defn

    def get(self, name: str, supplied_value: Any) -> Any:
        """Use this in the resource's __init__ method to get
        the parameter for setting its value as an attribute. For example:

            self.export = self.get('export', params.get('export')) # type: bool
        """
        if name in self.global_defs:
            defn = self.global_defs[name]
        elif name in self.local_defs:
            defn = self.local_defs[name]
        else:
            raise ParamNotFoundError(name)
        parsed_value = defn.parse(supplied_value)
        defn.validate(parsed_value)
        return parsed_value

    def get_params(self, resource) -> Dict[str, Any]:
        """Retrieve the enclosing resource's global parameters
        and convert to json form.
        """
        return {
            name: self.global_defs[name].to_json(getattr(resource, name))
            for name in self.global_defs.keys()
        }

    def get_local_params(self, resource) -> Dict[str, Any]:
        """Retrieve the enclosing resource's local parameters
        and convert to json form.
        """
        return {
            name: self.local_defs[name].to_json(getattr(resource, name))
            for name in self.local_defs.keys()
        }
