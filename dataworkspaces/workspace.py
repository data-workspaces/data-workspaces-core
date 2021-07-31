"""
Main definitions of the workspace abstractions

*Workspace backends*, like git, subclass from the ``Workspace`` base
class. Resource implementations (e.g. local files or git resource)
subclass from the ``Resource`` base class.

Optional capabilities for both workspace backends and resource
backends are defined via abstract *mixin* classes. These classes
do not inherit from the base workspace/resource classes, to avoid
issues with multiple inheritance.

Complex operations involving resources use the following pattern,
where COMMAND is the command and CAPABILITY is the capability needed
to perform the command::

  class CAPABILITYWorkspaceMixin:
    ...
    def _COMMAND_precheck(self, resource_list:List[CAPABILITYResourceMixin]) -> None:
      # Backend can override to add more checks
      for r in resource_list:
        r.COMMAND_precheck()

    def COMMAND(sef resource_list:List[CAPAIBILITYResourceMixin]) -> None:
      self._COMMAND_precheck(resource_list)
      ...
      for r in resource_list:
        r.COMMAND()

The module ``dataworkspaces.commands.COMMAND`` should look like this::

  def COMMAND_command(workspace, ...):
    if not isinstance(workspace, CAPABILITYWorkspaceMixin):
      raise ConfigurationError("Workspace %s does not support CAPABILITY"%
                               workspace.name)
    mixin = cast(CAPABILITYWorkspaceMixin, workspace)

    ... error checking ...

    resource_list = ...

    workspace.COMMAND(resource_list)
    workspace.save("Completed command COMMAND")

"""

from typing import Dict, Any, Iterable, Optional, List, Tuple, Set, cast, Pattern, Union

from abc import ABCMeta, abstractmethod
import importlib
import os.path
import os
import datetime
import getpass
import json
import re
from urllib.parse import ParseResult, urlparse

from dataworkspaces.errors import ConfigurationError, PathNotAResourceError, InternalError
from dataworkspaces.utils.hash_utils import is_a_git_hash, is_a_shortened_git_hash, hash_bytes

from dataworkspaces.utils.param_utils import (
    PARAM_DEFS,
    LOCAL_PARAM_DEFS,
    get_global_param_defaults,
    get_local_param_defaults,
    ParamNotFoundError,
    RESULTS_DIR_TEMPLATE,
    RESULTS_MOVE_EXCLUDE_FILES,
    HOSTNAME,
    ResourceParams,
)
from dataworkspaces.utils.snapshot_utils import (
    validate_template,
    expand_dir_template,
    make_re_pattern_for_dir_template,
)
from dataworkspaces.utils.file_utils import get_subpath_from_absolute
from dataworkspaces.utils.lineage_utils import ResourceRef, LineageStore

# Standin for a JSON object/dict. The value type is overly
# permissive, as mypy does not yet support recursive types.
JSONDict = Dict[str, Any]
JSONList = List[Any]


class Workspace(metaclass=ABCMeta):
    def __init__(self, name: str, dws_version: str, batch: bool = False, verbose: bool = False):
        """Required properties are the workspace name
        and the version of dws that created it.
        batch and verbose are for the command line interface.
        """
        #: attribute: A short name for this workspace (str)
        self.name = name
        #: attribute: Version of dataworkspaces that was used to create the workspace (str)
        self.dws_version = dws_version  #
        #: attribute: True if input from user should be avoided (bool)
        self.batch = batch
        #: attribute: Print detailed logging (bool)
        self.verbose = verbose

    @abstractmethod
    def get_instance(self) -> str:
        """Return a unique identifier for this instance of the workspace.
        For lineage tracking, it is assumed that only one pipeline is running
        at a time in an instance. If the workspace exists on a local filesystem,
        then it should correspond to the machine and path where the workspace
        resides. Typically, some combination of hostname and user are sufficient.

        Uniquenes of the instance is important for things like naming the
        results snapshot subdirectories.
        """
        pass

    @abstractmethod
    def _get_global_params(self) -> JSONDict:
        """Get a dict of configuration parameters for this workspace,
        which apply across all instances. This contains only those
        parameters which are set during initialization or excplicitly
        set by the user. get_global_param() will combine these with
        system-defined defaults.
        """
        pass

    def get_global_param(self, param_name: str) -> Any:
        """Returns the value of the global param if set, otherwise the
        default. If the param is not set, returns the default value.
        If the param is not defined throws ParamNotFoundError.
        """
        params = self._get_global_params()
        if param_name in params:
            return params[param_name]
        elif param_name in PARAM_DEFS:
            return PARAM_DEFS[param_name].default_value
        else:
            raise ParamNotFoundError("No global parameter with name '%s'" % param_name)

    @abstractmethod
    def _get_local_params(self) -> JSONDict:
        """Get a dict of configuration parameters for this particular
        install of the workspace (e.g. local filesystem paths, hostname).
        This contains only those parameters which are set during initialization
        or explicitly set by the user. get_local_param will combine these
        with system-defined defaults.
        """
        pass

    def get_local_param(self, param_name: str) -> Any:
        """Returns the value of the local param if set, otherwise the
        default. If the param is not set, returns the default value.
        If the param is not defined throws ParamNotFoundError.
        """
        params = self._get_local_params()
        if param_name in params:
            return params[param_name]
        elif param_name in LOCAL_PARAM_DEFS:
            return LOCAL_PARAM_DEFS[param_name].default_value
        else:
            raise ParamNotFoundError("No local parameter with name '%s'" % param_name)

    @abstractmethod
    def _set_global_param(self, name: str, value: Any) -> None:
        """Implementation of low level saving by the backend.
        Setting does not necessarily take effect until save() is called"""
        pass

    def set_global_param(self, name: str, value: Any) -> None:
        """Validate and set a global parameter.
        Setting does not necessarily take effect until save() is called
        """
        if name not in PARAM_DEFS:
            raise ParamNotFoundError("No global parameter named '%s'" % name)
        PARAM_DEFS[name].validate(value)
        self._set_global_param(name, value)

    @abstractmethod
    def _set_local_param(self, name: str, value: Any) -> None:
        """Setting does not necessarily take effect until save() is called"""
        pass

    def set_local_param(self, name: str, value: Any) -> None:
        """Validate and set a local parameter.
        Setting does not necessarily take effect until save() is called
        """
        if name not in LOCAL_PARAM_DEFS:
            raise ParamNotFoundError("No local parameter named '%s'" % name)
        LOCAL_PARAM_DEFS[name].validate(value)
        self._set_local_param(name, value)

    @abstractmethod
    def get_scratch_directory(self) -> str:
        """Return an absolute path for the local scratch directory to be used
        by this workspace.
        """
        pass

    @abstractmethod
    def get_resource_names(self) -> Iterable[str]:
        """Return an iterable of resource names. The names should be
        returned in a consistent order, specifically the order in which
        they were added to the workspace. This supports backwards compatilbity
        for operations like snapshots.
        """
        pass

    @abstractmethod
    def _get_resource_params(self, resource_name) -> JSONDict:
        """Get the parameters for this resource from the workspace's
        metadata store - used when instantitating resources. Show
        throw a ConfigurationError if resource does not exist.

        The parameters should be placed in the ordered dict in a
        consistent order, to support backwards compatible hashing.
        Specifically the parameters should be ordered as follows:
        resource_type, name, role, relative_path, [resource-specific params]
        """
        pass

    def get_resource_role(self, resource_name) -> str:
        """Get the role of a resource without having to instantiate it.
        """
        params = self._get_resource_params(resource_name)
        return params["role"]

    def get_resource_type(self, resource_name) -> str:
        """Get the type of a resource without having to instantiate it.
        """
        params = self._get_resource_params(resource_name)
        return params["resource_type"]

    @abstractmethod
    def _get_resource_local_params(self, resource_name: str) -> Optional[JSONDict]:
        """If a resource has local parameters defined for it, return them.
        Otherwise, return None.
        """
        pass

    @abstractmethod
    def _add_params_for_resource(self, resource_name: str, params: JSONDict) -> None:
        """
        Add the params for a new resource in this workspace
        """
        pass

    @abstractmethod
    def _add_local_params_for_resource(self, resource_name: str, local_params: JSONDict) -> None:
        """
        Add the local params either coming from a cloned or a new resource.
        """
        pass

    @abstractmethod
    def _set_global_param_for_resource(self, resource_name: str, name: str, value: Any) -> None:
        """It is up to the caller to verify that the resource exists and has
        this parameter defined. Value should be json-serializable (via the to_json() method
        of the param type). Setting does not necessarily take effect until save() is called"""
        pass

    @abstractmethod
    def _set_local_param_for_resource(self, resource_name: str, name: str, value: Any) -> None:
        """It is up to the caller to verify that the resource exists and has
        this parameter defined. Value should be json-serializable (via the to_json() method
        of the param type). Setting does not necessarily take effect until save() is called"""
        pass

    def get_resource(self, name: str) -> "Resource":
        """Get the associated resource from the workspace metadata.
        """
        params = self._get_resource_params(name)
        resource_type = params["resource_type"]
        f = _get_resource_factory_by_resource_type(resource_type)
        local_params = self._get_resource_local_params(name)
        if f.has_local_state() and local_params is None:
            raise InternalError("Resource '%s' has local state and needs to be cloned" % name)
        return f.from_json(params, local_params if local_params is not None else {}, self)

    def get_resources(self) -> Iterable["Resource"]:
        """Iterate through all the resources
        """
        for rname in self.get_resource_names():
            yield self.get_resource(rname)

    def add_resource(self, name: str, resource_type: str, role: str, *args, **kwargs) -> "Resource":
        """Add a resource to the repository for tracking.
        """
        if name in self.get_resource_names():
            raise ConfigurationError(
                "Attempting to add a resource '%s', but there is already one with that name in the workspace"
                % name
            )
        if role not in RESOURCE_ROLE_CHOICES:
            raise ConfigurationError("Invalid resource role '%s'" % role)
        f = _get_resource_factory_by_resource_type(resource_type)
        r = f.from_command_line(role, name, self, *args, **kwargs)
        self._add_params_for_resource(r.name, r.get_params())
        self._add_local_params_for_resource(r.name, r.get_local_params())
        return r

    def clone_resource(self, name: str) -> "LocalStateResourceMixin":
        """Instantiate the resource locally.
        This is used in cases where the resource has local state.
        """
        if name not in self.get_resource_names():
            raise ConfigurationError(
                "A resource by the name '%s' does not exist in this workspace" % name
            )
        resource_type = self.get_resource_type(name)
        f = _get_resource_factory_by_resource_type(resource_type)
        assert f.has_local_state()  # should only be calling if local state
        r = f.clone(self._get_resource_params(name), self)
        self._add_local_params_for_resource(r.name, r.get_local_params())
        return r

    def get_names_of_resources_with_local_state(self) -> Iterable[str]:
        """Return an iterable of the resource names in the workspace that
        have local state.
        """
        for name in self.get_resource_names():
            resource_type = self.get_resource_type(name)
            f = _get_resource_factory_by_resource_type(resource_type)
            if f.has_local_state():
                yield name

    def get_names_for_resources_that_need_to_be_cloned(self) -> Iterable[str]:
        """Find all the resources that have local state, but no local parameters
        (not even an empty dict). These needed to be cloned. This is to be
        called during the pull() command.
        """
        for name in self.get_resource_names():
            params = self._get_resource_params(name)
            resource_type = params["resource_type"]
            f = _get_resource_factory_by_resource_type(resource_type)
            local_params = self._get_resource_local_params(name)
            if f.has_local_state() and local_params is None:
                yield name

    def validate_resource_name(
        self, resource_name: str, subpath: Optional[str] = None, expected_role: Optional[str] = None
    ) -> None:
        """Validate that the given resource name and optional subpath
        are valid in the current state of the workspace. Otherwise throws
        a ConfigurationError.
        """
        if resource_name not in self.get_resource_names():
            raise ConfigurationError("No resource named '%s'" % resource_name)
        r = self.get_resource(resource_name)
        if subpath is not None:
            r.validate_subpath_exists(subpath)
        if expected_role and r.role != expected_role:
            raise ConfigurationError(
                "Expected resource '%s' to be in role '%s', but role was '%s'"
                % (resource_name, expected_role, r.role)
            )

    def validate_local_path_for_resource(
        self, proposed_resource_name: str, proposed_local_path: str
    ) -> None:
        """When creating a resource, validate that the proposed
        local path is usable for the resource. By default, this checks
        existing resources with local state to see if they have conflicting
        paths and, if a local path exists for the workspace, whether there
        is a conflict (the entire workspace cannot be used as a resource
        path).

        Subclasses may want to add more checks. For subclasses that
        do not support *any* local state, including in resources, they
        can override the base implementation and throw an exception.
        """
        real_local_path = os.path.realpath(proposed_local_path)
        if self.get_workspace_local_path_if_any() is not None:
            if (
                os.path.realpath(cast(str, self.get_workspace_local_path_if_any()))
                == real_local_path
            ):
                raise ConfigurationError("Cannot use the entire workspace as a resource local path")
        for r in self.get_resources():
            if not isinstance(r, LocalStateResourceMixin) or r.get_local_path_if_any() is None:
                continue
            other_real_path = os.path.realpath(cast(str, r.get_local_path_if_any()))
            common = os.path.commonpath([real_local_path, other_real_path])
            if other_real_path == common or real_local_path == common:
                raise ConfigurationError(
                    "Proposed path %s for resource %s, conflicts with local path %s for resource %s"
                    % (
                        proposed_local_path,
                        proposed_resource_name,
                        r.get_local_path_if_any(),
                        r.name,
                    )
                )

    def map_local_path_to_resource(
        self, path: str, expecting_a_code_resource: bool = False
    ) -> ResourceRef:
        """Given a path on the local filesystem, map it to
           a resource and the path within the resource.
           Raises PathNotAResourceError if no match is found.

           Note: this does not check whether the path already exists.
        """
        if not os.path.isabs(path):
            path = os.path.normpath(
                os.path.join(os.path.abspath(os.path.expanduser(os.path.curdir)), path)
            )
        for rname in self.get_names_of_resources_with_local_state():
            r = self.get_resource(rname)
            assert isinstance(r, LocalStateResourceMixin)
            rpath = r.get_local_path_if_any()
            if rpath is None:
                continue
            try:
                subpath = get_subpath_from_absolute(rpath, path)
                role = self.get_resource_role(rname)
                if expecting_a_code_resource and role != ResourceRoles.CODE:
                    raise ConfigurationError(
                        "Expecting a code resource, but %s is %s" % (rname, role)
                    )
                return ResourceRef(rname, subpath)
            except ValueError:
                pass  # just try the next one
        raise PathNotAResourceError(
            "Path '%s' does not correspond to a resource in this workspace" % path
        )

    def suggest_resource_name(self, resource_type: str, role: str, *args):
        """Given the arguments passed in for creating a resource, suggest
        a (unique) name for the resource.
        """
        name = _get_resource_factory_by_resource_type(resource_type).suggest_name(self, role, *args)
        existing_resource_names = frozenset(self.get_resource_names())
        if name not in existing_resource_names:
            return name
        longer_name = name + "-" + role
        if longer_name not in existing_resource_names:
            return longer_name
        i = 2
        while True:
            numbered_name = longer_name + "-" + str(i)
            if numbered_name not in existing_resource_names:
                return numbered_name
            i += 1

    @abstractmethod
    def get_workspace_local_path_if_any(self) -> Optional[str]:
        """If the workspace maintains local state and has a "home"
        directory, return it. Otherwise, return None.

        This is useful for things like providing defaults for resource
        local paths or providing special handling for resources enclosed
        in the workspace (e.g. GitRepoResource vs. GitSubdirResource)
        """
        pass

    @abstractmethod
    def _get_local_scratch_space_for_resource(
        self, resource_name: str, create_if_not_present: bool = False
    ) -> str:
        """Return a local directory path that can be used by the resource as a local
        scratch or caching space.

        If create_if_not_present is specified, and the directory does not exist,
        it should create the directory and do
        any related work (e.g. add to the workspace's gitignore). If
        create_if_not_present is not specified and the directory does not exist,
        it should raise an InternalError. It is expected that create_if_not_present
        is specified when adding or cloning a resource, but not otherwise.
        """
        pass

    @abstractmethod
    def save(self, message: str) -> None:
        """Save the current state of the workspace"""
        pass

    @abstractmethod
    def as_snapshot_ws(self) -> "SnapshotWorkspaceMixin":
        """If this workspace supports snapshots, cast
        it to a SnapshotWorkspaceMixin. Otherwise,
        raise an NotSupportedError exception.
        """
        pass

    @abstractmethod
    def as_lineage_ws(self) -> "SnapshotWorkspaceMixin":
        """If this workspace supports snapshots and lineage, cast
        it to a SnapshotWorkspaceMixin. Otherwise,
        raise an NotSupportedError exception.
        """
        pass


class WorkspaceFactory(metaclass=ABCMeta):
    """This class collects the various ways of instantiating a workspace:
    creating from an existing one, initing a new one, and cloning into a
    new environment.

    Each backend should implement a subclass and provide a singleton instance
    as the FACTORY member of the module.
    """

    @staticmethod
    @abstractmethod
    def load_workspace(batch: bool, verbose: bool, parsed_uri: ParseResult) -> Workspace:
        """Instantiate and return a workspace.
        """
        pass

    @staticmethod
    @abstractmethod
    def init_workspace(
        workspace_name: str,
        dws_version: str,
        global_params: JSONDict,
        local_params: JSONDict,
        batch: bool,
        verbose: bool,
        *args,
        **kwargs,
    ) -> Workspace:
        pass

    @staticmethod
    @abstractmethod
    def clone_workspace(local_params: JSONDict, batch: bool, verbose: bool, *args) -> Workspace:
        """Clone an existing workspace into the local environment. Note that
        hostname is used as an instance identifier (TODO: make this more generic).

        This only clones the workspace itself, any local state resources should be
        cloned separately.

        If a workspace has no local state, this factory method might not do anything.
        """
        pass


def _get_factory(backend_mod_name: str) -> WorkspaceFactory:
    try:
        m = importlib.import_module(backend_mod_name)
    except ImportError as e:
        raise ConfigurationError("Unable to load workspace backend '%s'" % backend_mod_name) from e
    if not hasattr(m, "FACTORY"):
        raise InternalError(
            "Workspace backend %s does not provide a FACTORY attribute" % backend_mod_name
        )
    factory = m.FACTORY  # type: ignore
    if not isinstance(factory, WorkspaceFactory):
        raise InternalError(
            "Workspace backend factory has type '%s', " % backend_mod_name
            + "not a subclass of WorkspaceFactory"
        )
    return factory


def load_workspace(uri: str, batch: bool, verbose: bool) -> Workspace:
    """Given a requested workspace backend, and backend-specific
    parameters, instantiate and return a workspace. The workspace
    is specified by a uri, where the backend-type is the scheme and
    rest is interpreted by the backend.

    The backend name / scheme is used to load a backend module
    whose name is dataworkspaces.backends.SCHEME.
    """
    parsed_uri = urlparse(uri)
    return _get_factory("dataworkspaces.backends." + parsed_uri.scheme).load_workspace(
        batch, verbose, parsed_uri
    )


def _find_containing_workspace(start_dir: Optional[str] = None) -> Optional[str]:
    """For commands that execute in the context of a containing
    workspace, find the nearest containging workspace and return
    its absolute path. If none is found, return None.
    """
    if start_dir:
        curr_base = os.path.abspath(os.path.expanduser(start_dir))
    else:
        curr_base = os.path.abspath(os.path.expanduser(os.path.curdir))
    while curr_base != "/":
        if os.path.isdir(os.path.join(curr_base, ".dataworkspace")) and os.access(
            curr_base, os.W_OK
        ):
            return curr_base
        else:
            curr_base = os.path.dirname(curr_base)
    return None


def find_and_load_workspace(
    batch: bool, verbose: bool, uri_or_local_path: Optional[str] = None
) -> Workspace:
    """This tries to find the workspace and load it. There are three cases:

    1. If uri_or_local_path is a uri, we call load_workspace() directly
    2. If uri_or_local_path is specified, but not a uri, we interpret it as a local path
       and try to instantitate a git-backend workspace at that location in the loca filesystem.
    3. If uri_or_local_path is not specified, we start at the current directory
       and search up the directory tree until we find something that looks like a
       git backend workspace.

    TODO: In the future, this should also look for a config file that might specify the
    workspace or list workspaces by name.
    """
    if uri_or_local_path is not None:
        if ":" not in uri_or_local_path:
            return load_workspace("git:" + uri_or_local_path, batch, verbose)
        else:
            return load_workspace(uri_or_local_path, batch, verbose)
    else:
        ws_dir = _find_containing_workspace()
        if ws_dir is not None:
            return load_workspace("git:" + ws_dir, batch, verbose)
        else:
            raise ConfigurationError(
                "Did not find a data workspace enclosing the diretory %s"
                % os.path.abspath(os.path.expanduser(os.path.curdir))
            )


def init_workspace(
    backend_name: str,
    workspace_name: str,
    hostname: str,
    batch: bool,
    verbose: bool,
    scratch_dir: str,
    *args,
    **kwargs,
) -> Workspace:
    """Given a requested workspace backend, and backend-specific parameters,
    initialize a new workspace, then instantitate and return it.

    A backend name is a module name. The module should have an init_workspace()
    function defined.

    TODO: the hostname should be generalized as an "instance name", but we
    also need backward compatibility.
    TODO: is this function now redundant? Compare to :func:`~load_workspace()`.
    """
    import dataworkspaces

    return _get_factory(backend_name).init_workspace(
        workspace_name,
        dataworkspaces.__version__,
        get_global_param_defaults(),
        get_local_param_defaults(hostname),
        batch,
        verbose,
        scratch_dir,
        *args,
        **kwargs,
    )


def clone_workspace(
    backend_name: str, hostname: str, batch: bool, verbose: bool, *args
) -> Workspace:
    """Instantiate the workspace factory based on backend name and then clone the
    specified workspace to the local environment."""
    return _get_factory(backend_name).clone_workspace(
        get_local_param_defaults(hostname), batch, verbose, *args
    )


class ResourceRoles:
    """This class defines constants for the four
    resource roles.
    """

    SOURCE_DATA_SET = "source-data"
    INTERMEDIATE_DATA = "intermediate-data"
    CODE = "code"
    RESULTS = "results"


RESOURCE_ROLE_CHOICES = [
    ResourceRoles.SOURCE_DATA_SET,
    ResourceRoles.INTERMEDIATE_DATA,
    ResourceRoles.CODE,
    ResourceRoles.RESULTS,
]

# short explanation of each role
RESOURCE_ROLE_PURPOSES = {
    ResourceRoles.SOURCE_DATA_SET: "source data",
    ResourceRoles.INTERMEDIATE_DATA: "intermediate data",
    ResourceRoles.CODE: "code",
    ResourceRoles.RESULTS: "experimental results",
}


class Resource(metaclass=ABCMeta):
    """Base class for all resources"""

    def __init__(self, resource_type: str, name: str, role: str, workspace: Workspace):
        #: attribute: name for this resource's type (e.g. git, local-files, etc.) (str)
        self.resource_type = resource_type
        #: attribute: unique name for this resource within the workspace (str)
        self.name = name
        #: Role of the resource, one of :class:`~ResourceRoles`
        self.role = role
        #: attribute: The workspace that contains this resource (Workspace)
        self.workspace = workspace
        # setup our parameter definitions
        self.param_defs = ResourceParams()

    def has_results_role(self):
        return self.role == ResourceRoles.RESULTS

    def get_params(self) -> JSONDict:
        """Get the parameters that define the configuration
        of the resource globally.
        """
        return self.param_defs.get_params(self)

    @abstractmethod
    def validate_subpath_exists(self, subpath: str) -> None:
        """Validate that the subpath is valid within this
        resource. Otherwise should raise a ConfigurationError."""
        pass

    def is_exported(self) -> bool:
        """Returns True if this resource has an export parameter and it
        is True.
        """
        return hasattr(self, "export") and getattr(self, "export") == True

    def is_imported(self) -> bool:
        """Returns True if this resource has an imported parameter and it
        is True.
        """
        return hasattr(self, "imported") and getattr(self, "imported") == True


class FileResourceMixin(metaclass=ABCMeta):
    """This is a mixin to be implemented by resources
    which provide a hierarchy of files. Examples include
    a git repo, local filesystem, or S3 bucket. A
    database would be a resource that does NOT implement this
    API.
    """

    @abstractmethod
    def results_move_current_files(
        self, rel_dest_root: str, exclude_files: Set[str], exclude_dirs_re: Pattern
    ) -> None:
        """A snapshot is being taken, and we want to move the
        files in the resource to the relative subdirectory rel_dest_root.
        We should exclude the files in the set exclude_files and exclude
        any directories matching exclude_dirs_re (e.g. the directory to
        which the files are being moved).
        """
        pass

    @abstractmethod
    def results_copy_current_files(
        self, rel_dest_root: str, exclude_files: Set[str], exclude_dirs_re: Pattern
    ) -> None:
        """A snapshot is being taken, and we want to copy the
        files in the resource to the relative subdirectory rel_dest_root.
        We should exclude the files in the set exclude_files and exclude
        any directories matching exclude_dirs_re (e.g. the directory to
        which the files are being moved).

        By default results_move_current_files() is called, but the copy is used when
        we export the resource.
        """
        pass

    @abstractmethod
    def add_results_file(self, data: Union[JSONDict, JSONList], rel_dest_path: str) -> None:
        """Save JSON results data to the specified path in the resource. Note that,
        although this is usually used for results role resources, it could also be
        used for intermediate-data resources if they are exported (causing the lineage
        file to be written to the resource).

        TODO: this is used for both results and lineage files. Perhaps we should either rename
        it to something like add_json_file() or create a separate call for lineage.
        """
        pass

    @abstractmethod
    def upload_file(self, src_local_path: str, rel_dest_path: str) -> None:
        """Copy a local file to the specified path in the
        resource. This may be a local copy or an upload, depending
        on the resource implmentation
        """
        pass

    @abstractmethod
    def read_results_file(self, subpath: str) -> JSONDict:
        """Read and parse json results data from the specified path
        in the resource. If the path does not exist or is not a file
        throw a ConfigurationError.
        """
        pass

    @abstractmethod
    def does_subpath_exist(
        self, subpath: str, must_be_file: bool = False, must_be_directory: bool = False
    ) -> bool:
        """Return True the subpath is valid within this
        resource, False otherwise. If must_be_file is True,
        return True only if the subpath corresponds to content.
        If must_be_directory is True, return True only if the subpath
        corresponds to a directory.
        """
        pass

    @abstractmethod
    def delete_file(self, rel_path: str) -> None:
        """Delete a file from the resource. If the resource is read-only or
        otherwise does not support modifications, should throw a NotSupportedError.
        """
        pass

    @abstractmethod
    def open(self, rel_path:str, mode:str):
        """Returns a file like object in the specified mode.
        """
        pass

    @abstractmethod
    def ls(self, rel_path:str) -> List[str]:
        """List the files under the relative path (use empty string for root)"""
        pass


class LocalStateResourceMixin(metaclass=ABCMeta):
    """Mixin for the resource api for resources with local state
    that need to be "cloned"
    """

    def get_local_params(self) -> JSONDict:
        """Get the parameters that define any local configuration of
        the resource (e.g. local filepaths)
        """
        return cast(Resource, self).param_defs.get_local_params(cast(Resource, self))

    @abstractmethod
    def get_local_path_if_any(self) -> Optional[str]:
        """If the resource has an associated local path on the system,
        return it. Othewise, return None. Even if it has local state,
        this might not be a file-based resource. Thus, the return
        value is an Optional string.
        """
        pass

    def validate_subpath_exists(self, subpath: str) -> None:
        """Validate that the subpath is valid within this
        resource. Default implementation checks the local
        filesystem if any. If the resource is not file-based,
        then the subclass should override this method to
        implement the check.
        """
        lp = self.get_local_path_if_any()
        if lp is not None:
            path = os.path.join(lp, subpath)
            if not os.path.exists(
                path
            ):  # use exists() instead of isdir() as subpath could be a file
                raise ConfigurationError(
                    "Subpath %s does not exist for resource %s, expecting it at '%s'"
                    % (subpath, cast(Workspace, self).name, path)
                )

    @abstractmethod
    def pull_precheck(self):
        """Perform any prechecks before updating this resource from the
        remote origin.
        """
        pass

    @abstractmethod
    def pull(self):
        """Update this resource with the latest changes from the remote
        origin.
        """
        pass

    @abstractmethod
    def push_precheck(self):
        """Perform any prechecks before uploading this resource's changes to the
        remote origin.
        """
        pass

    @abstractmethod
    def push(self):
        """Upload this resource's changes to the remote origin.
        """
        pass


class ResourceFactory(metaclass=ABCMeta):
    """Abstract factory class to be implemented for each
    resource type.
    """

    @abstractmethod
    def from_command_line(
        self, role: str, name: str, workspace: Workspace, *args, **kwargs
    ) -> Resource:
        """Instantiate a resource object from the add command's
        arguments"""
        pass

    @abstractmethod
    def from_json(self, params: JSONDict, local_params: JSONDict, workspace: Workspace) -> Resource:
        """Instantiate a resource object from saved params and local params"""
        pass

    @abstractmethod
    def has_local_state(self) -> bool:
        """Return true if this resource has local state and needs
        a clone step the first time it is used.
        """
        pass

    @abstractmethod
    def clone(self, params: JSONDict, workspace: Workspace) -> LocalStateResourceMixin:
        """Instantiate a local copy of the resource 
        that came from the remote origin. We don't yet have local params,
        since this resource is not yet on the local machine. If not in batch
        mode, this method can ask the user for any additional information needed
        (e.g. a local path). In batch mode, should either come up with a reasonable
        default or error out if not enough information is available."""
        pass

    @abstractmethod
    def suggest_name(self, workspace: Workspace, role: str, *args) -> str:
        """Given the arguments passed in to create a resource,
        suggest a name for the case where the user did not provide one
        via --name. This will be used by suggest_resource_name() to
        find a short, but unique name for the resource.
        """
        pass


def _get_resource_factory_by_resource_type(resource_type):
    import dataworkspaces.resources.resource_types

    RT = dataworkspaces.resources.resource_types.RESOURCE_TYPES
    if resource_type not in RT:
        raise InternalError(
            "'%s' not a valid resource type. Valid types are: %s."
            % (resource_type, ", ".join(sorted(RT.keys())))
        )
    f = RT[resource_type]()
    assert isinstance(f, ResourceFactory), "Expecting ResourceFactory, class was %s" % type(f)
    return f


####################################################################
#      Mixins for Synchronized and Centralized workspaces          #
####################################################################
class SyncedWorkspaceMixin(metaclass=ABCMeta):
    """This mixin is for workspaces that support synchronizing with a master
    copy via push/pull operations.
    """

    def _pull_resources_precheck(self, resource_list: List[LocalStateResourceMixin]) -> None:
        """Default calls pull_precheck() on each of the supplied resources.
        """
        for r in resource_list:
            r.pull_precheck()

    @abstractmethod
    def pull_workspace(self) -> "SyncedWorkspaceMixin":
        """Pull the workspace itself and return a new workspace object reflecting
        the latest state changes.
        """
        pass

    def pull_resources(self, resource_list: List[LocalStateResourceMixin]) -> None:
        """Download latest updates from remote origin. By default,
        includes any resources that support syncing via the
        LocalStateResourceMixin.

        Note that this does not handle the actual workspace pull or the
        cloning of new resources.
        """
        self._pull_resources_precheck(resource_list)
        assert isinstance(self, Workspace)
        for r in resource_list:
            assert isinstance(r, Resource)
            print("[pull] pulling resource %s" % r.name)
            r.pull()
        print("[pull] all resources pulled successfully.")


        # We need to clear the current lineage for pulled resources since we
        # don't know what the pull command did to it.
        if isinstance(self, SnapshotWorkspaceMixin) and self.supports_lineage():
            instance = self.get_instance()
            lstore = self.get_lineage_store()
            for r in resource_list:
                assert isinstance(r, Resource)
                if self.verbose:
                    print("Clearing lineage on resource %s" % r.name)
                lstore.clear_entry(instance, ResourceRef(r.name, None))
                if r.is_imported():
                    cast(SnapshotResourceMixin, r).copy_imported_lineage(lstore)
                    if self.verbose:
                        print("Imported lineage for %s" % r.name)

    def _push_precheck(self, resource_list: List[LocalStateResourceMixin]) -> None:
        """Default calls pull_precheck() on each of the supplied resources.
        """
        for r in resource_list:
            r.push_precheck()

    def push(self, resource_list: List[LocalStateResourceMixin]) -> None:
        """Upload updates to remote origin.

        Backend subclass also needs to handle syncing of the workspace itself.
        If this is called with an empty set of resources, then we
        are just syncing the workspace. Pushing the workspace should include
        pushing of any new resources.
        """
        self._push_precheck(resource_list)

        for r in resource_list:
            print("[push] pushing resource %s" % cast(Resource, r).name)
            r.push()
        print("[push] all resources pushed successfully.")

    @abstractmethod
    def publish(self, *args) -> None:
        """Make a local repo available at a remote location. For example,
         we may make it available on GitHub, GitLab or some similar service.
        """
        pass


class CentralWorkspaceMixin(metaclass=ABCMeta):
    """This mixin is for workspaces that have a central store
    and do not need synchronization of the workspace itself.
    They still may need to sychronize individual resources.
    """

    def _pull_resources_precheck(self, resource_list: List[LocalStateResourceMixin]) -> None:
        """Default calls pull_precheck() on each of the supplied resources.
        """
        for r in resource_list:
            r.pull_precheck()

    def pull_resources(self, resource_list: List[LocalStateResourceMixin]) -> None:
        """Download latest resource updates from remote origin
        for resources that support syncing via the
        LocalStateResourceMixin.
        """
        pass
        self._pull_resources_precheck(resource_list)

        for r in resource_list:
            r.pull()

    @abstractmethod
    def get_resources_that_need_to_be_cloned(self) -> List[str]:
        """Return a list of resources with local state that are not present
        in the local system. This is used after a pull to clone these resources.
        """
        pass

    def _push_resources_precheck(self, resource_list: List[LocalStateResourceMixin]) -> None:
        """Default calls pull_precheck() on each of the supplied resources.
        """
        for r in resource_list:
            r.push_precheck()

    def push_resources(self, resource_list: List[LocalStateResourceMixin]) -> None:
        """Upload resource updates to remote origin.
        """
        self._push_resources_precheck(resource_list)

        for r in resource_list:
            r.push()


####################################################################
#               Mixins for Snapshot functionality                  #
####################################################################


class SnapshotMetadata:
    """The metadata we store for each snapshot (in addition to the manifest).
    relative_destination_path refers to the path used in resources that copy their current
    state to a subdirectory for each snapshot.
    """

    def __init__(
        self,
        hashval: str,
        tags: List[str],
        message: str,
        hostname: str,
        timestamp: str,
        relative_destination_path: str,
        restore_hashes: Dict[str, Optional[str]],
        metrics: Optional[JSONDict] = None,
        updated_timestamp: Optional[str] = None,
    ):
        self.hashval = hashval.lower()  # always normalize to lower case
        self.tags = tags
        self.message = message
        self.hostname = hostname
        self.timestamp = timestamp
        self.relative_destination_path = relative_destination_path
        self.restore_hashes = restore_hashes
        self.metrics = metrics
        self.updated_timestamp = updated_timestamp

    def has_tag(self, tag):
        return True if tag in self.tags else False

    def matches_partial_hash(self, partial_hash):
        """A partial hash matches if the full hash starts with it,
        normalizing to lower case.
        """
        return True if self.hashval.startswith(partial_hash.lower()) else False

    def to_json(self) -> JSONDict:
        v = {
            "hash": self.hashval,
            "tags": self.tags,
            "message": self.message,
            "hostname": self.hostname,
            "timestamp": self.timestamp,
            "relative_destination_path": self.relative_destination_path,
            "restore_hashes": self.restore_hashes,
            "metrics": self.metrics,
        }
        if self.updated_timestamp is not None:
            v["updated_timestamp"] = self.updated_timestamp
        return v

    @staticmethod
    def from_json(data: JSONDict) -> "SnapshotMetadata":
        return SnapshotMetadata(
            data["hash"],
            data["tags"],
            data["message"],
            data["hostname"],
            data["timestamp"],
            data["relative_destination_path"],
            data["restore_hashes"],
            data.get("metrics"),
            data.get("updated_timestamp"),
        )

    def __str__(self):
        return json.dumps(self.to_json())


class SnapshotWorkspaceMixin(metaclass=ABCMeta):
    """Mixin class for workspaces that support snapshots and restores.
    """

    @abstractmethod
    def get_next_snapshot_number(self) -> int:
        """Return a number that can be used for this snapshot. For a given
        local copy of thw workspace, it is guaranteed to be unique and increasing.
        It is not guarenteed to be globally unique (need to combine with hostname
        to get that).
        """
        pass

    def _snapshot_precheck(self, current_resources: Iterable[Resource]) -> None:
        """Run any prechecks before taking a snapshot. This should throw
        a ConfigurationError if the snapshot would fail for some reason.
        It generally just calls snapshot_precheck() on each of the resources.

        This method is called by snapshot()
        """
        for r in current_resources:
            if isinstance(r, SnapshotResourceMixin):
                r.snapshot_precheck()

    @abstractmethod
    def save_snapshot_metadata_and_manifest(
        self, metadata: SnapshotMetadata, manifest: bytes
    ) -> None:
        """
        Save the snapshot metadata and manifest using the hash in metadata.hashval.
        """
        pass

    def snapshot(
        self, tag: Optional[str] = None, message: str = ""
    ) -> Tuple[SnapshotMetadata, bytes]:
        """Take snapshot of the resources in the workspace, and metadata
        for the snapshot and a manifest in the workspace.
        We assume that the tag does not already exist
        (checks can be made in the command before calling this method).

        We also copy the lineage data if the workspace supports lineage.

        Returns the snapshot metadata and the (binary) snapshot hash. These
        should be saved into the workspace by the caller (i.e. the snapshot command).
        We don't do that here, as futher interactions with the user may be needed. In
        particular, if the hash is identical to a previous hash, we ask the user
        if they want to overwrite.
        """
        # First, we figure out the snapshot subdirectory in results
        # resources.
        snapshot_number = self.get_next_snapshot_number()
        snapshot_timestamp = datetime.datetime.now()
        workspace = cast(Workspace, self)
        exclude_files = set(workspace.get_global_param(RESULTS_MOVE_EXCLUDE_FILES))
        results_dir_template = workspace.get_global_param(RESULTS_DIR_TEMPLATE)
        username = getpass.getuser()
        hostname = workspace.get_local_param(HOSTNAME)
        validate_template(results_dir_template)
        # relative path to which we will move results files
        rel_dest_root = expand_dir_template(
            results_dir_template, username, hostname, snapshot_timestamp, snapshot_number, tag
        )
        exclude_dirs_re = re.compile(make_re_pattern_for_dir_template(results_dir_template))
        # Load the resource representation and run the prechecks
        current_resources = [r for r in cast(Workspace, self).get_resources()]

        self._snapshot_precheck(current_resources)

        # For exported resources, we need to delete any stale lineage.json files before the snapshot
        for r in current_resources:
            if (
                r.is_exported()
                and isinstance(r, FileResourceMixin)
                and r.does_subpath_exist("lineage.json")
            ):
                r.delete_file("lineage.json")

        # now, move the files for result resources
        resources_with_moved_files = []  # type: List[str]
        metrics = None
        for r in current_resources:
            if r.has_results_role() and isinstance(r, FileResourceMixin):
                file_mixin = cast(FileResourceMixin, r)
                if (metrics is None) and file_mixin.does_subpath_exist("results.json"):
                    data = file_mixin.read_results_file("results.json")
                    if isinstance(data, dict) and "metrics" in data:
                        metrics = data["metrics"]
                if not r.is_exported():
                    file_mixin.results_move_current_files(
                        rel_dest_root, exclude_files, exclude_dirs_re
                    )
                    resources_with_moved_files.append(r.name)
                else:  # copy the results, but leave the current results in-place
                    file_mixin.results_copy_current_files(
                        rel_dest_root, exclude_files, exclude_dirs_re
                    )

        # Take the actual snapshot
        manifest = []
        map_of_restore_hashes = {}  # type: Dict[str,Optional[str]]
        # compare hashes used for lineage
        map_of_compare_hashes = {}  # type: Dict[str,str]
        # now take the actual snapshots
        for r in current_resources:
            if isinstance(r, SnapshotResourceMixin):
                print("[snapshot] Taking snapshot of resource %s"% r.name)
                (compare_hash, restore_hash) = r.snapshot()
            else:
                (compare_hash, restore_hash) = (None, None)
            if compare_hash is not None:
                map_of_compare_hashes[r.name] = compare_hash
            map_of_restore_hashes[r.name] = restore_hash
            entry = cast(Workspace, self)._get_resource_params(r.name)
            entry["hash"] = compare_hash
            manifest.append(entry)
        print("[shapshot] all resources snapshotted successfully.")
        manifest_bytes = json.dumps(manifest, indent=2).encode("utf-8")
        manifest_hash = hash_bytes(manifest_bytes)

        metadata = SnapshotMetadata(
            manifest_hash,
            [tag] if tag else [],
            message,
            hostname,
            snapshot_timestamp.isoformat(),
            rel_dest_root,
            map_of_restore_hashes,
            metrics,
        )

        if self.supports_lineage():
            instance = workspace.get_instance()
            lstore = self.get_lineage_store()
            lstore.replace_placeholders(instance, map_of_compare_hashes, verbose=workspace.verbose)
            lstore.snapshot_lineage(instance, manifest_hash, [r.name for r in current_resources])

            # TODO: consider whether the writing of lineage data and the clearing
            # of results resources should be done outside of this method.

            # We write the lineage data out after the snapshot, as we want it to
            # include everything from the snapshot. Since results resources are
            # additive, we won't be missing anything if we
            # restore to this snapshot.
            self.write_result_lineage_for_snapshot(
                current_resources, metadata.relative_destination_path
            )
            self.write_export_lineage_for_snapshot(current_resources)

            # For all the results resources for which we moved the files to a
            # snapshot-specific subdirectory, we need to clear the lineage.
            # This needs to happen after the previous step.
            for rname in resources_with_moved_files:
                lstore.clear_entry(instance, ResourceRef(rname, None))
                if cast(Workspace, self).verbose:
                    print("Cleared lineage for results resource %s" % rname)
        return metadata, manifest_bytes

    def _restore_precheck(
        self,
        restore_hashes: Dict[str, Optional[str]],
        restore_resources: List["SnapshotResourceMixin"],
    ) -> None:
        """Run any prechecks before restoring to the specified hash value
        (aka certificate). This should throw a ConfigurationError if the
        restore would fail for some reason. The default calls the
        restore_precheck() for each resource in the list. Subclasses can
        override to add more checks.

        This method is called by restore()
        """
        for r in restore_resources:
            hashval = restore_hashes[cast(Resource, r).name]
            assert hashval is not None
            r.restore_precheck(hashval)

    def restore(
        self,
        snapshot_hash: str,
        restore_hashes: Dict[str, Optional[str]],
        restore_resources: List["SnapshotResourceMixin"],
    ) -> None:
        """Restore the specified resources to the specified hashes.
        The list should have been previously filtered to include only
        those with valid (not None) restore hashes.
        """
        self._restore_precheck(restore_hashes, restore_resources)

        for r in restore_resources:
            hashval = restore_hashes[cast(Resource, r).name]
            assert hashval is not None
            r.restore(hashval)

        if self.supports_lineage():
            assert isinstance(self, Workspace)
            self.get_lineage_store().restore_lineage(
                self.get_instance(),
                snapshot_hash,
                [cast(Resource, r).name for r in restore_resources],
                verbose=self.verbose,
            )

    @abstractmethod
    def get_snapshot_metadata(self, hash_val: str) -> SnapshotMetadata:
        """Given the full hash of a snapshot, return the metadata. This
        lookup should be quick.
        """
        pass

    @abstractmethod
    def get_snapshot_by_tag(self, tag: str) -> SnapshotMetadata:
        """Given a tag, return the asssociated snapshot metadata.
        This lookup could be slower ,if a reverse index is not kept."""
        pass

    @abstractmethod
    def get_snapshot_by_partial_hash(self, partial_hash: str) -> SnapshotMetadata:
        """Given a partial hash for the snapshot, find the snapshot whose hash
        starts with this prefix and return the metadata
        asssociated with the snapshot.
        """
        pass

    def get_snapshot_by_tag_or_hash(self, tag_or_hash: str) -> SnapshotMetadata:
        """Given a string that is either a tag or a (partial)hash corresponding to a
        snapshot, return the associated resrouce metadata. Throws a ConfigurationError
        if no entry is found.
        """
        if is_a_git_hash(tag_or_hash):
            return self.get_snapshot_metadata(tag_or_hash)
        elif is_a_shortened_git_hash(tag_or_hash):
            return self.get_snapshot_by_partial_hash(tag_or_hash)
        else:
            return self.get_snapshot_by_tag(tag_or_hash)

    @abstractmethod
    def _get_snapshot_manifest_as_bytes(self, hash_val: str) -> bytes:
        """Retrieve the manifest for this snapshot. This manifest
        was given to the backend via
        :func:`~save_snapshot_metadata_and_manifest~. This should
        throw a ConfigurationError if no associated snapshot is found
        for the hash.
        """
        pass

    def get_snapshot_manifest(self, hash_val: str) -> JSONList:
        """Returns the snapshot manifest for the given hash
        as a parsed JSON structure. The top-level dict maps
        resource names resource parameters.
        """
        raw_data = self._get_snapshot_manifest_as_bytes(hash_val)
        return json.loads(raw_data.decode("utf-8"))

    @abstractmethod
    def list_snapshots(
        self, reverse: bool = True, max_count: Optional[int] = None
    ) -> Iterable[SnapshotMetadata]:
        """Returns an iterable of snapshot metadata, sorted by timestamp ascending
        (or descending if reverse is True). If max_count is specified, return at
        most that many snaphsots.
        """
        pass

    def get_most_recent_snapshot(self) -> Optional[SnapshotMetadata]:
        """Helper function to return the metadata for the most recent
        snapshot (by timestamp). Returns None if no snapshot found
        """
        l = [s for s in self.list_snapshots(reverse=True, max_count=1)]
        if len(l) == 0:
            return None
        elif len(l) == 1:
            return l[0]
        else:
            assert 0, "list_snapshots returned more than max_count of 1"

    @abstractmethod
    def remove_tag_from_snapshot(self, hash_val: str, tag: str) -> None:
        """Remove the specified tag from the specified snapshot. Throw an
        InternalError if either the snapshot or the tag do not exist.
        """
        pass

    @abstractmethod
    def _delete_snapshot_metadata_and_manifest(self, hash_val: str) -> None:
        """Given a snapshot hash, delete the associated metadata.
        """
        pass

    def delete_snapshot(self, hash_val: str, include_resources=False) -> None:
        """Given a snapshot hash, delete the entry from the workspace's metadata.
        If include_resources is True, then delete any data from the associated resources
        (e.g. snapshot subdirectories).
        """
        try:
            md = self.get_snapshot_metadata(hash_val)
        except Exception:
            raise ConfigurationError("Did not find metadata associated with snapshot %s" % hash_val)
        if include_resources:
            current_resources = frozenset(cast(Workspace, self).get_resource_names())
            to_delete = current_resources.intersection(frozenset(md.restore_hashes.keys()))
            for rname in to_delete:
                r = cast(Workspace, self).get_resource(rname)
                if isinstance(r, SnapshotResourceMixin):
                    delete_hash = md.restore_hashes[rname]
                    if delete_hash is not None:
                        r.delete_snapshot(md.hashval, delete_hash, md.relative_destination_path)
                    else:
                        print("Cannot delete snapshot for resource %s, no restore hash" % rname)
        self._delete_snapshot_metadata_and_manifest(hash_val)
        if self.supports_lineage():
            instance = cast(Workspace, self).get_instance()
            self.get_lineage_store().delete_snapshot_lineage(instance, hash_val)

    @abstractmethod
    def supports_lineage(self) -> bool:
        """Return True if this workspace's backend supports lineage,
        False otherwise
        """
        pass

    @abstractmethod
    def get_lineage_store(self) -> LineageStore:
        """Return the store for lineage data. If this workspace backend
        does not support lineage for some reason, the call should
        raise a ConfigurationError.
        """
        pass

    def write_result_lineage_for_snapshot(
        self, current_resources: List[Resource], rel_dest_path: str
    ) -> None:
        """For all results resources, we write out the lineage.json files
        in the snapshot directory.
        """
        assert self.supports_lineage()
        assert isinstance(self, Workspace)
        instance = self.get_instance()
        store = self.get_lineage_store()
        for r in current_resources:
            if r.role != ResourceRoles.RESULTS:
                continue
            if not isinstance(r, FileResourceMixin):
                continue
            (lineage, warnings) = store.get_lineage_for_resource(instance, r.name)
            if len(lineage) > 0:
                lineage_dict = {
                    "resource_name": r.name,
                    "complete": warnings == 0,
                    "lineages": [l.to_json() for l in lineage],
                }
                r.add_results_file(lineage_dict, os.path.join(rel_dest_path, "lineage.json"))
                print("Wrote lineage for %s to lineage.json" % r.name)
            elif self.verbose:
                print("No lineage available for %s, not writing a lineage.json file" % r.name)

    def write_export_lineage_for_snapshot(self, current_resources: List[Resource]) -> None:
        """For all exported resources, we write out the lineage.json file in the
        root directoryfor the resource.
        """
        assert self.supports_lineage()
        assert isinstance(self, Workspace)
        instance = self.get_instance()
        store = self.get_lineage_store()
        for r in current_resources:
            if not isinstance(r, FileResourceMixin):
                continue
            if not r.is_exported():
                continue
            (lineage, warnings) = store.get_lineage_for_resource(instance, r.name)
            if len(lineage) > 0:
                lineage_dict = {
                    "resource_name": r.name,
                    "complete": warnings == 0,
                    "lineages": [l.to_json() for l in lineage],
                }
                r.add_results_file(lineage_dict, "lineage.json")
                print("Exported lineage for %s to lineage.json" % r.name)
            elif self.verbose:
                print(
                    "No lineage available for %s, not writing an exported lineage.json file"
                    % r.name
                )


class SnapshotResourceMixin(metaclass=ABCMeta):
    """Mixin for the resource api for resources that can take snapshots.
    """

    @abstractmethod
    def snapshot_precheck(self) -> None:
        """Run any prechecks before taking a snapshot. This should throw
        a ConfigurationError if the snapshot would fail for some reason.
        """
        pass

    @abstractmethod
    def snapshot(self) -> Tuple[Optional[str], Optional[str]]:
        """Take the actual snapshot of the resource and return a tuple
        of two hash values, the first for comparison, and the second for restoring.
        The comparison hash value is the one we save in the snapshot manifest. The
        restore hash value is saved in the snapshot metadata.
        In many cases both hashes are the same. If the resource does not support
        restores, it can return None for the second hash. This will cause
        attempted restores involving this resource to error out.
        """
        pass

    @abstractmethod
    def restore_precheck(self, restore_hashval: str) -> None:
        """Run any prechecks before restoring to the specified hash value
        (aka certificate). This should throw a ConfigurationError if the
        restore would fail for some reason.
        """
        pass

    @abstractmethod
    def restore(self, restore_hashval: str) -> None:
        pass

    @abstractmethod
    def delete_snapshot(
        self, workspace_snapshot_hash: str, resource_restore_hash: str, relative_path: str
    ) -> None:
        """Delete any state associated with the snapshot, including any
        files under relative_path
        """
        pass

    def copy_imported_lineage(self, lineage_store: LineageStore) -> None:
        """If imported lineage, copy the lineage.json file to the lineage store.
        The pull_resources() method on the workspace will call it after pulling the resource.

        If the resource does not store files locally, this default implementation
        will need to be overridden.
        """
        assert isinstance(self, LocalStateResourceMixin)
        local_path = cast(LocalStateResourceMixin, self).get_local_path_if_any()
        assert local_path is not None
        rname = cast(Resource, self).name
        lineage_path = os.path.join(local_path, "lineage.json")
        if not os.path.exists(lineage_path):
            raise ConfigurationError(
                "%s was an imported resource, but is missing exported lineage file at %s"
                % (rname, lineage_path)
            )
        with open(lineage_path, "r") as f:
            lineage_data = json.load(f)
        if lineage_data["resource_name"] != rname:
            raise ConfigurationError(
                "Resource name in imported lineage '%s' does not match '%s'"
                % (lineage_data["resource_name"], rname)
            )
        lineage_store.import_lineage_file(rname, lineage_data["lineages"])
