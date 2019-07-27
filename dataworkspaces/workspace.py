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

from typing import Dict, Any, Iterable, Optional, List, Tuple, NamedTuple, Set, cast, Pattern

from abc import ABCMeta, abstractmethod
import importlib
import os.path
import datetime
import getpass
import json
import re

from dataworkspaces.errors import ConfigurationError, InternalError
from dataworkspaces.utils.hash_utils import is_a_git_hash, is_a_shortened_git_hash
from dataworkspaces.utils.param_utils import PARAM_DEFS, LOCAL_PARAM_DEFS,\
                                             get_global_param_defaults,\
                                             get_local_param_defaults,\
                                             ParamNotFoundError,\
                                             RESULTS_DIR_TEMPLATE,\
                                             RESULTS_MOVE_EXCLUDE_FILES,\
                                             HOSTNAME
from dataworkspaces.utils.snapshot_utils import validate_template,\
                                                expand_dir_template,\
                                                make_re_pattern_for_dir_template
from dataworkspaces.utils.hash_utils import hash_bytes

# Standin for a JSON object/dict. The value type is overly
# permissive, as mypy does not yet support recursive types.
JSONDict=Dict[str,Any]

class Workspace(metaclass=ABCMeta):
    def __init__(self, name:str, dws_version:str,
                 batch:bool=False, verbose:bool=False):
        """Required properties are the workspace name
        and the version of dws that created it.
        batch and verbose are for the command line interface.
        """
        self.name = name
        self.dws_version = dws_version
        self.batch = batch
        self.verbose = verbose

    @abstractmethod
    def _get_global_params(self) -> JSONDict:
        """Get a dict of configuration parameters for this workspace,
        which apply across all instances. This contains only those
        parameters which are set during initialization or excplicitly
        set by the user. get_global_param() will combine these with
        system-defined defaults.
        """
        pass

    def get_global_param(self, param_name:str) -> Any:
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
            raise ParamNotFoundError("No global parameter with name '%s'"%
                                     param_name)

    @abstractmethod
    def _get_local_params(self) -> JSONDict:
        """Get a dict of configuration parameters for this particular
        install of the workspace (e.g. local filesystem paths, hostname).
        This contains only those parameters which are set during initialization
        or explicitly set by the user. get_local_param will combine these
        with system-defined defaults.
        """
        pass

    def get_local_param(self, param_name:str) -> Any:
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
            raise ParamNotFoundError("No local parameter with name '%s'"%
                                     param_name)

    @abstractmethod
    def _set_global_param(self, name:str, value:Any) -> None:
        """Implementation of low level saving by the backend.
        Setting does not necessarily take effect until save() is called"""
        pass

    def set_global_param(self, name:str, value:Any) -> None:
        """Validate and set a global parameter.
        Setting does not necessarily take effect until save() is called
        """
        if name not in PARAM_DEFS:
            raise ParamNotFoundError("No global parameter named '%s'"%name)
        PARAM_DEFS[name].validate(value)
        self._set_global_param(name, value)

    @abstractmethod
    def _set_local_param(self, name:str, value:Any) -> None:
        """Setting does not necessarily take effect until save() is called"""
        pass

    def set_local_param(self, name:str, value:Any) -> None:
        """Validate and set a local parameter.
        Setting does not necessarily take effect until save() is called
        """
        if name not in LOCAL_PARAM_DEFS:
            raise ParamNotFoundError("No local parameter named '%s'"%name)
        LOCAL_PARAM_DEFS[name].validate(value)
        self._set_local_param(name, value)

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
        return params['role']

    def get_resource_type(self, resource_name) -> str:
        """Get the type of a resource without having to instantiate it.
        """
        params = self._get_resource_params(resource_name)
        return params['resource_type']

    @abstractmethod
    def _get_resource_local_params(self, resource_name:str) -> Optional[JSONDict]:
        """If a resource has local parameters defined for it, return them.
        Otherwise, return None.
        """
        pass

    @abstractmethod
    def _add_params_for_resource(self, resource_name:str, params:JSONDict)->None:
        """
        Add the params for a new resource in this workspace
        """
        pass

    @abstractmethod
    def _add_local_params_for_resource(self, resource_name:str,
                                       local_params:JSONDict) -> None:
        """
        Add the local params either coming from a cloned or a new resource.
        """
        pass

    def get_resource(self, name:str) -> 'Resource':
        """Get the associated resource from the workspace metadata.
        """
        params = self._get_resource_params(name)
        resource_type = params['resource_type']
        f = _get_resource_factory_by_resource_type(resource_type)
        local_params = self._get_resource_local_params(name)
        if f.has_local_state() and local_params is None:
            raise InternalError("Resource '%s' has local state and needs to be cloned"%
                                name)
        return f.from_json(params, local_params if local_params is not None else {},
                           self)

    def get_resources(self) -> Iterable['Resource']:
        """Iterate through all the resources
        """
        for rname in self.get_resource_names():
            yield self.get_resource(rname)

    def add_resource(self, name:str, resource_type:str, role:str, *args, **kwargs)\
        -> 'Resource':
        """Add a resource to the repository for tracking.
        """
        if name in self.get_resource_names():
            raise ConfigurationError("Attempting to add a resource '%s', but there is already one with that name in the workspace"%
                                     name)
        if role not in RESOURCE_ROLE_CHOICES:
            raise ConfigurationError("Invalid resource role '%s'" % role)
        f = _get_resource_factory_by_resource_type(resource_type)
        r = f.from_command_line(role, name, self, *args, **kwargs)
        self._add_params_for_resource(r.name, r.get_params())
        self._add_local_params_for_resource(r.name, r.get_local_params())
        return r

    def clone_resource(self, name:str) -> 'LocalStateResourceMixin':
        """Instantiate the resource locally.
        This is used in cases where the resource has local state.
        """
        if name not in self.get_resource_names():
            raise ConfigurationError("A resource by the name '%s' does not exist in this workspace"%
                                     name)
        resource_type =  self.get_resource_type(name)
        f = _get_resource_factory_by_resource_type(resource_type)
        assert f.has_local_state() # should only be calling if local state
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

    def validate_resource_name(self, resource_name:str, subpath:Optional[str]=None,
                               expected_role:Optional[str]=None) -> None:
        """Validate that the given resource name and optional subpath
        are valid in the current state of the workspace. Otherwise throws
        a ConfigurationError.
        """
        if resource_name not in self.get_resource_names():
            raise ConfigurationError("No resource named '%s'" % resource_name)
        r = self.get_resource(resource_name)
        if subpath is not None:
            r.validate_subpath_exists(subpath)
        if expected_role and r.role!=expected_role:
            raise ConfigurationError("Expected resource '%s' to be in role '%s', but role was '%s'"%
            (resource_name, expected_role, r.role))

    def validate_local_path_for_resource(self, proposed_resource_name:str,
                                         proposed_local_path:str) -> None:
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
        if self.get_workspace_local_path_if_any()is not None:
            if os.path.realpath(cast(str, self.get_workspace_local_path_if_any())) \
                                ==real_local_path:
                raise ConfigurationError("Cannot use the entire workspace as a resource local path")
        for r in self.get_resources():
            if not isinstance(r, LocalStateResourceMixin) or \
               r.get_local_path_if_any() is None:
                continue
            other_real_path = os.path.realpath(r.get_local_path_if_any())
            if other_real_path.startswith(real_local_path) or \
               real_local_path.startswith(other_real_path):
                raise ConfigurationError("Proposed path %s for resource %s, conflicts with local path %s for resource %s"%
                                         (proposed_local_path, proposed_resource_name,
                                          r.get_local_path_if_any(), r.name))

    def suggest_resource_name(self, resource_type:str,
                              role:str, *args):
        """Given the arguments passed in for creating a resource, suggest
        a (unique) name for the resource.
        """
        name = _get_resource_factory_by_resource_type(resource_type).suggest_name(self,
                                                                                  *args)
        existing_resource_names = frozenset(self.get_resource_names())
        if name not in existing_resource_names:
            return name
        longer_name = name + '-' + role
        if longer_name not in existing_resource_names:
            return longer_name
        i = 2
        while True:
            numbered_name = longer_name + '-' + str(i)
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
    def save(self, message:str) -> None:
        """Save the current state of the workspace"""
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
    def load_workspace(batch:bool, verbose:bool, *args, **kwargs) -> Workspace:
        """Instantiate and return a workspace.
        """
        pass

    @staticmethod
    @abstractmethod
    def init_workspace(workspace_name:str, dws_version:str,
                       global_params:JSONDict, local_params:JSONDict,
                       batch:bool, verbose:bool,
                       *args, **kwargs) -> Workspace: pass

    @staticmethod
    @abstractmethod
    def clone_workspace(local_params:JSONDict, batch:bool, verbose:bool,
                        *args) -> Workspace:
        """Clone an existing workspace into the local environment. Note that
        hostname is used as an instance identifier (TODO: make this more generic).

        This only clones the workspace itself, any local state resources should be
        cloned separately.

        If a workspace has no local state, this factory method might not do anything.
        """
        pass


def _get_factory(backend_name:str) -> WorkspaceFactory:
    try:
        m = importlib.import_module(backend_name)
    except ImportError as e:
        raise ConfigurationError("Unable to load workspace backend '%s'"%
                                 backend_name) from e
    if not hasattr(m, 'FACTORY'):
        raise InternalError("Workspace backend %s does not provide a FACTORY attribute"%
                            backend_name)
    factory = m.FACTORY # type: ignore
    if not isinstance(factory, WorkspaceFactory):
        raise InternalError("Workspace backend factory has type '%s', "%backend_name +
                            "not a subclass of WorkspaceFactory")
    return factory


def load_workspace(backend_name:str, batch:bool, verbose:bool, *args, **kwargs) -> Workspace:
    """Given a requested workspace backend, and backend-specific
    parameters, instantiate and return a workspace.

    A backend name is a module name. The module should have a
    load_workspace() function defined.
    """
    return _get_factory(backend_name).load_workspace(batch, verbose, *args, **kwargs)


def init_workspace(backend_name:str, workspace_name:str, hostname:str,
                   batch:bool, verbose:bool,
                   *args, **kwargs) -> Workspace:
    """Given a requested workspace backend, and backend-specific parameters,
    initialize a new workspace, then instantitate and return it.

    A backend name is a module name. The module should have an init_workspace()
    function defined.

    TODO: the hostname should be generalized as an "instance name", but we
    also need backward compatibility.
    """
    import dataworkspaces
    return _get_factory(backend_name)\
             .init_workspace(workspace_name, dataworkspaces.__version__,
                             get_global_param_defaults(),
                             get_local_param_defaults(hostname),
                             batch, verbose, *args, **kwargs)

def clone_workspace(backend_name:str, hostname:str, batch:bool, verbose:bool, *args) -> Workspace:
    """Instantiate the workspace factory based on backend name and then clone the
    specified workspace to the local environment."""
    return _get_factory(backend_name).clone_workspace(get_local_param_defaults(hostname), batch, verbose, *args)


class ResourceRoles:
    SOURCE_DATA_SET='source-data'
    INTERMEDIATE_DATA='intermediate-data'
    CODE='code'
    RESULTS='results'

RESOURCE_ROLE_CHOICES = [
    ResourceRoles.SOURCE_DATA_SET,
    ResourceRoles.INTERMEDIATE_DATA,
    ResourceRoles.CODE,
    ResourceRoles.RESULTS
]

# short explanation of each role
RESOURCE_ROLE_PURPOSES = {
    ResourceRoles.SOURCE_DATA_SET:"source data",
    ResourceRoles.INTERMEDIATE_DATA:"intermediate data",
    ResourceRoles.CODE:"code",
    ResourceRoles.RESULTS:"experimental results"
}


class Resource(metaclass=ABCMeta):
    """Base class for all resources"""
    def __init__(self, resource_type:str, name:str, role:str, workspace:Workspace):
        self.resource_type = resource_type
        self.name = name
        self.role = role
        self.workspace = workspace

    def has_results_role(self):
        return self.role==ResourceRoles.RESULTS

    @abstractmethod
    def get_params(self) -> JSONDict:
        """Get the parameters that define the configuration
        of the resource globally.
        """
        pass

    @abstractmethod
    def validate_subpath_exists(self, subpath:str) -> None:
        """Validate that the subpath is valid within this
        resource. Otherwise should raise a ConfigurationError."""
        pass


class FileResourceMixin(metaclass=ABCMeta):
    """This is a mixin to be implemented by resources
    which provide a hierarchy of files. Examples include
    a git repo, local filesystem, or S3 bucket. A
    database would be a resource that does NOT implement this
    API.
    """
    @abstractmethod
    def results_move_current_files(self, rel_dest_root:str, exclude_files:Set[str],
                                   exclude_dirs_re:Pattern) -> None:
        """A snapshot is being taken, and we want to move the
        files in the resource to the relative subdirectory rel_dest_root.
        We should exclude the files in the set exclude_files and exclude
        any directories matching exclude_dirs_re (e.g. the directory to
        which the files are being moved).
        """
        pass

    @abstractmethod
    def add_results_file(self, data:JSONDict, rel_dest_path:str) -> None:
        """Save JSON results data to the specified path in the resource.
        """
        pass

class LocalStateResourceMixin(metaclass=ABCMeta):
    """Mixin for the resource api for resources with local state
    that need to be "cloned"
    """
    @abstractmethod
    def get_local_params(self) -> JSONDict:
        """Get the parameters that define any local configuration of
        the resource (e.g. local filepaths)
        """
        pass

    @abstractmethod
    def get_local_path_if_any(self):
        """If the resource has an associated local path on the system,
        return it. Othewise, return None.
        """
        pass

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
    def from_command_line(self, role:str, name:str, workspace:Workspace,
                          *args, **kwargs) -> Resource:
        """Instantiate a resource object from the add command's
        arguments"""
        pass

    @abstractmethod
    def from_json(self, params:JSONDict, local_params:JSONDict,
                  workspace:Workspace) -> Resource:
        """Instantiate a resource object from saved params and local params"""
        pass

    @abstractmethod
    def has_local_state(self) -> bool:
        """Return true if this resource has local state and needs
        a clone step the first time it is used.
        """
        pass

    @abstractmethod
    def clone(self, params:JSONDict, workspace:Workspace) -> LocalStateResourceMixin:
        """Instantiate a local copy of the resource 
        that came from the remote origin. We don't yet have local params,
        since this resource is not yet on the local machine. If not in batch
        mode, this method can ask the user for any additional information needed
        (e.g. a local path). In batch mode, should either come up with a reasonable
        default or error out if not enough information is available."""
        pass

    @abstractmethod
    def suggest_name(self, workspace:Workspace, *args) -> str:
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
        raise InternalError("'%s' not a valid resource type. Valid types are: %s."
                            % (resource_type, ', '.join(sorted(RT.keys()))))
    f = RT[resource_type]()
    assert isinstance(f, ResourceFactory), \
        "Expecting ResourceFactory, class was %s" % type(f)
    return f


####################################################################
#      Mixins for Synchronized and Centralized workspaces          #
####################################################################
class SyncedWorkspaceMixin(metaclass=ABCMeta):
    """This mixin is for workspaces that support synchronizing with a master
    copy via push/pull operations.
    """
    def _pull_resources_precheck(self, resource_list:List[LocalStateResourceMixin]) -> None:
        """Default calls pull_precheck() on each of the supplied resources.
        """
        for r in resource_list:
            r.pull_precheck()

    @abstractmethod
    def pull_workspace(self) -> 'SyncedWorkspaceMixin':
        """Pull the workspace itself and return a new workspace object reflecting
        the latest state changes.
        """
        pass

    def pull_resources(self, resource_list:List[LocalStateResourceMixin]) -> None:
        """Download latest updates from remote origin. By default,
        includes any resources that support syncing via the
        LocalStateResourceMixin.

        Note that this does not handle the actual workspace pull or the
        cloning of new resources.
        """
        self._pull_resources_precheck(resource_list)
        for r in resource_list:
            r.pull()

    def _push_precheck(self, resource_list:List[LocalStateResourceMixin]) -> None:
        """Default calls pull_precheck() on each of the supplied resources.
        """
        for r in resource_list:
            r.push_precheck()

    def push(self,  resource_list:List[LocalStateResourceMixin]) -> None:
        """Upload updates to remote origin.

        Backend subclass also needs to handle syncing of the workspace itself.
        If this is called with an empty set of resources, then we
        are just syncing the workspace. Pushing the workspace should include
        pushing of any new resources.
        """
        self._push_precheck(resource_list)

        for r in resource_list:
            r.push()

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
    def _pull_resources_precheck(self, resource_list:List[LocalStateResourceMixin]) -> None:
        """Default calls pull_precheck() on each of the supplied resources.
        """
        for r in resource_list:
            r.pull_precheck()

    def pull_resources(self, resource_list:List[LocalStateResourceMixin]) -> None:
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

    def _push_resources_precheck(self, resource_list:List[LocalStateResourceMixin]) -> None:
        """Default calls pull_precheck() on each of the supplied resources.
        """
        for r in resource_list:
            r.push_precheck()

    def push_resources(self,  resource_list:List[LocalStateResourceMixin]) -> None:
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
    def __init__(self, hashval:str,
                 tags:List[str],
                 message:str,
                 hostname:str,
                 timestamp:str,
                 relative_destination_path:str,
                 restore_hashes:Dict[str,Optional[str]],
                 metric_name:Optional[str]=None,
                 metric_value:Optional[Any]=None,
                 updated_timestamp:Optional[str]=None):
        self.hashval = hashval.lower() # always normalize to lower case
        self.tags = tags
        self.message = message
        self.hostname = hostname
        self.timestamp = timestamp
        self.relative_destination_path = relative_destination_path
        self.restore_hashes = restore_hashes
        self.metric_name = metric_name
        self.metric_value = metric_value
        self.updated_timestamp=updated_timestamp

    def has_tag(self, tag):
        return True if tag in self.tags else False

    def matches_partial_hash(self, partial_hash):
        """A partial hash matches if the full hash starts with it,
        normalizing to lower case.
        """
        return True if self.hashval.startwith(partial_hash.lower()) else False

    def to_json(self) -> JSONDict:
        v = {
            'hash':self.hashval,
            'tags':self.tags,
            'message':self.message,
            'hostname':self.hostname,
            'timestamp':self.timestamp,
            'relative_destination_path':self.relative_destination_path,
            'restore_hashes':self.restore_hashes,
            'metric_name':self.metric_name,
            'metric_value':self.metric_value
        }
        if self.updated_timestamp is not None:
            v['updated_timestamp'] = self.updated_timestamp
        return v

    @staticmethod
    def from_json(data:JSONDict) -> 'SnapshotMetadata':
        return SnapshotMetadata(data['hash'],
                                data['tags'],
                                data['message'],
                                data['hostname'],
                                data['timestamp'],
                                data['relative_destination_path'],
                                data['restore_hashes'],
                                data.get('metric_name'),
                                data.get('metric_value'),
                                data.get('updated_timestamp'))

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

    def _snapshot_precheck(self, current_resources:Iterable[Resource]) -> None:
        """Run any prechecks before taking a snapshot. This should throw
        a ConfigurationError if the snapshot would fail for some reason.
        It generally just calls snapshot_precheck() on each of the resources.

        This method is called by snapshot()
        """
        for r in current_resources:
            if isinstance(r, SnapshotResourceMixin):
                r.snapshot_precheck()

    @abstractmethod
    def save_snapshot_metadata_and_manifest(self, metadata:SnapshotMetadata,
                                            manifest:bytes) -> None:
        """
        Save the snapshot metadata and manifest using the hash in metadata.hashval.
        """
        pass

    def snapshot(self, tag:Optional[str]=None, message:str='', metric_name:Optional[str]=None,
                 metric_value:Optional[Any]=None) -> Tuple[SnapshotMetadata, bytes]:
        """Take snapshot of the resources in the workspace, and metadata
        for the snapshot and a manifest in the workspace.
        We assume that the tag does not already exist
        (checks can be made in the command before calling this method).

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
        workspace = cast(Workspace,self)
        exclude_files = set(workspace.get_global_param(RESULTS_MOVE_EXCLUDE_FILES))
        results_dir_template = workspace.get_global_param(RESULTS_DIR_TEMPLATE)
        username = getpass.getuser()
        hostname = workspace.get_local_param(HOSTNAME)
        validate_template(results_dir_template)
        # relative path to which we will move results files
        rel_dest_root = expand_dir_template(results_dir_template, username, hostname,
                                            snapshot_timestamp, snapshot_number,
                                            tag)
        exclude_dirs_re = re.compile(make_re_pattern_for_dir_template(results_dir_template))
        # Load the resource representation and run the prechecks
        current_resources = [r for r in cast(Workspace, self).get_resources()]

        self._snapshot_precheck(current_resources)

        # now, move the files for result resources
        for r in current_resources:
            if r.has_results_role() and isinstance(r, FileResourceMixin):
                cast(FileResourceMixin, r).results_move_current_files(rel_dest_root, exclude_files,
                                                                      exclude_dirs_re)

        manifest = {}
        map_of_restore_hashes = {} # type: Dict[str,Optional[str]]
        # now take the actual snapshots
        for r in current_resources:
            if isinstance(r, SnapshotResourceMixin):
                (compare_hash, restore_hash) = r.snapshot()
            else:
                (compare_hash, restore_hash) = (None, None)
            map_of_restore_hashes[r.name] = restore_hash
            manifest[r.name] = cast(Workspace, self)._get_resource_params(r.name)
            manifest[r.name]['hash'] = compare_hash
        manifest_bytes = json.dumps(manifest, indent=2).encode('utf-8')
        manifest_hash = hash_bytes(manifest_bytes)

        metadata = SnapshotMetadata(manifest_hash, [tag] if tag else [], message,
                                    hostname, snapshot_timestamp.isoformat(),
                                    rel_dest_root,
                                    map_of_restore_hashes,
                                    metric_name, metric_value)
        return metadata, manifest_bytes
        
    def _restore_precheck(self, restore_hashes:Dict[str,str],
                          restore_resources:List['SnapshotResourceMixin']) -> None:
        """Run any prechecks before restoring to the specified hash value
        (aka certificate). This should throw a ConfigurationError if the
        restore would fail for some reason. The default calls the
        restore_precheck() for each resource in the list. Subclasses can
        override to add more checks.

        This method is called by restore()
        """
        for r in restore_resources:
            r.restore_precheck(restore_hashes[cast(Resource, r).name])

    def restore(self, restore_hashes:Dict[str,str],
                restore_resources:List['SnapshotResourceMixin']) -> None:
        """Restore the specified resources to the specified hashes.
        The list should have been previously filtered to include only
        those with valid (not None) restore hashes.
        """
        self._restore_precheck(restore_hashes, restore_resources)

        for r in restore_resources:
            r.restore(restore_hashes[cast(Resource, r).name])

    @abstractmethod
    def get_snapshot_metadata(self, hash_val:str) -> SnapshotMetadata:
        """Given the full hash of a snapshot, return the metadata. This
        lookup should be quick.
        """
        pass

    @abstractmethod
    def get_snapshot_by_tag(self, tag:str) -> SnapshotMetadata:
        """Given a tag, return the asssociated snapshot metadata.
        This lookup could be slower ,if a reverse index is not kept."""
        pass

    @abstractmethod
    def get_snapshot_by_partial_hash(self, partial_hash:str) -> SnapshotMetadata:
        """Given a partial hash for the snapshot, find the snapshot whose hash
        starts with this prefix and return the metadata
        asssociated with the snapshot.
        """
        pass

    def get_snapshot_by_tag_or_hash(self, tag_or_hash:str) -> SnapshotMetadata:
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
    def _get_snapshot_manifest_as_bytes(self, hash_val:str) -> bytes:
        """Retrieve the manifest for this snapshot. This manifest
        was given to the backend via
        :func:`~save_snapshot_metadata_and_manifest~. This should
        throw a ConfigurationError if no associated snapshot is found
        for the hash.
        """
        pass

    def get_snapshot_manifest(self, hash_val:str) -> JSONDict:
        """Returns the snapshot manifest for the given hash
        as a parsed JSON structure. The top-level dict maps
        resource names resource parameters.
        """
        raw_data = self._get_snapshot_manifest_as_bytes(hash_val)
        return json.loads(raw_data.decode('utf-8'))

    @abstractmethod
    def list_snapshots(self, reverse:bool=True, max_count:Optional[int]=None) \
        -> Iterable[SnapshotMetadata]:
        """Returns an iterable of snapshot metadata, sorted by timestamp ascending
        (or descending if reverse is True). If max_count is specified, return at
        most that many snaphsots.
        """
        pass

    @abstractmethod
    def remove_tag_from_snapshot(self, hash_val:str, tag:str) -> None:
        """Remove the specified tag from the specified snapshot. Throw an
        InternalError if either the snapshot or the tag do not exist.
        """
        pass

    @abstractmethod
    def _delete_snapshot_metadata_and_manifest(self, hash_val:str)-> None:
        """Given a snapshot hash, delete the associated metadata.
        """
        pass

    def delete_snapshot(self, hash_val:str, include_resources=False)-> None:
        """Given a snapshot hash, delete the entry from the workspace's metadata.
        If include_resources is True, then delete any data from the associated resources
        (e.g. snapshot subdirectories).
        """
        try:
            md = self.get_snapshot_metadata(hash_val)
        except Exception:
            raise ConfigurationError("Did not find metadata associated with snapshot %s"
                                     % hash_val)
        if include_resources:
            current_resources = frozenset(cast(Workspace, self).get_resource_names())
            to_delete = current_resources.intersection(frozenset(md.restore_hashes.keys()))
            for rname in to_delete:
                r = cast(Workspace, self).get_resource(rname)
                if isinstance(r, SnapshotResourceMixin):
                    r.delete_snapshot(md.hashval, md.restore_hashes[rname],
                                      md.relative_destination_path)
        self._delete_snapshot_metadata_and_manifest(hash_val)


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
    def snapshot(self) -> Tuple[str, str]:
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
    def restore_precheck(self, restore_hashval:str) -> None:
        """Run any prechecks before restoring to the specified hash value
        (aka certificate). This should throw a ConfigurationError if the
        restore would fail for some reason.
        """
        pass

    @abstractmethod
    def restore(self, restore_hashval:str) -> None:
        pass

    @abstractmethod
    def delete_snapshot(self, workspace_snapshot_hash:str, resource_restore_hash:str,
                        relative_path:str) -> None:
        """Delete any state associated with the snapshot, including any
        files under relative_path
        """
        pass


####################################################################
#                Mixins for Lineage functionality                  #
####################################################################

class ResourceRef(NamedTuple):
    """A namedtuple that is to indentify a resource or path within
    a resource for lineage purposes (e.g. the input or output of a
    workflow step).
    The ``name`` parameter is the name of a resource. The optional
    ``subpath`` parameter is a relative path within that resource.
    The subpath lets you store inputs/outputs from multiple steps
    within the same resource and track them independently.
    """
    name: str
    subpath: Optional[str] = None


class LineageWorkspaceMixin(SnapshotResourceMixin):
    """Mixin class for workspaces that support a lineage store.
    This builds on the snapshots, so we extend from the Snapshot Workspace
    API.

    The lineage store should store resource ref to lineage mappings.
    When a snapshot takes place, the linage for the affected resources
    is saved. When a restore takes place, the lineage for the affected
    resources is restored as well.

    TODO: This needs some concept of either local state or an execution
    id, particularly in the case where there is a single centralized store.
    """
    @abstractmethod
    def add_lineage(self, ref:ResourceRef, lineage_data:JSONDict) -> None:pass

    @abstractmethod
    def get_lineage_for_ref(self, ref:ResourceRef) -> Optional[JSONDict]:
        """Returns the current lineage associated with the resource ref
        or None if there is no lineage data presensent.
        """
        pass

    @abstractmethod
    def get_lineages_for_resource(self, resource_name:str) -> List[JSONDict]:
        """Return a list of lineage objects associated with the resource.
        """
        pass

