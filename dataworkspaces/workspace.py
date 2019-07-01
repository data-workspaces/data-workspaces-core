"""
Main definition of the workspace abstraction
"""

from typing import Dict, Any, Iterable, Optional, List, Tuple, NamedTuple

from abc import ABCMeta, abstractmethod

class Workspace(metaclass=ABCMeta):
    def __init__(self, name:str, dws_version:str):
        self.name = name
        self.dws_version = dws_version

    @abstractmethod
    def get_global_params(self) -> Dict[str,Any]:
        """Get a dict of configuration parameters for this workspace,
        which apply across all instances.
        """
        pass

    @abstractmethod
    def get_local_params(self) -> Dict[str,Any]:
        """Get a dict of configuration parameters for this particular
        install of the workspace (e.g. local filesystem paths, hostname).
        """
        pass

    @abstractmethod
    def set_global_param(self, name:str, value:Any) -> None:
        """Setting does not necessarily take effect until save() is called"""
        pass

    @abstractmethod
    def set_local_param(self, name:str, value:Any) -> None:
        """Setting does not necessarily take effect until save() is called"""
        pass

    @abstractmethod
    def get_resource_names(self) -> Iterable[str]: pass

    @abstractmethod
    def get_resource(self, name:str) -> 'Resource':
        """Get the associated resource from the workspace metadata.
        """
        pass

    @abstractmethod
    def add_resource(self, r:Resource) -> None:
        """Add a resource to the repository for tracking.
        """
        pass

    @abstractmethod
    def clone_resource(self, name:str, local_params:Dict[str,Any]) -> 'Resource':
        """Instantiate the resource locally with the specified local parameters.
        This is used in cases where the resource has local state.
        """
        pass

    @abstractmethod
    def validate_resource_name(self, resource_name:str, subpath:Optional[str]=None,
                               expected_type:Optional[str]=None) -> None:
        """Validate that the given resource name and optional subpath
        are valid in the current state of the workspace. Otherwise throws
        a ConfigurationError.
        """
        pass

    @abstractmethod
    def save(self) -> None:
        """Save the current state of the workspace"""
        pass

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

class ResourceStates:
    NEW='new'
    ADDED='added'
    CLONED='cloned'

class Resource(metaclass=ABCMeta):
    """Base class for all resources"""
    def __init__(self, scheme:str, name:str, role:str, state:str, workspace:Workspace):
        self.scheme = scheme
        self.name = name
        self.role = role
        self.state = state
        self.workspace = workspace

    def has_results_role(self):
        return self.role==ResourceRoles.RESULTS

    @abstractmethod
    def get_params(self) -> Dict[str,Any]:
        """Get the parameters that define the configuration
        of the resource globally.
        """
        pass

    @abstractmethod
    def validate_subpath_exists(self, subpath:str) -> None:
        """Validate that the subpath is valid within this
        resource. Otherwise should raise a ConfigurationError."""
        pass


class LocalStateResourceMixin(metaclass=ABCMeta):
    """Mixin for the resource api for resources with local state
    that need to be "cloned"
    """
    @abstractmethod
    def get_local_params(self) -> Dict[str,Any]:
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



####################################################################
#               Mixins for Snapshot functionality                  #
####################################################################

class SnapshotMetadata:
    def __init__(self, hashval:str, tags:List[str],
                 message:str, hostname:str,
                 timestamp:str,
                 restore_hashes:Dict[str,str]):
        self.hashval = hashval
        self.tags = tags
        self.message = message
        self.hostname = hostname
        self.timestamp = timestamp
        self.restore_hashes = restore_hashes


class SnapshotWorkspaceMixin(metaclass=ABCMeta):
    """Mixin class for workspaces that support snapshots and restores.
    """
    @abstractmethod
    def snapshot_precheck(self) -> None:
        """Run any prechecks before taking a snapshot. This should throw
        a ConfigurationError if the snapshot would fail for some reason.
        It generally just calls snapshot_precheck() on each of the resources.
        """
        pass

    @abstractmethod
    def snapshot(self, tag:Optional[str]=None, message:str='') -> str:
        """Take snapshot of the resources in the workspace, and metadata
        for the snapshot and a manifest in the workspace. Returns a hash
        of the manifest.
        """
        pass

    @abstractmethod
    def restore_prechecks(self, restore_tag_or_hash:str,
                          only:Optional[List[str]]=None,
                          leave:Optional[List[str]]=None) \
         -> None:
        """Run any prechecks before restoring to the specified tag or hash value
        (aka certificate). This should throw a ConfigurationError if the
        restore would fail for some reason.
        """
        pass

    @abstractmethod
    def restore(self, restore_tag_or_hash:str,
                only:Optional[List[str]]=None,
                leave:Optional[List[str]]=None) \
         -> None:
        pass

    @abstractmethod
    def get_snapshot_metadata(self, tag_or_hash:str) -> SnapshotMetadata: pass

    @abstractmethod
    def list_snapshots(self, max_count:Optional[int]=None) \
        -> Iterable[SnapshotMetadata]:
        pass


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
    def restore_prechecks(self, restore_hashval:str) -> None:
        """Run any prechecks before restoring to the specified hash value
        (aka certificate). This should throw a ConfigurationError if the
        restore would fail for some reason.
        """
        pass

    @abstractmethod
    def restore(self, restore_hashval:str) -> None:
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


class LineageBase(metaclass=ABCMeta):
    """Abstract base class for lineage
    """
    pass

class LineageWorkspaceMixin(SnapshotResourceMixin):
    """Mixin class for workspaces that support a lineage store.
    This builds on the snapshots, so we extend fromt he Snapshot Workspace
    API.

    The lineage store should store resource ref to lineage mappings.
    When a snapshot takes place, the linage for the affected resources
    is saved. When a restore takes place, the lineage for the affected
    resources is restored as well.

    TODO: This needs some concept of either local state or an execution
    id, particularly in the case where there is a single centralized store.
    """
    @abstractmethod
    def add_lineage(self, ref:ResourceRef, lineage:LineageBase) -> None:pass

    @abstractmethod
    def get_lineage_for_ref(self, ref:ResourceRef) -> Optional[LineageBase]:
        """Returns the current lineage associated with the resource ref
        or None if there is no lineage data presensent.
        """
        pass

    @abstractmethod
    def get_lineages_for_resource(self, resource_name:str) -> List[LineageBase]:
        """Return a list of lineage objects associated with the resource.
        """
