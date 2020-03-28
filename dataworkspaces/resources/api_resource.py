import os
from os.path import join, exists
from typing import Tuple, Optional, List, Any
import hashlib

import click

from dataworkspaces.errors import ConfigurationError, InternalError
from dataworkspaces.workspace import (
    Workspace,
    Resource,
    ResourceRoles,
    SnapshotResourceMixin,
    LocalStateResourceMixin,
    ResourceFactory,
    JSONDict,
)

API_RESOURCE_TYPE = "api-resource"


class ApiResource(Resource, LocalStateResourceMixin, SnapshotResourceMixin):
    """This is a resource type for an API that has to be called to get data.
    It is only valid for source data and intermediate resources.

    To get the hash for a snapshot, a caller needs to go through the data in memory
    and compute the hash. This is usually done by "monkey-patching" a framework's
    API to get the data. The hash is stored in a local scratch directory which
    is read when the snapshot is taken.

    This resource inherits from LocalStateResourceMixin so that we can get
    a clone call to initialze the scratch directory when the workspace or
    individual resource is cloned.
    """

    def __init__(self, name: str, role: str, workspace: Workspace):
        super().__init__(API_RESOURCE_TYPE, name, role, workspace)
        self.hash_states = []  # type: List[Any]

    def validate_subpath_exists(self, subpath: str) -> None:
        raise ConfigurationError(
            "Subpath %s is not valid for resource %s: API resources do not support subpaths"
            % (subpath, self.name)
        )

    def get_local_path_if_any(self) -> Optional[str]:
        return None

    def pull_precheck(self):
        pass

    def pull(self):
        pass

    def push_precheck(self):
        pass

    def push(self):
        pass

    def snapshot_precheck(self) -> None:
        pass

    def snapshot(self) -> Tuple[Optional[str], Optional[str]]:
        scratch = self.workspace._get_local_scratch_space_for_resource(self.name)
        hashfile = join(scratch, "hashval.txt")
        if exists(hashfile):
            with open(hashfile, "r") as f:
                data = f.read().rstrip()
            return (data, None)
        else:
            click.echo("WARNING: no hash available for resource %s" % self.name)
            return (None, None)

    def restore_precheck(self, restore_hashval: str) -> None:
        raise InternalError("Attempt to restore resource %s, which is not restoreable" % self.name)

    def restore(self, restore_hashval: str) -> None:
        raise InternalError("Attempt to restore resource %s, which is not restoreable" % self.name)

    def delete_snapshot(
        self, workspace_snapshot_hash: str, resource_restore_hash: str, relative_path: str
    ) -> None:
        pass

    def init_hash_state(self) -> None:
        """Drop whatever was there before and init with a fresh hash object.
        Use this when starting training.
        """
        self.hash_states = [hashlib.sha1()]

    def dup_hash_state(self) -> None:
        """Push a copy of current TOS. Used when we want to start
        testing. It should already have the training state. We'll
        add a new hash for test data and then pop it when done testing.
        """
        assert len(self.hash_states) > 0
        self.hash_states.append(self.hash_states[-1].copy())

    def pop_hash_state(self) -> None:
        assert len(self.hash_states) > 0
        del self.hash_states[-1]

    def get_hash_state(self):
        assert len(self.hash_states) > 0
        return self.hash_states[-1]

    def save_current_hash(self, comment: Optional[str] = None) -> None:
        """Save the current hash state to the scratch space. If a
        comment is provided, it is written to a separate file.
        """
        assert len(self.hash_states) > 0
        hashval = self.hash_states[-1].hexdigest()
        scratch = self.workspace._get_local_scratch_space_for_resource(self.name)
        hashfile = join(scratch, "hashval.txt")
        with open(hashfile, "w") as f:
            f.write(hashval)
        if self.workspace.verbose:
            print("dws>> %s: wrote hashval of '%s' to %s'" % (self.name, hashval, hashfile))
        commentfile = join(scratch, "comment.txt")
        if comment is not None:
            with open(commentfile, "w") as f:
                f.write(comment + "\n")
        else:
            if exists(commentfile):
                os.remove(commentfile)


class ApiResourceFactory(ResourceFactory):
    def from_command_line(
        self, role: str, name: str, workspace: Workspace, *args, **kwargs
    ) -> Resource:
        """Instantiate a resource object from the add command's
        arguments"""
        if role not in (ResourceRoles.SOURCE_DATA_SET, ResourceRoles.INTERMEDIATE_DATA):
            raise ConfigurationError(
                "API resources only supported for %s and %s roles"
                % (ResourceRoles.SOURCE_DATA_SET, ResourceRoles.INTERMEDIATE_DATA)
            )
        workspace._get_local_scratch_space_for_resource(name, create_if_not_present=True)
        return ApiResource(name, role, workspace)

    def from_json(self, params: JSONDict, local_params: JSONDict, workspace: Workspace) -> Resource:
        """Instantiate a resource object from saved params and local params"""
        return ApiResource(params["name"], params["role"], workspace)

    def has_local_state(self) -> bool:
        """Return true if this resource has local state and needs
        a clone step the first time it is used.

        We return True because we have the local scratch space for the hashes
        that needs to be set up during a clone.
        """
        return True

    def clone(self, params: JSONDict, workspace: Workspace) -> LocalStateResourceMixin:
        """Instantiate a local copy of the resource 
        that came from the remote origin. We don't yet have local params,
        since this resource is not yet on the local machine. If not in batch
        mode, this method can ask the user for any additional information needed
        (e.g. a local path). In batch mode, should either come up with a reasonable
        default or error out if not enough information is available."""
        workspace._get_local_scratch_space_for_resource(params["name"], create_if_not_present=True)
        return ApiResource(params["name"], params["role"], workspace)

    def suggest_name(self, workspace: Workspace, role: str, *args) -> str:
        """Given the arguments passed in to create a resource,
        suggest a name for the case where the user did not provide one
        via --name. This will be used by suggest_resource_name() to
        find a short, but unique name for the resource.
        """
        return role + "-api"
