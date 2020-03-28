"""
Git backend for storing a workspace
"""

import os
from os.path import (
    exists,
    join,
    isdir,
    basename,
    isabs,
    abspath,
    expanduser,
    dirname,
    curdir,
    commonpath,
)
import shutil
import json
import re
import uuid
from urllib.parse import ParseResult, urlparse
from typing import Any, Iterable, Optional, List, Dict, Tuple, cast

assert Dict  # make pyflakes happy

import click

import dataworkspaces.workspace as ws
from dataworkspaces.workspace import JSONDict, SnapshotMetadata
from dataworkspaces.errors import ConfigurationError, InternalError
from dataworkspaces.utils.subprocess_utils import call_subprocess
from dataworkspaces.utils.git_utils import (
    commit_changes_in_repo,
    git_init,
    git_add,
    is_git_dirty,
    is_pull_needed_from_remote,
    GIT_EXE_PATH,
    set_remote_origin,
    verify_git_config_initialized,
    git_remove_file,
    git_remove_subtree,
    ensure_entry_in_gitignore,
)
from dataworkspaces.utils.git_fat_utils import (
    validate_git_fat_in_path_if_needed,
    run_git_fat_pull_if_needed,
    validate_git_fat_in_path,
    run_git_fat_push_if_needed,
    setup_git_fat_for_repo,
    is_a_git_fat_repo,
)
from dataworkspaces.utils.git_lfs_utils import (
    init_git_lfs,
    is_a_git_lfs_repo,
    ensure_git_lfs_configured_if_needed,
)
from dataworkspaces.utils.file_utils import safe_rename, get_subpath_from_absolute
from dataworkspaces.utils.param_utils import (
    HOSTNAME,
    init_scratch_directory,
    clone_scratch_directory,
    get_scratch_directory,
    SCRATCH_DIRECTORY,
    LOCAL_SCRATCH_DIRECTORY,
)
from dataworkspaces.utils.lineage_utils import (
    FileLineageStore,
    LineageStore,
    ResourceRef,
    ResourceLineage,
)


BASE_DIR = ".dataworkspace"
GIT_IGNORE_FILE_PATH = ".dataworkspace/.gitignore"
CONFIG_FILE_PATH = ".dataworkspace/config.json"
LOCAL_PARAMS_PATH = ".dataworkspace/local_params.json"
RESOURCES_FILE_PATH = ".dataworkspace/resources.json"
RESOURCE_LOCAL_PARAMS_PATH = ".dataworkspace/resource_local_params.json"
SNAPSHOT_DIR_PATH = ".dataworkspace/snapshots"
SNAPSHOT_METADATA_DIR_PATH = ".dataworkspace/snapshot_metadata"
CURRENT_LINEAGE_DIR_PATH = ".dataworkspace/current_lineage"
SNAPSHOT_LINEAGE_DIR_PATH = ".dataworkspace/snapshot_lineage"


class GitFileLineageStore(FileLineageStore):
    """Subclass of file lineage store that adds
    the lineage files to the git repo.
    """

    def __init__(self, workspace: "Workspace"):
        super().__init__(
            cast(Workspace, workspace).get_instance(),
            join(workspace.workspace_dir, CURRENT_LINEAGE_DIR_PATH),
            join(workspace.workspace_dir, SNAPSHOT_LINEAGE_DIR_PATH),
        )
        self.workspace = workspace

    def _add_to_git(self, path: str):
        ws_dir = cast(str, self.workspace.workspace_dir)
        git_add(
            ws_dir,
            [get_subpath_from_absolute(ws_dir, path)],  # type: ignore
            verbose=self.workspace.verbose,
        )

    def _save_rfile_to_snapshot(
        self,
        resource_name: str,
        lineage_map: Dict[ResourceRef, ResourceLineage],
        snapshot_hash: str,
    ) -> str:
        rpath = super()._save_rfile_to_snapshot(resource_name, lineage_map, snapshot_hash)
        self._add_to_git(rpath)
        return rpath

    def _copy_rfile_to_snapshot(self, resource_name: str, snapshot_hash: str) -> Tuple[str, str]:
        (spath, dpath) = super()._copy_rfile_to_snapshot(resource_name, snapshot_hash)
        self._add_to_git(dpath)
        return (spath, dpath)

    def _write_placeholder_to_snapshot(
        self, snapshot_hash: str, filename: str, content: str
    ) -> str:
        spath = super()._write_placeholder_to_snapshot(snapshot_hash, filename, content)
        self._add_to_git(spath)
        return spath

    def delete_snapshot_lineage(self, instance: str, snapshot_hash: str) -> None:
        """Delete any lineage data associated with the specified snapshot.
        """
        lineage_relative_path = join(SNAPSHOT_LINEAGE_DIR_PATH, snapshot_hash)
        git_remove_subtree(
            self.workspace.workspace_dir, lineage_relative_path, verbose=self.workspace.verbose
        )


class Workspace(ws.Workspace, ws.SyncedWorkspaceMixin, ws.SnapshotWorkspaceMixin):
    def __init__(self, workspace_dir: str, batch: bool = False, verbose: bool = False):
        self.workspace_dir = workspace_dir  # type: str
        cf_data = self._load_json_file(CONFIG_FILE_PATH)
        super().__init__(cf_data["name"], cf_data["dws-version"], batch, verbose)
        self.global_params = cf_data["global_params"]
        self.local_params = self._load_json_file(LOCAL_PARAMS_PATH)
        hostname = self.local_params[
            HOSTNAME
        ]  # TODO: make this user settable when creating the workspace
        assert isinstance(hostname, str)
        self.instance = hostname
        self.resource_params = self._load_json_file(RESOURCES_FILE_PATH)  # type: List[JSONDict]
        self.resource_params_by_name = {}  # type: Dict[str, JSONDict]
        for r in self.resource_params:
            self.resource_params_by_name[r["name"]] = r
        self.resource_local_params_by_name = self._load_json_file(
            RESOURCE_LOCAL_PARAMS_PATH
        )  # type: Dict[str,JSONDict]
        self.lineage_store = GitFileLineageStore(self)
        self.scratch_dir = get_scratch_directory(
            self.workspace_dir, self.global_params, self.local_params
        )

    def get_instance(self) -> str:
        return self.instance

    def supports_lineage(self) -> bool:
        return True

    def get_lineage_store(self) -> LineageStore:
        return self.lineage_store

    def get_scratch_directory(self) -> str:
        if self.scratch_dir is not None:
            return self.scratch_dir
        else:
            raise ConfigurationError(
                "Neither the %s nor %s parameters are set, so cannot find scratch directory. Please set one using 'dws config'."
                % (SCRATCH_DIRECTORY, LOCAL_SCRATCH_DIRECTORY)
            )

    def _load_json_file(self, relative_path):
        f_path = join(self.workspace_dir, relative_path)
        if not exists(f_path):
            raise ConfigurationError("Did not find workspace metadata file %s" % f_path)
        with open(f_path, "r") as f:
            return json.load(f)

    def _save_json_to_file(self, obj, relative_path):
        f_path = join(self.workspace_dir, relative_path)
        with open(f_path, "w") as f:
            json.dump(obj, f, indent=2)

    def _get_global_params(self) -> JSONDict:
        """Get a dict of configuration parameters for this workspace,
        which apply across all instances.
        """
        return self.global_params

    def _get_local_params(self) -> JSONDict:
        """Get a dict of configuration parameters for this particular
        install of the workspace (e.g. local filesystem paths, hostname).
        """
        return self.local_params

    def _set_global_param(self, name: str, value: Any) -> None:
        """Setting does not necessarily take effect until save() is called"""
        data = self._get_global_params()
        data[name] = value
        self._save_json_to_file(
            {"name": self.name, "dws-version": self.dws_version, "global_params": data},
            CONFIG_FILE_PATH,
        )

    def _set_local_param(self, name: str, value: Any) -> None:
        data = self._get_local_params()
        data[name] = value
        self._save_json_to_file(data, LOCAL_PARAMS_PATH)

    def get_resource_names(self) -> Iterable[str]:
        return self.resource_params_by_name.keys()

    def _get_resource_params(self, resource_name) -> JSONDict:
        """Get the parameters for this resource from the workspace's
        metadata store - used when instantitating resources. Show
        throw an exception if resource does not exist.
        """
        if resource_name not in self.resource_params_by_name:
            raise ConfigurationError(
                "A resource by the name '%s' does not exist in this workspace" % resource_name
            )
        return self.resource_params_by_name[resource_name]

    def _get_resource_local_params(self, resource_name: str) -> Optional[JSONDict]:
        """If a resource has local parameters defined for it, return them.
        Otherwise, return None.
        """
        if resource_name in self.resource_local_params_by_name:
            return self.resource_local_params_by_name[resource_name]
        else:
            return None

    def _add_params_for_resource(self, resource_name: str, params: JSONDict) -> None:
        """
        Add the necessary state for a new resource to the workspace.
        """
        assert params["name"] == resource_name
        self.resource_params.append(params)
        self.resource_params_by_name[resource_name] = params
        self._save_json_to_file(self.resource_params, RESOURCES_FILE_PATH)

    def _add_local_params_for_resource(self, resource_name: str, local_params: JSONDict) -> None:
        """
        Add local params either for a new or cloned resource.
        """
        self.resource_local_params_by_name[resource_name] = local_params
        self._save_json_to_file(self.resource_local_params_by_name, RESOURCE_LOCAL_PARAMS_PATH)

    def _set_global_param_for_resource(self, resource_name: str, name: str, value: Any) -> None:
        """It is up to the caller to verify that the resource exists and has
        this parameter defined. Value should be json-serializable (via the to_json() method
        of the param type). Setting does not necessarily take effect until save() is called"""
        assert resource_name in self.resource_params_by_name, (
            "Missing resource params entry for resource %s" % resource_name
        )
        self.resource_params_by_name[resource_name][name] = value
        for pdict in self.resource_params:
            if pdict["name"] == resource_name:
                pdict[name] = value
                self._save_json_to_file(self.resource_params, RESOURCES_FILE_PATH)
                return
        assert 0, "Did not find resource params entry"

    def _set_local_param_for_resource(self, resource_name: str, name: str, value: Any) -> None:
        """It is up to the caller to verify that the resource exists and has
        this parameter defined. Value should be json-serializable (via the to_json() method
        of the param type). Setting does not necessarily take effect until save() is called"""
        assert resource_name in self.resource_local_params_by_name, (
            "Missing resource local params entry for resource %s" % resource_name
        )
        self.resource_local_params_by_name[resource_name][name] = value
        self._save_json_to_file(self.resource_local_params_by_name, RESOURCE_LOCAL_PARAMS_PATH)

    def get_workspace_local_path_if_any(self) -> Optional[str]:
        return self.workspace_dir

    def _add_local_dir_to_gitignore_if_needed(self, resource):
        """Figure out whether resource has a local path under the workspace's
        git repo, which needs to be added to .gitignore. If so, do it.
        """
        if resource.resource_type == "git-subdirectory":
            return  # this is always a part of the dataworkspace's repo
        elif not isinstance(resource, ws.LocalStateResourceMixin):
            return  # no local state, so not an iddue
        local_path = resource.get_local_path_if_any()
        if local_path is None:
            return
        assert isabs(local_path), "Resource local path should be absolute"
        if commonpath([local_path, self.workspace_dir]) != self.workspace_dir:
            return None
        local_relpath = local_path[len(self.workspace_dir) + 1 :]
        if not local_relpath.endswith("/"):
            local_relpath = local_relpath + "/"  # matches only directories
        # Add a / as the start to indicate that the path starts at the root of the repo.
        # Otherwise, we'll hit cases where the path could match other directories (e.g. issue #11)
        local_relpath = "/" + local_relpath if not local_relpath.startswith("/") else local_relpath
        ensure_entry_in_gitignore(
            self.workspace_dir,
            ".gitignore",
            local_relpath,
            match_independent_of_slashes=True,
            verbose=self.verbose,
        )

    def add_resource(
        self, name: str, resource_type: str, role: str, *args, **kwargs
    ) -> ws.Resource:
        r = super().add_resource(name, resource_type, role, *args, **kwargs)
        self._add_local_dir_to_gitignore_if_needed(r)
        return r

    def clone_resource(self, name: str) -> ws.LocalStateResourceMixin:
        """Only called if the resource has local state....
        """
        r = super().clone_resource(name)
        self._add_local_dir_to_gitignore_if_needed(r)
        return r

    def _get_local_scratch_space_for_resource(
        self, resource_name: str, create_if_not_present: bool = False
    ) -> str:
        scratch_path = join(self.workspace_dir, ".dataworkspace/scratch/%s" % resource_name)
        if not isdir(scratch_path):
            if create_if_not_present is False:
                raise InternalError(
                    "Scratch path '%s' for resource %s is missing" % (scratch_path, resource_name)
                )
            os.makedirs(scratch_path)
            ensure_entry_in_gitignore(
                self.workspace_dir,
                ".dataworkspace/.gitignore",
                "/scratch/%s/" % resource_name,
                commit=True,
            )
        return scratch_path

    def save(self, message: str) -> None:
        """Save the current state of the workspace"""
        commit_changes_in_repo(self.workspace_dir, message, verbose=self.verbose)

    def pull_workspace(self) -> ws.SyncedWorkspaceMixin:
        # first, check for problems
        if is_git_dirty(self.workspace_dir):
            raise ConfigurationError(
                "Data workspace metadata repo at %s has uncommitted changes. Please commit before pulling."
                % self.workspace_dir
            )
        validate_git_fat_in_path_if_needed(self.workspace_dir)

        # do the pooling
        call_subprocess(
            [GIT_EXE_PATH, "pull", "origin", "master"], cwd=self.workspace_dir, verbose=self.verbose
        )
        run_git_fat_pull_if_needed(self.workspace_dir, self.verbose)

        # reload and return new workspace
        return Workspace(self.workspace_dir, batch=self.batch, verbose=self.verbose)

    def _push_precheck(self, resource_list: List[ws.LocalStateResourceMixin]) -> None:
        if is_git_dirty(self.workspace_dir):
            raise ConfigurationError(
                "Data workspace metadata repo at %s has uncommitted changes. Please commit before pushing."
                % self.workspace_dir
            )
        if is_pull_needed_from_remote(self.workspace_dir, "master", self.verbose):
            raise ConfigurationError(
                "Data workspace at %s requires a pull from remote origin" % self.workspace_dir
            )
        validate_git_fat_in_path_if_needed(self.workspace_dir)
        super()._push_precheck(resource_list)

    def push(self, resource_list: List[ws.LocalStateResourceMixin]) -> None:
        super().push(resource_list)
        call_subprocess(
            [GIT_EXE_PATH, "push", "origin", "master"], cwd=self.workspace_dir, verbose=self.verbose
        )
        run_git_fat_push_if_needed(self.workspace_dir, verbose=self.verbose)

    def publish(self, *args) -> None:
        if len(args) != 1:
            raise InternalError("publish takes one argument: remote_repository, got %s" % args)
        set_remote_origin(self.workspace_dir, args[0], verbose=self.verbose)

    def get_next_snapshot_number(self) -> int:
        """Snapshot numbers are assigned based on how many snapshots have
        already been taken. Counting starts at 1. Note that snaphsot
        numbers are not necessarily unique, as people could simultaneously
        take snapshots in different copies of the workspace. Thus, we
        usually combine the snapshot with the hostname.
        """
        md_dirpath = join(self.workspace_dir, SNAPSHOT_METADATA_DIR_PATH)
        if not isdir(md_dirpath):
            return 1  # first snapshot
        # we recursively walk the tree to be future-proof in case we
        # find that we need to start putting metadata into subdirectories.
        def process_dir(dirpath):
            cnt = 0
            for f in os.listdir(dirpath):
                p = join(dirpath, f)
                if isdir(p):
                    cnt += process_dir(p)
                elif f.endswith("_md.json"):
                    cnt += 1
            return cnt

        return 1 + process_dir(md_dirpath)

    def get_snapshot_metadata(self, hash_val: str) -> SnapshotMetadata:
        hash_val = hash_val.lower()
        md_filename = join(
            join(self.workspace_dir, SNAPSHOT_METADATA_DIR_PATH), "%s_md.json" % hash_val
        )
        if not exists(md_filename):
            raise ConfigurationError("No metadata entry for snapshot %s" % hash_val)
        with open(md_filename, "r") as f:
            data = json.load(f)
        md = ws.SnapshotMetadata.from_json(data)
        assert md.hashval == hash_val
        return md

    def get_snapshot_by_tag(self, tag: str) -> SnapshotMetadata:
        """Given a tag, return the asssociated snapshot metadata.
        This lookup could be slower ,if a reverse index is not kept."""
        md_dir = join(self.workspace_dir, SNAPSHOT_METADATA_DIR_PATH)
        regexp = re.compile(re.escape(tag))
        for fname in os.listdir(md_dir):
            if not fname.endswith("_md.json"):
                continue
            fpath = join(md_dir, fname)
            with open(fpath, "r") as f:
                raw_data = f.read()
            if regexp.search(raw_data) is not None:
                md = SnapshotMetadata.from_json(json.loads(raw_data))
                if md.has_tag(tag):
                    return md
        raise ConfigurationError("Snapshot for tag %s not found" % tag)

    def get_snapshot_by_partial_hash(self, partial_hash: str) -> SnapshotMetadata:
        """Given a partial hash for the snapshot, find the snapshot whose hash
        starts with this prefix and return the metadata
        asssociated with the snapshot.
        """
        partial_hash = partial_hash.lower()
        md_dir = join(self.workspace_dir, SNAPSHOT_METADATA_DIR_PATH)
        for fname in os.listdir(md_dir):
            if not fname.endswith("_md.json"):
                continue
            hashval = fname[0:-8].lower()
            if not hashval.startswith(partial_hash):
                continue
            return self.get_snapshot_metadata(hashval)
        raise ConfigurationError("Snapshot match for partial hash %s not found" % partial_hash)

    def _get_snapshot_manifest_as_bytes(self, hash_val: str) -> bytes:
        snapshot_dir = join(self.workspace_dir, SNAPSHOT_DIR_PATH)
        snapshot_file = join(snapshot_dir, "snapshot-%s.json" % hash_val.lower())
        if not exists(snapshot_file):
            raise ConfigurationError("No snapshot found for hash value %s" % hash_val)
        with open(snapshot_file, "rb") as f:
            return f.read()

    def list_snapshots(
        self, reverse: bool = True, max_count: Optional[int] = None
    ) -> Iterable[SnapshotMetadata]:
        """Returns an iterable of snapshot metadata, sorted by timestamp ascending
        (or descending if reverse is True). If max_count is specified, return at
        most that many snaphsots.
        """
        md_dir = join(self.workspace_dir, SNAPSHOT_METADATA_DIR_PATH)
        snapshots = []
        for fname in os.listdir(md_dir):
            if not fname.endswith("_md.json"):
                continue
            with open(join(md_dir, fname), "r") as f:
                snapshots.append(SnapshotMetadata.from_json(json.load(f)))
        snapshots.sort(key=lambda md: md.timestamp, reverse=reverse)
        return snapshots if max_count is None else snapshots[0:max_count]

    def _delete_snapshot_metadata_and_manifest(self, hash_val: str) -> None:
        """Given a snapshot hash, delete the associated metadata.
        """
        rel_snapshot_file = join(SNAPSHOT_DIR_PATH, "snapshot-%s.json" % hash_val.lower())
        git_remove_file(self.workspace_dir, rel_snapshot_file, verbose=self.verbose)
        rel_metadata_file = join(SNAPSHOT_METADATA_DIR_PATH, "%s_md.json" % hash_val.lower())
        git_remove_file(self.workspace_dir, rel_metadata_file, verbose=self.verbose)

    def _snapshot_precheck(self, current_resources: Iterable[ws.Resource]) -> None:
        """Run any prechecks before taking a snapshot. This should throw
        a ConfigurationError if the snapshot would fail for some reason.
        """
        # call prechecks on the individual resources
        super()._snapshot_precheck(current_resources)
        validate_git_fat_in_path_if_needed(self.workspace_dir)

    def _restore_precheck(
        self,
        restore_hashes: Dict[str, Optional[str]],
        restore_resources: List[ws.SnapshotResourceMixin],
    ) -> None:
        """Run any prechecks before restoring. This should throw
        a ConfigurationError if the restore would fail for some reason.
        """
        # call prechecks on the individual resources
        super()._restore_precheck(restore_hashes, restore_resources)
        validate_git_fat_in_path_if_needed(self.workspace_dir)

    def restore(
        self,
        snapshot_hash: str,
        restore_hashes: Dict[str, Optional[str]],
        restore_resources: List[ws.SnapshotResourceMixin],
    ) -> None:
        """We override restore to perform a git-fat pull at the end,
        if needed.
        """
        super().restore(snapshot_hash, restore_hashes, restore_resources)
        run_git_fat_pull_if_needed(self.workspace_dir, self.verbose)

    def remove_tag_from_snapshot(self, hash_val: str, tag: str) -> None:
        """Remove the specified tag from the specified snapshot. Throw an
        InternalError if either the snapshot or the tag do not exist.
        """
        md_filename = join(
            join(self.workspace_dir, SNAPSHOT_METADATA_DIR_PATH), "%s_md.json" % hash_val.lower()
        )
        if not exists(md_filename):
            raise InternalError("No metadata entry for snapshot %s" % hash_val)
        with open(md_filename, "r") as f:
            data = json.load(f)
        md = ws.SnapshotMetadata.from_json(data)
        assert md.hashval == hash_val
        if tag not in md.tags:
            raise InternalError("Tag %s not found in snapshot %s" % (tag, hash_val))
        md.tags = [tag for tag in md.tags if tag != tag]
        with open(md_filename, "w") as f:
            json.dump(md.to_json(), f, indent=2)

    def save_snapshot_metadata_and_manifest(
        self, metadata: SnapshotMetadata, manifest: bytes
    ) -> None:
        snapshot_dir_path = join(self.workspace_dir, SNAPSHOT_DIR_PATH)
        if not exists(snapshot_dir_path):
            os.makedirs(snapshot_dir_path)
        snapshot_manifest_path = join(snapshot_dir_path, "snapshot-%s.json" % metadata.hashval)
        with open(snapshot_manifest_path, "wb") as f:
            f.write(manifest)
        snapshot_md_dir = join(self.workspace_dir, SNAPSHOT_METADATA_DIR_PATH)
        if not exists(snapshot_md_dir):
            os.makedirs(snapshot_md_dir)
        snapshot_metadata_path = join(snapshot_md_dir, "%s_md.json" % metadata.hashval)
        with open(snapshot_metadata_path, "w") as mdf:
            json.dump(metadata.to_json(), mdf, indent=2)

    def as_snapshot_ws(self) -> ws.SnapshotWorkspaceMixin:
        """If this workspace supports snapshots, cast
        it to a SnapshotWorkspaceMixin. Otherwise,
        raise an NotSupportedError exception.
        """
        return cast(ws.SnapshotWorkspaceMixin, self)

    def as_lineage_ws(self) -> ws.SnapshotWorkspaceMixin:
        """If this workspace supports snapshots and lineage, cast
        it to a SnapshotWorkspaceMixin. Otherwise,
        raise an NotSupportedError exception.
        """
        return cast(ws.SnapshotWorkspaceMixin, self)


class WorkspaceFactory(ws.WorkspaceFactory):
    @staticmethod
    def load_workspace(batch: bool, verbose: bool, parsed_uri: ParseResult) -> ws.Workspace:  # type: ignore
        path = parsed_uri.path
        if not isabs(path):
            path = abspath(expanduser(path))
        if not isdir(path):
            raise ConfigurationError("Workspace directory %s does not exist" % path)
        metadata_path = join(path, ".dataworkspace")
        if not isdir(metadata_path):
            raise ConfigurationError(
                "Workspace directory %s does not correspond to an initialized git-backend workspace"
                % path
            )
        return Workspace(path, batch, verbose)

    @staticmethod
    def init_workspace(  # type: ignore
        workspace_name: str,
        dws_version: str,
        global_params: JSONDict,
        local_params: JSONDict,
        batch: bool,
        verbose: bool,
        scratch_dir: str,
        workspace_dir: str,
        git_fat_remote: Optional[str] = None,
        git_fat_user: Optional[str] = None,
        git_fat_port: Optional[int] = None,
        git_fat_attributes: Optional[str] = None,
        git_lfs_attributes: Optional[str] = None,
    ) -> ws.Workspace:
        if not exists(workspace_dir):
            raise ConfigurationError(
                "Directory for new workspace '%s' does not exist" % workspace_dir
            )
        md_dir = join(workspace_dir, BASE_DIR)
        if isdir(md_dir):
            raise ConfigurationError(
                "Found %s directory under %s" % (BASE_DIR, workspace_dir)
                + " Has this workspace already been initialized?"
            )
        verify_git_config_initialized(workspace_dir, batch=batch, verbose=verbose)
        os.mkdir(md_dir)
        snapshot_dir = join(workspace_dir, SNAPSHOT_DIR_PATH)
        os.mkdir(snapshot_dir)
        snapshot_md_dir = join(workspace_dir, SNAPSHOT_METADATA_DIR_PATH)
        os.mkdir(snapshot_md_dir)

        (abs_scratch_dir, scratch_dir_gitignore) = init_scratch_directory(
            scratch_dir, workspace_dir, global_params, local_params
        )
        with open(join(workspace_dir, CONFIG_FILE_PATH), "w") as f:
            json.dump(
                {
                    "name": workspace_name,
                    "dws-version": dws_version,
                    "global_params": global_params,
                },
                f,
                indent=2,
            )
        with open(join(workspace_dir, RESOURCES_FILE_PATH), "w") as f:
            json.dump([], f, indent=2)
        with open(join(workspace_dir, LOCAL_PARAMS_PATH), "w") as f:
            json.dump(local_params, f, indent=2)
        with open(join(workspace_dir, RESOURCE_LOCAL_PARAMS_PATH), "w") as f:
            json.dump({}, f, indent=2)
        os.mkdir(join(workspace_dir, CURRENT_LINEAGE_DIR_PATH))

        with open(join(workspace_dir, GIT_IGNORE_FILE_PATH), "a") as f:
            f.write("%s\n" % basename(LOCAL_PARAMS_PATH))
            f.write("%s\n" % basename(RESOURCE_LOCAL_PARAMS_PATH))
            f.write("current_lineage/\n")
        if exists(join(workspace_dir, ".git")):
            click.echo("%s is already a git repository, will just add to it" % workspace_dir)
        else:
            git_init(workspace_dir, verbose=verbose)
        git_add(
            workspace_dir,
            [CONFIG_FILE_PATH, RESOURCES_FILE_PATH, GIT_IGNORE_FILE_PATH],
            verbose=verbose,
        )
        if scratch_dir_gitignore is not None:
            # add the scratch directory's gitignore entry to the top level of
            # the repo, not the .gitignore within .dataworkspace
            ensure_entry_in_gitignore(
                workspace_dir, ".gitignore", scratch_dir_gitignore, commit=False, verbose=verbose
            )
        commit_changes_in_repo(workspace_dir, "dws init", verbose=verbose)

        if not isdir(abs_scratch_dir):
            if verbose:
                print("Creating scratch directory %s" % abs_scratch_dir)
            os.makedirs(abs_scratch_dir)

        if git_fat_remote is not None:
            setup_git_fat_for_repo(
                workspace_dir,
                git_fat_remote,
                git_fat_user,
                git_fat_port,
                git_fat_attributes,
                verbose,
            )
        if git_lfs_attributes or is_a_git_lfs_repo(workspace_dir):
            if git_fat_remote:
                raise ConfigurationError("Cannot have both git-lfs and git-fat for the same repo.")
            init_git_lfs(workspace_dir, git_lfs_attributes, verbose=verbose)
        return Workspace(workspace_dir, batch, verbose)

    @staticmethod
    def clone_workspace(local_params: JSONDict, batch: bool, verbose: bool, *args) -> ws.Workspace:
        # args is REPOSITORY_URL [DIRECTORY]
        if len(args) == 0:
            raise ConfigurationError(
                "Need to specify a Git repository URL when cloning a workspace"
            )
        else:
            repository = args[0]  # type: str
        directory = args[1] if len(args) == 2 else None  # type: Optional[str]
        if len(args) > 2:
            raise ConfigurationError(
                "Clone of git backend expecting at most two arguments, received: %s" % repr(args)
            )

        # initial checks on the directory
        if directory:
            directory = abspath(expanduser(directory))
            parent_dir = dirname(directory)
            if isdir(directory):
                raise ConfigurationError("Clone target directory '%s' already exists" % directory)
            initial_path = directory
        else:
            parent_dir = abspath(expanduser(curdir))
            initial_path = join(
                parent_dir, uuid.uuid4().hex
            )  # get a unique name within this directory
        if not isdir(parent_dir):
            raise ConfigurationError("Parent directory '%s' does not exist" % parent_dir)
        if not os.access(parent_dir, os.W_OK):
            raise ConfigurationError("Unable to write into directory '%s'" % parent_dir)

        verify_git_config_initialized(parent_dir, batch=batch, verbose=verbose)

        # ping the remote repo
        cmd = [GIT_EXE_PATH, "ls-remote", "--quiet", repository]
        try:
            call_subprocess(cmd, parent_dir, verbose)
        except Exception as e:
            raise ConfigurationError("Unable to access remote repository '%s'" % repository) from e

        # we have to clone the repo first to find out its name!
        try:
            cmd = [GIT_EXE_PATH, "clone", repository, initial_path]
            call_subprocess(cmd, parent_dir, verbose)
            config_file = join(initial_path, CONFIG_FILE_PATH)
            if not exists(config_file):
                raise ConfigurationError("Did not find configuration file in remote repo")
            with open(config_file, "r") as f:
                config_json = json.load(f)
            if "name" not in config_json:
                raise InternalError("Missing 'name' property in configuration file")
            workspace_name = config_json["name"]
            if directory is None:
                new_name = join(parent_dir, workspace_name)
                if isdir(new_name):
                    raise ConfigurationError("Clone target directory %s already exists" % new_name)
                safe_rename(initial_path, new_name)
                directory = new_name

            cf_path = join(directory, CONFIG_FILE_PATH)
            if not exists(cf_path):
                raise ConfigurationError("Did not find workspace config file %s" % cf_path)
            with open(cf_path, "r") as f:
                cf_data = json.load(f)
            global_params = cf_data["global_params"]
            # get the scratch directory (also adds local param if needed)
            abs_scratch_dir = clone_scratch_directory(directory, global_params, local_params, batch)
            if not isdir(abs_scratch_dir):
                if verbose:
                    print("Creating scratch directory %s" % abs_scratch_dir)
                os.makedirs(abs_scratch_dir)
            with open(join(directory, LOCAL_PARAMS_PATH), "w") as f:
                json.dump(local_params, f, indent=2)  # create an initial local params file
            with open(join(directory, RESOURCE_LOCAL_PARAMS_PATH), "w") as f:
                json.dump(
                    {}, f, indent=2
                )  # create resource local params, to be populated via resource clones
            snapshot_md_dir = join(directory, SNAPSHOT_METADATA_DIR_PATH)
            if not exists(snapshot_md_dir):
                # It is possible that we are cloning a repo with no snapshots
                os.mkdir(snapshot_md_dir)
            snapshot_dir = join(directory, SNAPSHOT_DIR_PATH)
            if not exists(snapshot_dir):
                # It is possible that we are cloning a repo with no snapshots
                os.mkdir(snapshot_dir)
            current_lineage_dir = join(directory, CURRENT_LINEAGE_DIR_PATH)
            if not exists(current_lineage_dir):
                os.mkdir(current_lineage_dir)
            if is_a_git_fat_repo(directory):
                validate_git_fat_in_path()
                import dataworkspaces.third_party.git_fat as git_fat

                python2_exe = git_fat.find_python2_exe()
                git_fat.run_git_fat(python2_exe, ["init"], cwd=directory, verbose=verbose)
                # pull the objects referenced by the current head
                git_fat.run_git_fat(python2_exe, ["pull"], cwd=directory, verbose=verbose)
            ensure_git_lfs_configured_if_needed(directory, verbose=verbose)

        except:
            if isdir(initial_path):
                shutil.rmtree(initial_path)
            if (directory is not None) and isdir(directory):
                shutil.rmtree(directory)
            raise

        return WorkspaceFactory.load_workspace(batch, verbose, urlparse(directory))


FACTORY = WorkspaceFactory()
