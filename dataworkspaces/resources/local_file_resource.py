# Copyright 2018,2019 by MPI-SWS and Data-ken Research. Licensed under Apache 2.0. See LICENSE.txt.
"""
Resource for files living in a local directory 
"""
from errno import EEXIST
import os
from os.path import join, exists, isdir
from typing import List, Pattern, Tuple, Optional, Set, Union, cast
import json
import shutil

import click

from dataworkspaces.errors import ConfigurationError, PathError
from dataworkspaces.utils.subprocess_utils import call_subprocess
from dataworkspaces.utils.file_utils import does_subpath_exist, LocalPathType
from dataworkspaces.utils.git_utils import GIT_EXE_PATH, is_git_staging_dirty
from dataworkspaces.workspace import (
    Workspace,
    Resource,
    LocalStateResourceMixin,
    FileResourceMixin,
    SnapshotResourceMixin,
    SnapshotWorkspaceMixin,
    JSONDict,
    JSONList,
    ResourceFactory,
)
import dataworkspaces.resources.hashtree as hashtree
from dataworkspaces.utils.snapshot_utils import (
    move_current_files_local_fs,
    copy_current_files_local_fs,
)
import dataworkspaces.backends.git as git_backend
from dataworkspaces.utils.param_utils import StringType, BoolType


LOCAL_FILE = "file"


def _relative_rsrc_dir_for_git_workspace(role, name):
    return ".dataworkspace/" + LOCAL_FILE + "/" + role + "/" + name


class LocalFileResource(
    Resource, LocalStateResourceMixin, FileResourceMixin, SnapshotResourceMixin
):
    """Resource class for local files. Can also serve as the base class for other resource
    types (e.g. rclone)."""

    def __init__(
        self,
        resource_type: str,
        name: str,
        role: str,
        workspace: Workspace,
        global_local_path: str,
        my_local_path: Optional[str],
        compute_hash: Optional[bool] = None,
        export: Optional[bool] = None,
        imported: Optional[bool] = None,
        ignore: Optional[List[str]] = None,
    ):
        super().__init__(resource_type, name, role, workspace)
        self.param_defs.define(
            "global_local_path",
            default_value=None,
            optional=False,
            is_global=True,
            help="Location of files on local filesystem, as defined when the resource is created. "
            + "May be overridden locally via my_local_path.",
            ptype=StringType(),
        )
        self.global_local_path = self.param_defs.get(
            "global_local_path", global_local_path
        )  # type: str
        self.param_defs.define(
            "my_local_path",
            default_value=None,
            optional=True,
            is_global=False,
            help="Override of global_local_path, just for this instance of the workspace.",
            ptype=StringType(),
        )
        self.my_local_path = self.param_defs.get(
            "my_local_path", my_local_path
        )  # type: Optional[str]
        # the actual local path we'll use
        self.local_path = (
            self.my_local_path if self.my_local_path is not None else self.global_local_path
        )
        self.param_defs.define(
            "compute_hash",
            default_value=False,
            optional=False,
            is_global=True,
            help="If True, then compute the full hash of all files rather than using sizes.",
            ptype=BoolType(),
        )
        self.compute_hash = self.param_defs.get("compute_hash", compute_hash)  # type: bool
        self.param_defs.define(
            "export",
            default_value=False,
            optional=False,
            help="True if metadata for export should be added each snapshot",
            is_global=True,
            ptype=BoolType(),
            allow_missing=True,
        )
        self.export = self.param_defs.get("export", export)  # type: bool
        self.param_defs.define(
            "imported",
            default_value=False,
            optional=False,
            help="If True, then this resource has lineage imported from another workspace",
            is_global=True,
            ptype=BoolType(),
            allow_missing=True,
        )
        self.imported = self.param_defs.get("imported", imported)  # type: bool
        self.ignore = ignore if (ignore is not None) else []  # TODO: should this be a parameter?
        if isinstance(workspace, git_backend.Workspace):
            # if the workspace is a git repo, then we can store our
            # hash files there.
            self.rsrcdir = os.path.join(
                workspace.workspace_dir, _relative_rsrc_dir_for_git_workspace(self.role, self.name)
            )
        else:
            # If the workspace is not a git repo, we store the hash
            # files under the local path. Note that this is not going
            # to be replicated to other workspace instances, unless you
            # are using a shared file system (e.g. NFS).
            # TODO: add an API to the workspace for storing this arbitrary
            # data. It will also need changes to the hashtree file.
            self.rsrcdir = os.path.abspath(os.path.join(self.local_path, ".hashes"))
            self.ignore.append(".hashes")

    def get_local_path_if_any(self):
        return self.local_path

    def results_move_current_files(
        self, rel_dest_root: str, exclude_files: Set[str], exclude_dirs_re: Pattern
    ) -> None:
        move_current_files_local_fs(
            self.name,
            self.local_path,
            rel_dest_root,
            exclude_files,
            exclude_dirs_re,
            verbose=self.workspace.verbose,
        )

    def results_copy_current_files(
        self, rel_dest_root: str, exclude_files: Set[str], exclude_dirs_re: Pattern
    ) -> None:
        copy_current_files_local_fs(
            self.name,
            self.local_path,
            rel_dest_root,
            exclude_files,
            exclude_dirs_re,
            verbose=self.workspace.verbose,
        )

    def add_results_file(self, data: Union[JSONDict, JSONList], rel_dest_path: str) -> None:
        """save JSON results data to the specified path in the resource.
        """
        # assert self.role == ResourceRoles.RESULTS
        abs_dest_path = os.path.join(self.local_path, rel_dest_path)
        parent_dir = os.path.dirname(abs_dest_path)
        if not os.path.isdir(parent_dir):
            os.makedirs(parent_dir)
        with open(abs_dest_path, "w") as f:
            json.dump(data, f, indent=2)

    def upload_file(self, local_path: str, rel_dest_path: str) -> None:
        """Copy a local file to the specified path in the
        resource. This may be a local copy or an upload, depending
        on the resource implmentation
        """
        abs_dest_path = os.path.join(self.local_path, rel_dest_path)
        parent_dir = os.path.dirname(abs_dest_path)
        if not exists(local_path):
            raise ConfigurationError("Source file %s does not exist." % local_path)
        if not os.path.isdir(parent_dir):
            os.makedirs(parent_dir)
        shutil.copyfile(local_path, rel_dest_path)

    def does_subpath_exist(
        self, subpath: str, must_be_file: bool = False, must_be_directory: bool = False
    ) -> bool:
        return does_subpath_exist(self.local_path, subpath, must_be_file, must_be_directory)

    def delete_file(self, rel_path: str) -> None:
        os.remove(os.path.join(self.local_path, rel_path))

    def read_results_file(self, subpath: str) -> JSONDict:
        """Read and parse json results data from the specified path
        in the resource. If the path does not exist or is not a file
        throw a ConfigurationError.
        """
        path = os.path.join(self.local_path, subpath)
        if not os.path.isfile(path):
            raise ConfigurationError(
                "subpath %s does not exist or is not a file in resource %s" % (subpath, self.name)
            )
        with open(path, "r") as f:
            try:
                return json.load(f)
            except Exception as e:
                raise ConfigurationError(
                    "Parse error when reading %s in resource %s" % (subpath, self.name)
                ) from e

    def ls(self, rel_path:str) -> List[str]:
        if rel_path!='':
            path = os.path.join(self.local_path, rel_path)
        else:
            path = self.local_path
        if isdir(path):
            files = os.listdir(path)
            if rel_path!='':
                return [join(rel_path, file) for file in files]
            else:
                return files
        elif exists(path):
            return [rel_path]
        else:
            raise PathError(f"Path {rel_path} does not exist in resource {self.name}")

    def open(self, rel_path:str, mode:str):
        if rel_path=='':
            raise PathError("Need to specify a file to open, empty string is not valid here")
        path = os.path.join(self.local_path, rel_path)
        return open(path, mode)

    def get_local_params(self) -> JSONDict:
        return {"local_path": self.my_local_path}

    def pull_precheck(self) -> None:
        """Nothing to do, since we donot support sync.
        """
        pass

    def pull(self) -> None:
        """Nothing to do, since we donot support sync.
        """
        pass

    def push_precheck(self) -> None:
        """Nothing to do, since we donot support sync.
        """
        pass

    def push(self) -> None:
        """Nothing to do, since we donot support sync.
        """
        pass

    def snapshot_precheck(self) -> None:
        pass

    def snapshot(self) -> Tuple[Optional[str], Optional[str]]:
        if self.compute_hash:
            h = hashtree.generate_sha_signature(
                self.rsrcdir, self.local_path, ignore=self.ignore, verbose=self.workspace.verbose
            )
        else:
            h = hashtree.generate_size_signature(
                self.rsrcdir, self.local_path, ignore=self.ignore, verbose=self.workspace.verbose
            )
        assert os.path.exists(os.path.join(self.rsrcdir, h))
        if isinstance(self.workspace, git_backend.Workspace):
            workspace_path = self.workspace.get_workspace_local_path_if_any()
            assert workspace_path is not None
            if is_git_staging_dirty(
                workspace_path, subdir=_relative_rsrc_dir_for_git_workspace(self.role, self.name)
            ):
                call_subprocess(
                    [
                        GIT_EXE_PATH,
                        "commit",
                        "-m",
                        "Add snapshot hash files for resource %s" % self.name,
                    ],
                    cwd=workspace_path,
                    verbose=self.workspace.verbose,
                )
        return (h, None)

    def restore_precheck(self, hashval):
        # TODO: look at handling of restore - we probably want to do a compare and error out if
        # different. This would mean passing in both the compare and restore hashes.
        if self.compute_hash:
            rc = hashtree.check_sha_signature(
                hashval,
                self.rsrcdir,
                self.local_path,
                ignore=self.ignore,
                verbose=self.workspace.verbose,
            )
        else:
            rc = hashtree.check_size_signature(
                hashval,
                self.rsrcdir,
                self.local_path,
                ignore=self.ignore,
                verbose=self.workspace.verbose,
            )
        if not rc:
            print("ERROR IN RESTORE")
            raise ConfigurationError("Local file structure not compatible with saved hash")

    def restore(self, hashval):
        pass  # local files: do nothing to restore

    def delete_snapshot(
        self, workspace_snapshot_hash: str, resource_restore_hash: str, relative_path: str
    ) -> None:
        snapshot_dir_path = join(self.local_path, relative_path)
        if isdir(snapshot_dir_path):
            if self.workspace.verbose:
                print(
                    "Deleting snapshot directory %s from resource %s" % (relative_path, self.name)
                )
            shutil.rmtree(snapshot_dir_path)

    def validate_subpath_exists(self, subpath: str) -> None:
        super().validate_subpath_exists(subpath)

    def __str__(self):
        return "Local directory %s in role '%s'" % (self.local_path, self.role)


def setup_path_for_hashes(role: str, name: str, workspace: Workspace, local_path: str):
    """When creating the resource, make sure we have a path for the hashes.
    Subclasses of LocalFilesResource will need to call this in their factory
    functions."""
    workspace_path = workspace.get_workspace_local_path_if_any()
    if isinstance(workspace, git_backend.Workspace):
        assert workspace_path is not None
        hash_path = join(workspace_path, _relative_rsrc_dir_for_git_workspace(role, name))
        try:
            os.makedirs(hash_path)
            with open(os.path.join(hash_path, "dummy.txt"), "w") as f:
                f.write("Placeholder to ensure directory is added to git\n")
            call_subprocess(
                [
                    GIT_EXE_PATH,
                    "add",
                    join(_relative_rsrc_dir_for_git_workspace(role, name), "dummy.txt"),
                ],
                cwd=workspace_path,
            )
            call_subprocess(
                [GIT_EXE_PATH, "commit", "-m", "Adding resource %s" % name], cwd=workspace_path
            )
        except OSError as exc:
            if exc.errno == EEXIST and os.path.isdir(hash_path):
                pass
            else:
                raise
    else:
        non_git_hashes = join(local_path, ".hashes")
        if not exists(non_git_hashes):
            os.mkdir(non_git_hashes)


class LocalFileFactory(ResourceFactory):
    def from_command_line(self, role, name, workspace, local_path, compute_hash, export, imported):
        """Instantiate a resource object from the add command's arguments"""
        if not os.path.isdir(local_path):
            raise ConfigurationError(local_path + " does not exist")
        if not os.access(local_path, os.R_OK):
            raise ConfigurationError(local_path + " does not have read permission")
        setup_path_for_hashes(role, name, workspace, local_path)
        if imported:
            lineage_path = join(local_path, "lineage.json")
            if not exists(lineage_path):
                raise ConfigurationError(
                    "--imported was specified, but missing exported lineage file %s" % lineage_path
                )
            if (
                not isinstance(workspace, SnapshotWorkspaceMixin)
                or not workspace.supports_lineage()
            ):
                raise ConfigurationError(
                    "--imported was specified, but this workspace does not support lineage"
                )
            with open(lineage_path, "r") as f:
                lineage_data = json.load(f)
            if lineage_data["resource_name"] != name:
                raise ConfigurationError(
                    "Resource name in imported lineage '%s' does not match '%s'"
                    % (lineage_data["resource_name"], name)
                )
            cast(SnapshotWorkspaceMixin, workspace).get_lineage_store().import_lineage_file(
                name, lineage_data["lineages"]
            )

        return LocalFileResource(
            LOCAL_FILE,
            name,
            role,
            workspace,
            local_path,
            my_local_path=None,
            compute_hash=compute_hash,
            export=export,
            imported=imported,
        )

    def from_json(
        self, params: JSONDict, local_params: JSONDict, workspace: Workspace
    ) -> LocalFileResource:
        """Instantiate a resource object from saved params and local params"""
        return LocalFileResource(
            LOCAL_FILE,
            params["name"],
            params["role"],
            workspace,
            # for backward compatibility, we also check for "local_path"
            global_local_path=params["global_local_path"]
            if "global_local_path" in params
            else params["local_path"],
            my_local_path=local_params["my_local_path"]
            if "my_local_path" in local_params
            else (local_params["local_path"] if "local_path" in local_params else None),
            compute_hash=params["compute_hash"],
            export=params.get("export", None),
            imported=params.get("imported", None),
        )

    def has_local_state(self) -> bool:
        return True

    def clone(self, params: JSONDict, workspace: Workspace) -> LocalStateResourceMixin:
        """Instantiate a resource that was created remotely. We need to verify that
        the local copy of the data exists -- we are not responsible for making certain
        it is in th correct place.
        """
        name = params["name"]
        # check local_path, too for backward compatibility
        global_local_path = (
            params["global_local_path"] if "global_local_path" in params else params["local_path"]
        )  # type: str
        local_params = {}  # type: JSONDict
        if exists(global_local_path):
            local_path = global_local_path
        else:
            if not workspace.batch:
                local_path = cast(
                    str,
                    click.prompt(
                        "Local files resource '%s' was located at '%s' on the original system. W\here is it located on this system?"
                        % (name, global_local_path),
                        type=LocalPathType(exists=True),
                    ),
                )
                local_params["my_local_path"] = local_path
            else:
                raise ConfigurationError(
                    "Local files resource %s is missing from %s." % (name, global_local_path)
                )
        if not isinstance(workspace, git_backend.Workspace):
            non_git_hashes = join(local_path, ".hashes")
            if not exists(non_git_hashes):
                os.mkdir(non_git_hashes)
        return self.from_json(params, local_params, workspace)

    def suggest_name(self, workspace, role, local_path, compute_hash, export, imported):
        return os.path.basename(local_path)
