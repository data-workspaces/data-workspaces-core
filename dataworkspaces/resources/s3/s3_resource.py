# Copyright 2018,2019 by MPI-SWS and Data-ken Research. Licensed under Apache 2.0. See LICENSE.txt.
"""
Resource for files living in a local directory 
"""
import os
from os.path import join, exists, isdir
from typing import List, Pattern, Tuple, Optional, Set, Union, cast
import json

from s3fs import S3FileSystem

import click

from dataworkspaces.errors import ConfigurationError, NotSupportedError
from dataworkspaces.utils.subprocess_utils import call_subprocess
from dataworkspaces.utils.file_utils import does_subpath_exist, LocalPathType
from dataworkspaces.utils.git_utils import GIT_EXE_PATH, is_git_staging_dirty
from dataworkspaces.workspace import (
    Workspace,
    Resource,
    ResourceRoles,
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


S3_RESOURCE_TYPE = "s3"


def _relative_rsrc_dir_for_git_workspace(role, name):
    return ".dataworkspace/" + LOCAL_FILE + "/" + role + "/" + name


# For anything specific to results resources, we throw this error
RESULTS_ROLE_NOT_SUPPORTED=NotSupportedError(f"{ResourceRoles.RESULTS} not currently supported for S3 resources")


class S3Resource(
    Resource, LocalStateResourceMixin, FileResourceMixin, SnapshotResourceMixin
):
    """Resource class for S3 bucket."""

    def __init__(
        self,
        resource_type: str,
        name: str,
        role: str,
        workspace: Workspace,
        bucket_name: str,
        region: str,
    ):
        if role==ResourceRoles.RESULTS:
            raise RESULTS_ROLE_NOT_SUPPORTED
        super().__init__(resource_type, name, role, workspace)
        self.param_defs.define(
            "bucket_name",
            default_value=None,
            optional=False,
            is_global=True,
            help="Name of the bucket",
            ptype=StringType(),
        )
        self.bucket_name = self.param_defs.get(
            "bucket_name", bucket_name
        )  # type: str
        self.param_defs.define(
            "region",
            default_value=None,
            optional=False,
            is_global=False,
            help="S3 Region for bucket",
            ptype=StringType(),
        )
        self.region = self.param_defs.get(
            "region", region
        ) # type: str
        self.fs = S3FileSystem(region=region)
        # local scratch directory is for stuff we keep locally reflecting the
        # current state of our local workspace.
        self.local_scratch_dir = join(join(workspace.get_scratch_directory(), S3_RESOURCE_TYPE),
                                      name)
        # ensure the scratch directory exits
        if not exists(self.local_scratch_dir):
            os.makedirs(self.local_scratch_dir)
        self.current_snapshot_file = join(self.local_scratch_dir, 'current_snapshot.txt')
        if exists(self.current_snapshot_file):
            with open(self.current_snapshot_file, 'r') as f:
                self.current_snapshot = f.read().strip()
        else:
            self.current_snapshot = None
        # we cache snapshot files in a subdirectory of the scratch dir
        self.snapshot_cache_dir = join(self.local_scratch_dir, "snapshot_cache")
        # Make sure it exists.
        if not exists(self.snapshot_cache_dir):
            os.makedir(self.snapshot_cache_dir)

    def get_local_path_if_any(self):
        return None

    def results_move_current_files(
        self, rel_dest_root: str, exclude_files: Set[str], exclude_dirs_re: Pattern
    ) -> None:
        raise RESULTS_ROLE_NOT_SUPPORTED

    def results_copy_current_files(
        self, rel_dest_root: str, exclude_files: Set[str], exclude_dirs_re: Pattern
    ) -> None:
        raise RESULTS_ROLE_NOT_SUPPORTED

    def add_results_file(self, data: Union[JSONDict, JSONList], rel_dest_path: str) -> None:
        """save JSON results data to the specified path in the resource.
        """
        raise RESULTS_ROLE_NOT_SUPPORTED

    def _verify_no_snapshot(self):
        if self.current_snapshot is not None:
            raise NotSupportedError("Cannot update bucket when a snapshot is selected. "+
                                    f"Current snapshot is {self.current_snapshot}")

    def upload_file(self, local_path: str, rel_dest_path: str) -> None:
        """Copy a local file to the specified path in the
        resource. This may be a local copy or an upload, depending
        on the resource implmentation
        """
        self._verify_no_snapshot()
        if not exists(local_path):
            raise ConfigurationError("Source file %s does not exist." % local_path)
        self.fs.put(local_path, join(self.bucket, rel_dest_path))

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



class S3ResourceFactory(ResourceFactory):
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
