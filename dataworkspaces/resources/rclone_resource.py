# Copyright 2018 by MPI-SWS and Data-ken Research. Licensed under Apache 2.0. See LICENSE.txt.
"""
Resource for files copied in by rclone 
"""
import os
import os.path
import stat
from typing import Tuple, List, Set, Pattern, Optional, Union, Any, cast
import json
import shutil
import click

from dataworkspaces.errors import ConfigurationError
from dataworkspaces.workspace import (
    Workspace,
    Resource,
    LocalStateResourceMixin,
    FileResourceMixin,
    SnapshotResourceMixin,
    JSONDict,
    JSONList,
    ResourceRoles,
    ResourceFactory,
)
from dataworkspaces.utils.snapshot_utils import move_current_files_local_fs
from dataworkspaces.utils.file_utils import does_subpath_exist, LocalPathType
from dataworkspaces.third_party.rclone import RClone
from dataworkspaces.utils.param_utils import (
    ParamType,
    ParamParseError,
    ParamValidationError,
    StringType,
    BoolType,
)

RCLONE_RESOURCE_TYPE = "rclone"

"""
dws add rclone [options] remote local

See 
"""


class RemoteOriginType(ParamType):
    """Custom param type for maintaining the remote origin in the form
    remote:abspath.
    """

    def parse(self, str_value: str) -> str:
        if ":" not in str_value:
            raise ParamParseError(
                "Remote origin '%s' is missing a ':', should be of the form remote:path" % str_value
            )
        (remote_name, rpath) = str_value.split(":")
        if os.path.isabs(rpath):
            return str_value
        else:
            return remote_name + ":" + os.path.abspath(rpath)

    def validate(self, value: Any) -> None:
        if not isinstance(value, str):
            raise ParamValidationError("Remote origin '%s' is not a string" % repr(value))
        if ":" not in value:
            raise ParamValidationError(
                "Remote origin '%s' is missing a ':', should be of the form remote:path" % value
            )

    def __str__(self):
        return "remote_origin"


class RcloneResource(Resource, LocalStateResourceMixin, FileResourceMixin, SnapshotResourceMixin):
    def __init__(
        self,
        name: str,
        role: str,
        workspace: Workspace,
        remote_origin: str,
        global_local_path: str,
        my_local_path: Optional[str],
        config: Optional[str] = None,
        export: bool = False,
        compute_hash: bool = False,
        ignore: List[str] = [],
    ):
        super().__init__(RCLONE_RESOURCE_TYPE, name, role, workspace)
        self.param_defs.define(
            "remote_origin",
            default_value=None,
            optional=False,
            is_global=True,
            help="Rclone remote origin in the form remote:path",
            ptype=RemoteOriginType(),
        )
        self.remote_origin = self.param_defs.get("remote_origin", remote_origin)  # type: str
        (self.remote_name, remote_path) = self.remote_origin.split(":")
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
        self.local_path = os.path.abspath(
            self.my_local_path if self.my_local_path is not None else self.global_local_path
        )
        self.param_defs.define(
            "export",
            default_value=False,
            optional=True,
            help="True if metadata for export should be added each snapshot",
            is_global=True,
            ptype=BoolType(),
        )
        self.export = self.param_defs.get("export", export)  # type: bool
        self.param_defs.define(
            "compute_hash",
            default_value=False,
            optional=True,
            is_global=True,
            help="If True, then compute the full hash of all files rather than using sizes.",
            ptype=BoolType(),
        )
        self.compute_hash = self.param_defs.get("compute_hash", compute_hash)  # type: bool
        self.param_defs.define(
            "config",
            default_value=None,
            optional=True,
            is_global=True,
            help="Optional path to rclone config file (otherwise uses the default)",
            ptype=StringType(),
        )
        self.config = self.param_defs.get("config", config)  # type: Optional[str]

        self.ignore = ignore  # TODO: should this be a parameter?

        if config:
            self.rclone = RClone(cfgfile=self.config)
        else:
            self.rclone = RClone()

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

    def add_results_file(self, data: Union[JSONDict, JSONList], rel_dest_path: str) -> None:
        """save JSON results data to the specified path in the resource.
        """
        assert self.role == ResourceRoles.RESULTS
        abs_dest_path = os.path.join(self.local_path, rel_dest_path)
        parent_dir = os.path.dirname(abs_dest_path)
        if not os.path.isdir(parent_dir):
            os.makedirs(parent_dir)
        with open(abs_dest_path, "w") as f:
            json.dump(data, f, indent=2)

    def does_subpath_exist(
        self, subpath: str, must_be_file: bool = False, must_be_directory: bool = False
    ) -> bool:
        return does_subpath_exist(self.local_path, subpath, must_be_file, must_be_directory)

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

    def upload_file(self, src_local_path: str, rel_dest_path: str) -> None:
        """Copy a local file to the specified path in the
        resource. This may be a local copy or an upload, depending
        on the resource implmentation
        """
        abs_dest_path = os.path.join(self.local_path, rel_dest_path)
        parent_dir = os.path.dirname(abs_dest_path)
        if not os.path.exists(src_local_path):
            raise ConfigurationError("Source file %s does not exist." % src_local_path)
        if not os.path.isdir(parent_dir):
            os.makedirs(parent_dir)
        shutil.copyfile(src_local_path, rel_dest_path)

    def get_local_params(self) -> JSONDict:
        return {}  # TODO: local filepath can override global path

    def pull_precheck(self) -> None:
        """Nothing to do, since we donot support sync.
        TODO: Support pulling from remote
        """
        pass

    def pull(self) -> None:
        """Nothing to do, since we donot support sync.
        TODO: Support pulling from remote
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
        if self.workspace.verbose:
            print("In snapshot: %s %s" % (self.remote_origin, self.local_path))
        if self.compute_hash:
            (ret, out) = self.rclone.check(self.remote_origin, self.local_path, flags=["--one-way"])
        else:
            (ret, out) = self.rclone.check(
                self.remote_origin, self.local_path, flags=["--one-way", "--size-only"]
            )
        print("Snapshot returns ", ret, out)
        return (ret, None)  # None for the restore hash since we cannot restore

    def restore_precheck(self, hashval):
        pass
        # rc = hashtree.check_hashes(hashval, self.rsrcdir, self.local_path, ignore=self.ignore)
        # if not rc:
        #     raise ConfigurationError("Local file structure not compatible with saved hash")

    def restore(self, hashval):
        pass  # rclone-d files: do nothing to restore

    def delete_snapshot(
        self, workspace_snapshot_hash: str, resource_restore_hash: str, relative_path: str
    ) -> None:
        snapshot_dir_path = os.path.join(self.local_path, relative_path)
        if os.path.isdir(snapshot_dir_path):
            if self.workspace.verbose:
                print(
                    "Deleting snapshot directory %s from resource %s" % (relative_path, self.name)
                )
            shutil.rmtree(snapshot_dir_path)

    def validate_subpath_exists(self, subpath: str) -> None:
        super().validate_subpath_exists(subpath)

    def __str__(self):
        return "Rclone-d repo %s, locally copied in %s in role '%s'" % (
            self.remote_origin,
            self.local_path,
            self.role,
        )


class RcloneFactory(ResourceFactory):
    def _add_prechecks(self, local_path, remote_path, config) -> RClone:
        if os.path.exists(local_path) and not (os.access(local_path, os.W_OK)):
            raise ConfigurationError(local_path + " does not have write permission")
        if config:
            rclone = RClone(cfgfile=config)
        else:
            rclone = RClone()
        known_remotes = rclone.listremotes()
        (remote_name, _) = remote_path.split(":")
        if remote_name not in known_remotes:
            raise ConfigurationError("Remote '" + remote_name + "' not found by rclone")
        return rclone

    def _copy_from_remote(self, local_path, remote_origin, rclone):
        if not (os.path.exists(local_path)):
            os.makedirs(local_path)
        ret = rclone.copy(remote_origin, local_path)
        if ret["code"] != 0:
            raise ConfigurationError("rclone copy raised error %d: %s" % (ret["code"], ret["err"]))
        # mark the files as readonly
        print("Marking files as readonly")
        for (dirpath, dirnames, filenames) in os.walk(local_path):
            for f_name in filenames:
                abspath = os.path.abspath(os.path.join(dirpath, f_name))
                mode = os.stat(abspath)[stat.ST_MODE]
                os.chmod(abspath, mode & ~stat.S_IWUSR & ~stat.S_IWGRP & ~stat.S_IWOTH)

    def from_command_line(
        self, role, name, workspace, remote_path, local_path, config, export, compute_hash
    ):
        rclone = self._add_prechecks(local_path, remote_path, config)
        self._copy_from_remote(local_path, remote_path, rclone)
        return RcloneResource(
            name,
            role,
            workspace,
            remote_path,
            global_local_path=local_path,
            my_local_path=None,
            config=config,
            export=export,
            compute_hash=compute_hash,
        )

    def from_json(
        self, params: JSONDict, local_params: JSONDict, workspace: Workspace
    ) -> RcloneResource:
        """Instantiate a resource object from the parsed resources.json file"""
        assert params["resource_type"] == RCLONE_RESOURCE_TYPE
        return RcloneResource(
            params["name"],
            params["role"],
            workspace,
            params["remote_origin"],
            # for backward compatibility, we also check for "local_path"
            global_local_path=params["global_local_path"]
            if "global_local_path" in params
            else params["local_path"],
            my_local_path=local_params["my_local_path"]
            if "my_local_path" in local_params
            else (local_params["local_path"] if "local_path" in local_params else None),
            config=params["config"],
            export=params.get("export", False),
            compute_hash=params["compute_hash"],
        )

    def has_local_state(self) -> bool:
        return True

    def clone(self, params: JSONDict, workspace: Workspace) -> LocalStateResourceMixin:
        """Instantiate a resource that was created remotely. In this case, we will
        copy from the remote origin.
        """
        remote_origin = params["remote_origin"]
        config = params["config"]
        name = params["name"]
        # check local_path, too for backward compatibility
        global_local_path = (
            params["global_local_path"] if "global_local_path" in params else params["local_path"]
        )  # type: str
        if os.path.exists(global_local_path):
            local_path = global_local_path
            my_local_path = None  # type: Optional[str]
        else:
            if not workspace.batch:
                # TODO: consider whether we can just create the directory the user specifies rather than
                # requiring it to pre-exist (since we will download anyway).
                local_path = cast(
                    str,
                    click.prompt(
                        "Rclone resource '%s' was located at '%s' on the original system. Where is it located on this system?"
                        % (name, global_local_path),
                        type=LocalPathType(exists=True),
                    ),
                )
                my_local_path = local_path
            else:
                raise ConfigurationError(
                    "Local files resource %s is missing from %s." % (name, global_local_path)
                )

        rclone = self._add_prechecks(local_path, remote_origin, config)
        self._copy_from_remote(local_path, remote_origin, rclone)
        return RcloneResource(
            name,
            params["role"],
            workspace,
            remote_origin,
            global_local_path=global_local_path,
            my_local_path=my_local_path,
            config=config,
            export=params.get("export", False),
            compute_hash=params["compute_hash"],
        )

    def suggest_name(self, workspace, role, remote_path, local_path, config, export, compute_hash):
        return os.path.basename(local_path)
