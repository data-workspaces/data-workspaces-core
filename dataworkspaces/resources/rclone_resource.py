# Copyright 2018 by MPI-SWS and Data-ken Research. Licensed under Apache 2.0. See LICENSE.txt.
"""
Resource for files copied in by rclone 
"""
import os
import os.path

# import stat
from typing import List, Optional, Any, cast
import json
import click

from dataworkspaces.errors import ConfigurationError
from dataworkspaces.workspace import (
    Workspace,
    LocalStateResourceMixin,
    SnapshotWorkspaceMixin,
    JSONDict,
    ResourceFactory,
)
from dataworkspaces.utils.file_utils import LocalPathType
from dataworkspaces.third_party.rclone import RClone
from dataworkspaces.utils.param_utils import (
    ParamType,
    ParamValidationError,
    StringType,
    EnumType,
    BoolType,
)

from dataworkspaces.resources.local_file_resource import LocalFileResource, setup_path_for_hashes

RCLONE_RESOURCE_TYPE = "rclone"

"""
dws add rclone [options] remote local

See 
"""


class RemoteOriginType(ParamType):
    """Custom param type for maintaining the remote origin in the form
    remote_name:path. Path does not need to be absolute
    """

    def validate(self, value: Any) -> None:
        if not isinstance(value, str):
            raise ParamValidationError("Remote origin '%s' is not a string" % repr(value))
        if ":" not in value:
            raise ParamValidationError(
                "Remote origin '%s' is missing a ':', should be of the form remote:path" % value
            )

    def __str__(self):
        return "remote_origin"


class RcloneResource(LocalFileResource):
    """Local files synchronized to a remote via rclone."""

    def __init__(
        self,
        name: str,
        role: str,
        workspace: Workspace,
        remote_origin: str,
        global_local_path: str,
        my_local_path: Optional[str],
        config: Optional[str] = None,
        compute_hash: Optional[bool] = None,
        export: Optional[bool] = None,
        imported: Optional[bool] = None,
        master: Optional[str] = None,
        sync_mode: Optional[str] = None,
        size_only: Optional[bool] = None,
        ignore: List[str] = [],
    ):
        super().__init__(
            RCLONE_RESOURCE_TYPE,
            name,
            role,
            workspace,
            global_local_path,
            my_local_path,
            compute_hash,
            export,
            imported,
            ignore,
        )
        self.param_defs.define(
            "remote_origin",
            default_value=None,
            optional=False,
            is_global=True,
            help="Rclone remote origin in the form remote:path",
            ptype=RemoteOriginType(),
        )
        self.remote_origin = self.param_defs.get("remote_origin", remote_origin)  # type: str
        (self.remote_name, _) = self.remote_origin.split(":")
        self.param_defs.define(
            "config",
            default_value=None,
            optional=True,
            is_global=True,
            help="Optional path to rclone config file (otherwise uses the default)",
            ptype=StringType(),
        )
        self.config = self.param_defs.get("config", config)  # type: Optional[str]
        self.param_defs.define(
            "master",
            default_value="none",
            optional=False,
            help="Determines which system is the master. If 'remote', then pulls will be done, but not pushes. "
            + "If 'local', then pushes will be done, but not pulls. If 'none' (the default), no action will be taken "
            + "for pushes and pulls (you need to synchronize manually using rclone). When first adding the resource "
            + " or cloning to a new machine, if the local directory does not exist, and 'remote' or 'none' were specified, "
            + " the contents of the remote will copied down to the local directory.",
            is_global=True,
            ptype=EnumType("none", "remote", "local"),
        )
        self.master = self.param_defs.get("master", master)  # type: str
        self.param_defs.define(
            "sync_mode",
            default_value="copy",
            optional=False,
            help="When copying between local and master, which rclone command to use. If you specify 'copy', files are "
            + "added or overwritten without deleting any files present at the target. If you specify 'sync', files at "
            + "the target are removed if they are not present at the source. The default is 'copy'. If master is 'none', "
            + "this setting has no effect.",
            is_global=True,
            ptype=EnumType("sync", "copy"),
        )
        self.sync_mode = self.param_defs.get("sync_mode", sync_mode)  # type: str
        self.param_defs.define(
            "size_only",
            default_value=False,
            optional=False,
            help="If specified, use only the file size (rather than also modification time and checksum) to "
            + "determine if a file has been changed. If your resource has a lot of files and access to the remote "
            + "is over a WAN, you probably want to set this. Otherwise, syncs/copies can be VERY slow.",
            is_global=True,
            ptype=BoolType(),
            allow_missing=True,
        )
        self.size_only = self.param_defs.get("size_only", size_only)  # type: bool

        if config:
            self.rclone = RClone(cfgfile=self.config)
        else:
            self.rclone = RClone()

    def pull_precheck(self) -> None:
        pass  # a dry run can be very expensive, so skipping for now

    def pull(self) -> None:
        if self.size_only:
            flags = ["--size-only"]
        else:
            flags = []
        flags.append("--verbose")
        if self.master == "remote":
            if self.sync_mode == "sync":
                ret = self.rclone.sync(self.remote_origin, self.local_path, flags=flags)
                if ret["code"] != 0:
                    raise ConfigurationError(
                        "rclone sync raised error %d: %s" % (ret["code"], ret["error"])
                    )
            elif self.sync_mode == "copy":
                ret = self.rclone.copy(self.remote_origin, self.local_path, flags=flags)
                if ret["code"] != 0:
                    raise ConfigurationError(
                        "rclone copy raised error %d: %s" % (ret["code"], ret["error"])
                    )
        else:
            click.echo("Skipping pull of resource %s, master is %s" % (self.name, self.master))

    def push_precheck(self) -> None:
        pass  # a dry run can be very expensive, so skipping for now

    def push(self) -> None:
        if self.size_only:
            flags = ["--size-only"]
        else:
            flags = []
        flags.append("--verbose")
        if self.master == "local":
            if self.sync_mode == "copy":
                ret = self.rclone.copy(self.local_path, self.remote_origin, flags=flags)
                if ret["code"] != 0:
                    raise ConfigurationError(
                        "rclone copy raised error %d: %s" % (ret["code"], ret["error"])
                    )
            else:
                ret = self.rclone.sync(self.local_path, self.remote_origin, flags=flags)
                if ret["code"] != 0:
                    raise ConfigurationError(
                        "rclone sync raised error %d: %s" % (ret["code"], ret["error"])
                    )
        else:
            click.echo("Skiping push of resource %s, master is %s" % (self.name, self.master))

    def __str__(self):
        return "Rclone-d repo %s, locally copied in %s in role '%s'" % (
            self.remote_origin,
            self.local_path,
            self.role,
        )


class RcloneFactory(ResourceFactory):
    def _add_prechecks(self, local_path, remote_origin, config) -> RClone:
        if os.path.exists(local_path) and not (os.access(local_path, os.W_OK)):
            raise ConfigurationError(local_path + " does not have write permission")
        if config:
            rclone = RClone(cfgfile=config)
        else:
            rclone = RClone()
        known_remotes = rclone.listremotes()
        (remote_name, _) = remote_origin.split(":")
        if remote_name not in known_remotes:
            raise ConfigurationError("Remote '" + remote_name + "' not found by rclone")
        return rclone

    def _copy_from_remote(
        self,
        name,
        local_path,
        remote_origin,
        rclone,
        master="none",
        sync_mode="copy",
        size_only=False,
        verbose=False,
    ):
        if master == "remote":
            click.echo("%s: performing initial %s from remote" % (name, sync_mode))
            if not os.path.exists(local_path):
                os.makedirs(local_path)
        elif master == "none" and (not os.path.exists(local_path)):
            click.echo("%s: performing initial copy from remote" % name)
            sync_mode = "copy"
            os.makedirs(local_path)
        elif master == "local" and not (os.path.exists(local_path)):
            click.echo("Creating empty directory at %s for resource %s" % (local_path, name))
            os.makedirs(local_path)
            return
        else:
            click.echo(
                "Skipping initial copy of resource %s to %s: path already exists and remote is local"
                % (name, local_path)
            )
            return

        if size_only:
            flags = ["--size-only"]
        else:
            flags = []
        flags.append("--verbose")

        if sync_mode == "copy":
            ret = rclone.copy(remote_origin, local_path, flags=flags)
        else:
            ret = rclone.sync(remote_origin, local_path, flags=flags)
        if ret["code"] != 0:
            raise ConfigurationError(
                "rclone %s raised error %d: %s" % (sync_mode, ret["code"], ret["error"])
            )
        # For now, leave the local copy alone
        # if master=='none':
        #     print("Marking files as readonly")
        #     for (dirpath, dirnames, filenames) in os.walk(local_path):
        #         for f_name in filenames:
        #             abspath = os.path.abspath(os.path.join(dirpath, f_name))
        #             mode = os.stat(abspath)[stat.ST_MODE]
        #             os.chmod(abspath, mode & ~stat.S_IWUSR & ~stat.S_IWGRP & ~stat.S_IWOTH)

    def from_command_line(
        self,
        role,
        name,
        workspace,
        remote_origin,
        local_path,
        config,
        compute_hash,
        export,
        imported,
        master,
        sync_mode,
        size_only,
    ):
        rclone = self._add_prechecks(local_path, remote_origin, config)
        self._copy_from_remote(
            name, local_path, remote_origin, rclone, master, sync_mode, size_only, workspace.verbose
        )
        setup_path_for_hashes(role, name, workspace, local_path)
        if imported:
            lineage_path = os.path.join(local_path, "lineage.json")
            if not os.path.exists(lineage_path):
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

        return RcloneResource(
            name,
            role,
            workspace,
            remote_origin,
            global_local_path=local_path,
            my_local_path=None,
            config=config,
            compute_hash=compute_hash,
            export=export,
            imported=imported,
            master=master,
            sync_mode=sync_mode,
            size_only=size_only,
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
            compute_hash=params["compute_hash"],
            export=params.get("export", False),
            imported=params.get("imported", False),
            master=params.get("master", None),
            sync_mode=params.get("sync_mode", None),
            size_only=params.get("size_only", None),
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
        master = params.get("master", None)
        sync_mode = params.get("sync_mode", None)
        size_only = params.get("size_only", None)

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
        self._copy_from_remote(
            name, local_path, remote_origin, rclone, master, sync_mode, size_only, workspace.verbose
        )
        return RcloneResource(
            name,
            params["role"],
            workspace,
            remote_origin,
            global_local_path=global_local_path,
            my_local_path=my_local_path,
            config=config,
            compute_hash=params["compute_hash"],
            export=params.get("export", False),
            imported=params.get("imported", False),
            master=master,
            sync_mode=sync_mode,
            size_only=size_only,
        )

    def suggest_name(
        self,
        workspace,
        role,
        remote_origin,
        local_path,
        config,
        compute_hash,
        export,
        imported,
        master,
        sync_mode,
        size_only,
    ):
        return os.path.basename(local_path)
