# Copyright 2018,2019 by MPI-SWS and Data-ken Research. Licensed under Apache 2.0. See LICENSE.txt.
"""
API for selected Data Workspaces management functions.
"""
from typing import Optional, NamedTuple, List, Iterable, cast

from dataworkspaces import __version__
from dataworkspaces.workspace import find_and_load_workspace,\
    LocalStateResourceMixin, SnapshotWorkspaceMixin, JSONDict
from dataworkspaces.commands.snapshot import snapshot_command
from dataworkspaces.commands.restore import restore_command

__api_version__ = '0.2'


def get_version():
    return __version__

def get_api_version():
    return __api_version__



class ResourceInfo(NamedTuple):
    name : str
    role : str
    resource_type : str
    local_path : Optional[str]


def get_resource_info(workspace_uri_or_path:Optional[str]=None, verbose:bool=False):
    """Returns a list of ResourceInfo instances, describing the resources
    defined for this workspace.
    """
    workspace = find_and_load_workspace(True, verbose, workspace_uri_or_path)

    return [
        ResourceInfo(r.name, r.role, r.resource_type,
                     cast(LocalStateResourceMixin, r).get_local_path_if_any()
                     if isinstance(r, LocalStateResourceMixin) else None)
        for r in workspace.get_resources()
    ]


class SnapshotInfo(NamedTuple):
    snapshot_number: int
    hashval : int
    tags : List[str]
    timestamp: str
    message: str
    metrics: Optional[JSONDict]


def take_snapshot(workspace_uri_or_path:Optional[str]=None, tag:Optional[str]=None, message:str='',
                  verbose:bool=False) -> str:
    """Take a snapshot of the workspace, using the tag and message,
    if provided. Returns the snapshot hash (which can be used to restore to
    this point).
    """
    workspace = find_and_load_workspace(True, verbose, workspace_uri_or_path)
    return snapshot_command(workspace, tag=tag, message=message)


def get_snapshot_history(workspace_uri_or_path:Optional[str]=None,
                         reverse:bool=False, max_count:Optional[int]=None,
                         verbose:bool=False) -> Iterable[SnapshotInfo]:
    """Get the history of snapshots, starting with the oldest first (unless :reverse: is True).
    Returns a list of SnapshotInfo instances, containing the snapshot number,
    hash, tag, timestamp, and message. If :max_count: is specified, returns at most that many snapshots.

    """
    workspace = find_and_load_workspace(True, verbose, workspace_uri_or_path)
    assert isinstance(workspace, SnapshotWorkspaceMixin)
    if not reverse:
        return [
            SnapshotInfo(snapshot_idx+1, md.hashval, md.tags, md.timestamp,
                         md.message, md.metrics) for (snapshot_idx, md) in
            enumerate(workspace.list_snapshots(reverse=False, max_count=max_count))
        ]
    else:
        last_snapshot_no = workspace.get_next_snapshot_number() - 1
        return [
            SnapshotInfo(last_snapshot_no-i, md.hashval, md.tags, md.timestamp,
                         md.message, md.metrics) for (i, md) in
            enumerate(workspace.list_snapshots(reverse=True, max_count=max_count))
        ]



def restore(tag_or_hash:str, workspace_uri_or_path:Optional[str]=None,
            only:Optional[List[str]]=None, leave:Optional[List[str]]=None,
            verbose:bool=False) -> int:
    """Restore to a previous snapshot, identified by either its hash
    or its tag (if one was specified). :param only: is an optional list of
    resources to store. If specified, all other resources will be left as-is.
    :param leave: is an optional list of resource to leave as-is. Both
    :param only: and :param leave: should not be specified together.

    Returns the number of resources changed.
    """
    workspace = find_and_load_workspace(True, verbose, workspace_uri_or_path)
    return restore_command(workspace, tag_or_hash=tag_or_hash,
                           only=only, leave=leave)



