# Copyright 2018,2019 by MPI-SWS and Data-ken Research. Licensed under Apache 2.0. See LICENSE.txt.
"""
API for selected Data Workspaces management functions.
"""
from typing import Optional, NamedTuple, List, Iterable, cast, Tuple
from os.path import join
import sys

from dataworkspaces import __version__
from dataworkspaces.workspace import find_and_load_workspace,\
    LocalStateResourceMixin, SnapshotWorkspaceMixin, JSONDict,\
    FileResourceMixin,ResourceRoles,Workspace
from dataworkspaces.commands.snapshot import snapshot_command
from dataworkspaces.commands.restore import restore_command
from dataworkspaces.commands.lineage import lineage_graph_command
from dataworkspaces.errors import ConfigurationError
import dataworkspaces.utils.lineage_utils as lu

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



def make_lineage_table(workspace_uri_or_path:Optional[str]=None,
                              tag_or_hash:Optional[str]=None, verbose:bool=False) \
    -> Iterable[Tuple[str, str, str, str, Optional[List[str]]]]:
    """Make a table of the lineage for each resource.
    The columns are: ref, lineage type, details, inputs
    """
    workspace = find_and_load_workspace(True, verbose, workspace_uri_or_path)
    if not isinstance(workspace, SnapshotWorkspaceMixin):
        raise ConfigurationError("Workspace %s does not support lineage" % workspace.name)
    if not workspace.supports_lineage():
        raise ConfigurationError("Workspace %s does not support lineage" % workspace.name)
    snapshot_hash = None # type: Optional[str]
    if tag_or_hash is not None:
        md = workspace.get_snapshot_by_tag_or_hash(tag_or_hash)
        snapshot_hash = md.hashval
    return lu.make_lineage_table(workspace.get_instance(), workspace.get_lineage_store(), snapshot_hash)


def make_lineage_graph(output_file:str,
                       workspace_uri_or_path:Optional[str]=None,
                       resource_name:Optional[str]=None,
                       tag_or_hash:Optional[str]=None,
                       width:int=1024, height:int=800,
                       verbose:bool=False) -> None:
    """Write a lineage graph as an html/javascript page to the specified file.
    """
    workspace = find_and_load_workspace(True, verbose, workspace_uri_or_path)
    lineage_graph_command(workspace, output_file, resource_name=resource_name,
                          snapshot=tag_or_hash, width=width, height=height)

def _find_results_file_if_present(workspace:Workspace, subpath:str,
                                  resource_name:Optional[str]=None)\
    -> Optional[Tuple[JSONDict, str]]:
    if resource_name is not None:
        check_resources = [resource_name]
    else:
        check_resources = [rn for rn in workspace.get_resource_names()
                           if workspace.get_resource_role(rn)==ResourceRoles.RESULTS]
    for rn in check_resources:
        resource = workspace.get_resource(rn)
        if not isinstance(resource, FileResourceMixin):
            continue
        if not resource.does_subpath_exist(subpath, must_be_file=True):
            continue
        return (resource.read_results_file(subpath), '%s:/%s' % (rn, subpath))
    return None # not found


def get_results(workspace_uri_or_path:Optional[str]=None,
                tag_or_hash:Optional[str]=None,
                resource_name:Optional[str]=None,
                verbose:bool=False) -> Optional[Tuple[JSONDict, str]]:
    """Get a results file a a parsed json dict. If no resource or snapshot
    is specified, searches all the results resources for a file. If a snapshot
    is specified, we look in the subdirectory where the resuls have been moved.
    If no snapshot is specified, and we don't find a file, we look in the most
    recent snapshot.

    Returns a tuple with the results and the logical path (resource:/subpath) to
    the results. If nothing is found, returns None.
    """
    workspace = find_and_load_workspace(True, verbose, workspace_uri_or_path)
    if tag_or_hash is not None:
        if not isinstance(workspace, SnapshotWorkspaceMixin):
            raise ConfigurationError("Workspace %s does not support snapshots" % workspace.name)
        md = workspace.get_snapshot_by_tag_or_hash(tag_or_hash)
        subpath = join(md.relative_destination_path, 'results.json')
        return _find_results_file_if_present(workspace, subpath, resource_name)
    else:
        result = _find_results_file_if_present(workspace, 'results.json', resource_name)
        if result is not None:
            return result
        # not found - ok, try the snapshot
        if not isinstance(workspace, SnapshotWorkspaceMixin):
            return None
        print("Did not find a results.json file in current workspace, checking most recent snapshot...",
              file=sys.stderr)
        rmd = workspace.get_most_recent_snapshot()
        if rmd is not None:
            subpath = join(rmd.relative_destination_path, 'results.json')
            return _find_results_file_if_present(workspace, subpath, resource_name)
        else:
            return None
