# Copyright 2018,2019 by MPI-SWS and Data-ken Research. Licensed under Apache 2.0. See LICENSE.txt.
"""
API for selected Data Workspaces management functions.
"""
from collections import namedtuple
import os
from os.path import isdir, join, dirname, abspath, expanduser, curdir
import json

from dataworkspaces import __version__
from .errors import ApiParamError
from .resources.resource import CurrentResources
from .commands.snapshot import snapshot_command
from .commands.restore import restore_command
from .commands.status import get_snapshot_metadata

__api_version__ = '0.1'


def get_version():
    return __version__

def get_api_version():
    return __api_version__


def _get_workspace(caller_workspace_arg=None):
    """For commands that execute in the context of a containing
    workspace, find the nearest containging workspace and return
    its absolute path. If the caller provides one, we validate it
    and return it. Otherwise, we search outward from the current directory.
    Throws an ApiParamError exception if the workspace was invalid
    or could not be found.
    """
    if caller_workspace_arg is not None:
        workspace_dir = abspath(expanduser(caller_workspace_arg))
        if not isdir(workspace_dir):
            raise ApiParamError("Workspace directory %s does not exist" %
                                workspace_dir)
        dws_dir = join(workspace_dir, '.dataworkspace')
        if not isdir(dws_dir) or not os.access(dws_dir, os.W_OK):
            raise ApiParamError("Provided directory for workspace %s has not been initialized as a data workspace" % workspace_dir)
        else:
            return workspace_dir
    else:
        curr_dir_abs = abspath(expanduser(curdir))
        curr_base = curr_dir_abs
        while curr_base != '/':
            if isdir(join(curr_base, '.dataworkspace')) and os.access(curr_base, os.W_OK):
                return curr_base
            else:
                curr_base = dirname(curr_base)
        raise ApiParamError("Cound not find an enclosing data workspace starting from %s"%
                            curr_dir_abs)


ResourceInfo=namedtuple('ResourceInfo',
                        ['name', 'role', 'type', 'local_path'])

def get_resource_info(workspace_dir=None):
    """Returns a list of ResourceInfo instances, describing the resources
    defined for this workspace.
    """
    workspace_dir = _get_workspace(workspace_dir)
    current_resources = CurrentResources.read_current_resources(workspace_dir,
                                                                batch=True,
                                                                verbose=False)
    return [
        ResourceInfo(r.name, r.role, r.scheme,
                     r.get_local_path_if_any())
        for r in current_resources.resources
    ]


SnapshotInfo=namedtuple('SnapshotInfo',
                        ['snapshot_number', 'hash', 'tags', 'timestamp',
                         'message'])


def take_snapshot(workspace_dir=None, tag=None, message=''):
    """Take a snapshot of the workspace, using the tag and message,
    if provided. Returns the snapshot hash (which can be used to restore to
    this point).
    """
    workspace_dir = _get_workspace(workspace_dir)
    return snapshot_command(workspace_dir, batch=True, verbose=False, tag=tag,
                            message=message)


def get_snapshot_history(workspace_dir=None):
    """Get the history of snapshots, starting with the oldest first.
    Returns a list of SnapshotInfo instances, containing the snapshot number,
    hash, tag, timestamp, and message.
    """
    workspace_dir = _get_workspace(workspace_dir)
    data = get_snapshot_metadata(workspace_dir, reverse=False)
    return [
        SnapshotInfo(snapshot_number+1, s['hash'], s['tags'], s['timestamp'],
                     s['message'])
        for (snapshot_number, s) in enumerate(data)
    ]


def restore(tag_or_hash, workspace_dir=None, only=None, leave=None):
    """Restore to a previous snapshot, identified by either its hash
    or its tag (if one was specified). :param only: is an optional list of
    resources to store. If specified, all other resources will be left as-is.
    :param leave: is an optional list of resource to leave as-is. Both
    :param only: and :param leave: should not be specified together.
    """
    workspace_dir = _get_workspace(workspace_dir)
    restore_command(workspace_dir, batch=True, verbose=False, tag_or_hash=tag_or_hash,
                    only=only, leave=leave, no_new_snapshot=True)



