
import os
from os.path import join, exists
import json
from tempfile import NamedTemporaryFile
import datetime

import click

from dataworkspaces.resources.resource import get_resource_from_json
import dataworkspaces.commands.actions as actions
from dataworkspaces.errors import InternalError

class SnapshotResource(actions.Action):
    def __init__(self, verbose, resource, map_of_hashes):
        super().__init__(verbose)
        self.resource = resource
        self.resource.snapshot_prechecks()
        self.map_of_hashes = map_of_hashes

    def run(self):
        self.map_of_hashes[self.resource.url] = self.resource.snapshot()

    def __str__(self):
        return "Run snapshot actions for %s" % str(self.resource)

class WriteSnapshotFile(actions.Action):
    def __init__(self, temp_filename, map_of_hashes, resource_json):
        self.temp_filename = temp_filename
        self.map_of_hashes = map_of_hashes
        self.resource_json = resource_json

    def run(self):
        data = []
        for resource in self.resource_json:
            resource['hash'] = self.map_of_hashes[resource['url']]
            data.append(resource)
        with open(self.temp_filename, 'w') as f:
            json.dump(data, f, indent=2)

    def __str__(self):
        return 'Create snaptshot file at %s' % self.temp_filename

class RenameSnapShotFile(actions.Action):
    def __init__(self, verbose, workspace_dir, snapshot_file, get_hash_fn):
        super().__init__(verbose)
        self.workspace_dir = workspace_dir
        self.snapshot_file = snapshot_file
        self.get_hash_fn = get_hash_fn
        self.target = None

    def run(self):
        hashval = self.get_hash_fn()
        self.target = join(self.workspace_dir,
                           '.dataworkspace/snapshots/snapshot-%s.json' % hashval)
        os.rename(self.snapshot_file, self.target)

    def __str__(self):
        if self.target:
            return "Rename %s to %s" % (self.snapshot_file, self.target)
        else:
            return "Rename %s to ./dataworkspace/snapshots/snapshot-<HASH>.json"%\
                self.snapshot_file

class AppendSnapshotHistory(actions.Action):
    def __init__(self, verbose, workspace_dir, tag, message, get_hash_fn):
        self.snapshot_history_file = join(workspace_dir, '.dataworkspace/snapshots/snapshot_history.json')
        if not exists(self.snapshot_history_file):
            raise InternalError("Missing snapshot history file at %s" % self.snapshot_history)
        self.get_hash_fn = get_hash_fn
        self.snapshot_data = {'tag':tag, 'message':message}

    def run(self):
        with open(self.snapshot_history_file, 'r') as f:
            data = json.load(f)
        self.snapshot_data['hash'] = self.get_hash_fn()
        self.snapshot_data['timestamp'] = datetime.datetime.now().isoformat()
        data.append(self.snapshot_data)
        with open(self.snapshot_history_file, 'w') as f:
            json.dump(data, f, indent=2)

    def __str__(self):
        return "Append snapshot metadasta to .dataworkspace/snapshots/snapshot_history.json"


def snapshot_command(workspace_dir, batch, verbose, tag=None, message=''):
    print("snapshot of %s, tag=%s, message=%s" % (workspace_dir, tag, message))
    resource_file = join(workspace_dir, '.dataworkspace/resources.json')
    if not exists(resource_file):
        raise InternalError("Missing resource file %s" % resource_file)
    with open(resource_file, 'r') as f:
        data = json.load(f)
    plan = []
    map_of_hashes = {}
    for rdata in data:
        plan.append(
            SnapshotResource(verbose,
                             get_resource_from_json(rdata, workspace_dir, batch,
                                                    verbose),
                             map_of_hashes))
    with NamedTemporaryFile(delete=False) as f:
        tfilename = f.name
    try:
        plan.append(WriteSnapshotFile(tfilename, map_of_hashes, data))
        hash_action = actions.GitHashObject(tfilename, verbose)
        plan.append(hash_action)
        rename_action = RenameSnapShotFile(verbose, workspace_dir, tfilename,
                                           lambda: hash_action.hash_value)
        plan.append(rename_action)
        history_action = AppendSnapshotHistory(verbose, workspace_dir, tag, message, lambda: hash_action.hash_value)
        plan.append(history_action)
        plan.append(actions.GitAddDeferred(workspace_dir,
                                           lambda:[rename_action.target,history_action.snapshot_history_file],
                                           verbose))
        plan.append(actions.GitCommit(workspace_dir,
                                      message=lambda:"Snapshot "+
                                                     (lambda h:h.hash_value)(hash_action),
                                      verbose=verbose))
        actions.run_plan(plan, "take snapshot of workspace",
                         "taken snapshot of workspace", batch, verbose)
    except:
        if exists(tfilename):
            os.remove(tfilename)
        raise


