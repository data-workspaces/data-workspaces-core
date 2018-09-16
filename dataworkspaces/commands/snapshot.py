
import os
from os.path import join, exists
import json
import datetime

import click

from dataworkspaces.resources.resource import CurrentResources
import dataworkspaces.commands.actions as actions
from dataworkspaces.errors import InternalError, ConfigurationError

class TakeResourceSnapshot(actions.Action):
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
    def __init__(self, verbose, workspace_dir, map_of_hashes, current_resources):
        self.verbose = verbose
        self.workspace_dir = workspace_dir
        self.map_of_hashes = map_of_hashes
        self.current_resources = current_resources
        self.snapshot_hash = None
        self.snapshot_filename = None
        self.new_snapshot = None

    def run(self):
        def write_fn(tempfile):
            self.current_resources.write_snapshot_manifest(tempfile, self.map_of_hashes)
        (self.snapshot_hash, self.snapshot_filename, self.new_snapshot) = \
            actions.write_and_hash_file(
                write_fn,
                join(self.workspace_dir,
                     ".dataworkspace/snapshots/snapshot-<HASHVAL>.json"),
                self.verbose)

    def __str__(self):
        return 'Create and hash snapshot file'

# TODO: If not a new snapshot, merge history entries!
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
        return "Append snapshot metadata to .dataworkspace/snapshots/snapshot_history.json"


def snapshot_command(workspace_dir, batch, verbose, tag=None, message=''):
    print("snapshot of %s, tag=%s, message=%s" % (workspace_dir, tag, message))
    if (tag is not None) and actions.is_a_git_hash(tag):
        raise ConfigurationError("Tag '%s' looks like a git hash. Please pick something else." % tag)
    current_resources = CurrentResources.read_current_resources(workspace_dir, batch, verbose)
    plan = []
    map_of_hashes = {}
    for r in current_resources.resources:
        plan.append(
            TakeResourceSnapshot(verbose, r, map_of_hashes))
    write_snapshot = WriteSnapshotFile(verbose, workspace_dir, map_of_hashes,
                                       current_resources)
    plan.append(write_snapshot)
    history_action = AppendSnapshotHistory(verbose, workspace_dir, tag, message, lambda: write_snapshot.snapshot_hash)
    plan.append(history_action)
    plan.append(actions.GitAddDeferred(workspace_dir,
                                       lambda:[write_snapshot.snapshot_filename,
                                               history_action.snapshot_history_file],
                                       verbose))
    plan.append(actions.GitCommit(workspace_dir,
                                  message=lambda:"Snapshot "+
                                                 (lambda h:h.snapshot_hash)(write_snapshot),
                                  verbose=verbose))
    actions.run_plan(plan, "take snapshot of workspace",
                     "taken snapshot of workspace", batch=batch, verbose=verbose)


