import os
import json

import click

import dataworkspaces.commands.actions as actions
from dataworkspaces import __version__

SNAPSHOT_HISTORY_FILE = '.dataworkspace/snapshots/snapshot_history.json'
RESOURCE_FILE = '.dataworkspace/resources.json'


class ReadSnapshotHistory(actions.Action):
    def __init__(self, ns, verbose, snapshot_history_file, limit=0):
        super().__init__(ns, verbose)
        self.snapshot_history_file = snapshot_history_file
        self.limit = limit

    def run(self):
        with open(self.snapshot_history_file, 'r') as f:
            history = json.load(f)
            num_snapshots = len(history)
        for v in reversed(history[-self.limit:]):
            click.echo('Version %s (created %s): %s' % (v['tag'], v['timestamp'], v['message']))
        limit = num_snapshots if self.limit == 0 else self.limit
        click.echo('Showing %d of %d snapshots' % (limit, num_snapshots))

    def __str__(self):
        return "Append snapshot metadata to .dataworkspace/snapshots/snapshot_history.json"


def show_snapshot_history(ns, workspace_dir, limit, batch, verbose):
    snapshot_file = os.path.join(workspace_dir, SNAPSHOT_HISTORY_FILE)
    if not os.path.exists(snapshot_file):
        if verbose:
            click.echo('No snapshot file')
        return

    plan = [ ]
    output_history = ReadSnapshotHistory(ns, verbose, snapshot_file, limit=limit)
    plan.append(output_history)
    actions.run_plan(plan, "Output recent snapshots", "done", batch=batch, verbose=verbose) 

def show_current_status(ns, workspace_dir, batch, verbose):
    rsrc_file = os.path.join
def status_command(workspace_dir, history, limit, batch, verbose):
    ns = actions.Namespace()
    show_current_status(ns, workspace_dir, batch, verbose)
    if history:
        show_snapshot_history(ns, workspace_dir, limit, batch, verbose)

