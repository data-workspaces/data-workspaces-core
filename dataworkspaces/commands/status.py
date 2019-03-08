# Copyright 2018,2019 by MPI-SWS and Data-ken Research. Licensed under Apache 2.0. See LICENSE.txt.
import os
import json

import click

import dataworkspaces.commands.actions as actions

from dataworkspaces.resources.resource import RESOURCE_ROLE_CHOICES 

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
        click.echo("\nHistory of snapshots")
        click.echo("%s %s %s %s" %
                   ('Hash'.ljust(40), 'Tag'.ljust(10), 'Created'.ljust(19),
                    'Message'))
        for v in reversed(history[-self.limit:]):
            click.echo('%s %s %s %s' %
                       (v['hash'],
                        (v['tag'] if v['tag'] is not None else 'N/A').ljust(10),
                        v['timestamp'][0:-7],
                        v['message'] if v['message'] is not None and
                                        v['message']!='' else 'N/A'))
        limit = num_snapshots if self.limit == 0 else self.limit
        click.echo('Showing %d of %d snapshots' % (limit, num_snapshots))

    def __str__(self):
        return ("Read snapshot metadata from %s" % self.snapshot_history_file)

class ReadResources(actions.Action):
    def __init__(self, ns, verbose, resource_file):
        super().__init__(ns, verbose)
        self.resource_file = resource_file

    def pretty(self, rsrc, indent=2):
        if rsrc['resource_type'] == 'git':
            click.echo(' '*indent, nl=False)
            click.echo('git repo %s' % rsrc['name'])
            if self.verbose:
                click.echo(' '*(indent+2)+ ('Remote=%s' % rsrc['remote_origin_url']))
            return
        if rsrc['resource_type'] == 'file':
            click.echo(' '*indent, nl=False)
            click.echo('local files %s' % rsrc['name'])
            if self.verbose:
                click.echo(' '*(indent+2), nl=False)
                click.echo('LocalPath=%s' % rsrc['local_path'])
            return

    def run(self):
        with open(self.resource_file, 'r') as f:
            state = json.load(f)
        items = { }
        for r in RESOURCE_ROLE_CHOICES:
            items[r] = []
        for v in state: 
            assert 'role' in v, "'role' not found for resource %s" % v
            assert v['role'] in RESOURCE_ROLE_CHOICES, '%s not a valid role' % v['role']
            items[v['role']].append(v) 
        for r in RESOURCE_ROLE_CHOICES:
            if items[r] != []:
                click.echo('Role %s' % r)
                click.echo('-' *(5+len(r)))
                for e in items[r]:
                    self.pretty(e, indent=2) 
            else:
                click.echo('No items with role %s' % r)


    def __str__(self):
        return ("Read resources from %s" % self.resource_file)


def show_snapshot_history(ns, workspace_dir, limit, batch, verbose):
    snapshot_file = os.path.join(workspace_dir, SNAPSHOT_HISTORY_FILE)
    if not os.path.exists(snapshot_file):
        if verbose:
            click.echo('No snapshot file')
        return
    output_history = ReadSnapshotHistory(ns, verbose, snapshot_file, limit=limit)
    return output_history

def show_current_status(ns, workspace_dir, batch, verbose):
    rsrc_file = os.path.join(workspace_dir, RESOURCE_FILE)
    if not os.path.exists(rsrc_file):
        if verbose:
            click.echo('No resource file')
        return

    output_status = ReadResources(ns, verbose, rsrc_file)
    return output_status
    
def status_command(workspace_dir, history, limit, batch, verbose):
    ns = actions.Namespace()
    plan = [ ]
    plan.append(show_current_status(ns, workspace_dir, batch, verbose))
    if history:
        plan.append(show_snapshot_history(ns, workspace_dir, limit, batch, verbose))
    actions.run_plan(plan, "Show status", "shown the current status", batch=batch, verbose=verbose) 

