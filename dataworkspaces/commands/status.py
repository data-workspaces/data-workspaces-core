# Copyright 2018,2019 by MPI-SWS and Data-ken Research. Licensed under Apache 2.0. See LICENSE.txt.
import os
from os.path import join, isdir
import json

import click

import dataworkspaces.commands.actions as actions
from .init import get_snapshot_metadata_dir_path
from dataworkspaces.resources.resource import \
    RESOURCE_ROLE_CHOICES, get_resource_file_path


def get_snapshot_metadata(workspace, reverse=True):
    def process_dir(dirpath):
        for f in os.listdir(dirpath):
            p = join(dirpath, f)
            if isdir(p):
                process_dir(p)
            elif f.endswith('_md.json'):
                with open(p, 'r') as fobj:
                    data = json.load(fobj)
                    data['metric'] = None
                    data['value']   = None
                    # print("Data in get_snapshot_metadata = ", data)
                    rel_dest_path = data['relative_destination_path']
                    results_file = join(join(join(workspace, 'results'), rel_dest_path), 'results.json') 
                    # print("results_file at ", results_file)
                    if os.path.exists(results_file):
                        with open(results_file, 'r') as rfile:
                            results_json = json.load(rfile)
                            metrics = results_json.get('metrics', None)
                            if metrics:
                                data['metric'] = next(iter(metrics.keys())) # get the first attribute from metrics
                                                                             # we'll print this in the status summary
                                data['value'] =   metrics[data['metric']] 
                yield data
    md_path = get_snapshot_metadata_dir_path(workspace)
    metadata = [data for data in process_dir(md_path)]
    metadata.sort(key=lambda data:data['timestamp'], reverse=reverse)
    return metadata

METRIC_NAME_WIDTH=18
METRIC_VAL_WIDTH=12

class ReadSnapshotHistory(actions.Action):
    def __init__(self, ns, verbose, workspace_dir, limit=None):
        super().__init__(ns, verbose)
        self.workspace_dir = workspace_dir
        self.limit = limit

    def run(self):
        # with open(self.snapshot_history_file, 'r') as f:
        #     history = json.load(f)
        #     num_snapshots = len(history)
        history = get_snapshot_metadata(self.workspace_dir)
        click.echo("\nHistory of snapshots")
        click.echo("%s %s %s %s %s %s" %
                   ('Hash'.ljust(8), 'Tags'.ljust(20), 'Created'.ljust(19),
                    'Metric'.ljust(METRIC_NAME_WIDTH),
                    'Value'.ljust(METRIC_VAL_WIDTH),
                    'Message'))
        def format_metric_val(val):
            if val is None:
                return 'N/A'.ljust(METRIC_VAL_WIDTH)
            elif not isinstance(val, float):
                return str(val).ljust(METRIC_VAL_WIDTH)
            elif val<1.0 and val>-1.0:
                return ('%.3f'%val).ljust(METRIC_VAL_WIDTH)
            else:
                return ('%.1f'%val).ljust(METRIC_VAL_WIDTH)
        for v in history[0:self.limit] if self.limit is not None else history:
            click.echo('%s %s %s %s %s %s' %
                       (v['hash'][0:7]+' ',
                        (', '.join(v['tags']) if v['tags'] is not None else 'N/A').ljust(20),
                        v['timestamp'][0:-7],
                        (v['metric'] if v['metric'] is not None else 'N/A').ljust(METRIC_NAME_WIDTH),
                        format_metric_val(v.get('value', None)),
                        v['message'] if v['message'] is not None and
                                        v['message']!='' else 'N/A'))
        num_shown = len(history) if self.limit is None \
                    else min(self.limit, len(history))
        click.echo('Showing %d of %d snapshots' %
                   (num_shown, len(history)))

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
                click.echo(' '*(indent+2)+ ('Remote: %s' % rsrc['remote_origin_url']))
            return
        elif rsrc['resource_type'] == 'git-subdirectory':
            click.echo(' '*indent, nl=False)
            click.echo('git subdirectory %s' % rsrc['name'])
            if self.verbose:
                click.echo(' '*(indent+2)+ ('Relative path: %s' % rsrc['relative_path']))
            return
        elif rsrc['resource_type'] == 'file':
            click.echo(' '*indent, nl=False)
            click.echo('local files %s' % rsrc['name'])
            if self.verbose:
                click.echo(' '*(indent+2), nl=False)
                click.echo('LocalPath: %s' % rsrc['local_path'])
            return
        else:
            click.echo(' '*indent, nl=False)
            click.echo("%s %s" % (rsrc['resource_type'], rsrc['name']))
            if self.verbose:
                for p in rsrc.keys():
                    if p in ['resource_type', 'name']:
                        continue
                    click.echo(' '*(indent+2), nl=False)
                    click.echo("%s: %s" % (p, rsrc[p]))

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
                click.echo('Role %s' % r)
                click.echo('-' *(5+len(r)))
                click.echo('  No items with role %s' % r)


    def __str__(self):
        return ("Read resources from %s" % self.resource_file)


def show_snapshot_history(ns, workspace_dir, limit, batch, verbose):
    # snapshot_file = os.path.join(workspace_dir, SNAPSHOT_HISTORY_FILE)
    # if not os.path.exists(snapshot_file):
    #     if verbose:
    #         click.echo('No snapshot file')
    #     return
    output_history = ReadSnapshotHistory(ns, verbose, workspace_dir,
                                         limit=limit)
    return output_history

def show_current_status(ns, workspace_dir, batch, verbose):
    rsrc_file = get_resource_file_path(workspace_dir)
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

