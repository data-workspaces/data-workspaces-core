# Copyright 2018,2019 by MPI-SWS and Data-ken Research. Licensed under Apache 2.0. See LICENSE.txt.
import click

from dataworkspaces.resources.resource import RESOURCE_ROLE_CHOICES



METRIC_NAME_WIDTH=18
METRIC_VAL_WIDTH=12

def print_snapshot_history(workspace, reverse=True, max_count=None):
    history = workspace.list_snapshots(reverse, max_count)
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
    for md in history:
        click.echo('%s %s %s %s %s %s' %
                   (md.hashval[0:7]+' ',
                    (', '.join(md.tags) if md.tags is not None and len(md.tags)>0 else 'N/A').ljust(20),
                    md.timestamp[0:-7],
                    (md.metric_name if md.metric_name is not None else 'N/A').ljust(METRIC_NAME_WIDTH),
                    format_metric_val(md.metric_value),
                    md.message if md.message is not None and
                                    md.message!='' else 'N/A'))
    if max_count is not None and len(history)==max_count:
        click.echo('Showing first %d snapshots' % max_count)
    else:
        click.echo("%d snapshots total" % len(history))


def pp_resource_params(params, indent=2, verbose=False):
    if params['resource_type'] == 'git':
        click.echo(' '*indent, nl=False)
        click.echo('git repo %s' % params['name'])
        if verbose:
            click.echo(' '*(indent+2)+ ('Remote: %s' % params['remote_origin_url']))
        return
    elif params['resource_type'] == 'git-subdirectory':
        click.echo(' '*indent, nl=False)
        click.echo('git subdirectory %s' % params['name'])
        if verbose:
            click.echo(' '*(indent+2)+ ('Relative path: %s' % params['relative_path']))
        return
    elif params['resource_type'] == 'file':
        click.echo(' '*indent, nl=False)
        click.echo('local files %s' % params['name'])
        if verbose:
            click.echo(' '*(indent+2), nl=False)
            click.echo('LocalPath: %s' % params['local_path'])
        return
    else:
        click.echo(' '*indent, nl=False)
        click.echo("%s %s" % (params['resource_type'], params['name']))
        if verbose:
            for p in params.keys():
                if p in ['resource_type', 'name']:
                    continue
                click.echo(' '*(indent+2), nl=False)
                click.echo("%s: %s" % (p, params[p]))

def print_resource_status(workspace):
        items = { }
        for c in RESOURCE_ROLE_CHOICES:
            items[c] = []
        for rname in workspace.get_resource_names():
            params = workspace._get_resource_params(rname)
            items[params['role']].append(params)
        for r in RESOURCE_ROLE_CHOICES:
            if items[r] != []:
                click.echo('Role %s' % r)
                click.echo('-' *(5+len(r)))
                for rp in items[r]:
                    pp_resource_params(rp, indent=2, verbose=workspace.verbose) 
            else:
                click.echo('Role %s' % r)
                click.echo('-' *(5+len(r)))
                click.echo('  No items with role %s' % r)


def status_command(workspace, history, limit):
    print("Status for workspace: %s" % workspace.name)
    print_resource_status(workspace)
    if history:
        print_snapshot_history(workspace, reverse=True, max_count=limit)


