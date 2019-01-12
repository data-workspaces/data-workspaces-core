# Copyright 2018 by MPI-SWS and Data-ken Research. Licensed under Apache 2.0. See LICENSE.txt.
import os
from os.path import isdir, join, dirname, basename
import json

import click

from dataworkspaces.resources.resource import \
    get_resource_file_path, get_resource_local_params_file_path,\
    RESOURCE_ROLE_CHOICES
import dataworkspaces.commands.actions as actions
from .params import get_all_defaults, get_local_defaults,\
                    get_local_params_file_path
from dataworkspaces import __version__
from .add import add_command

def get_config_file_path(workspace_dir):
    return join(workspace_dir, '.dataworkspace/config.json')

def get_snapshot_dir_path(workspace_dir):
    return join(workspace_dir, '.dataworkspace/snapshots')

def get_snapshot_history_file_path(workspace_dir):
    return join(get_snapshot_dir_path(workspace_dir),
                "snapshot_history.json")


class MakeWorkSpaceConfig(actions.Action):
    def __init__(self, ns, verbose, workspace_dir, workspace_name, hostname):
        super().__init__(ns, verbose)
        self.dataworkspace_dir =  join(workspace_dir, '.dataworkspace')
        self.config_fpath = get_config_file_path(workspace_dir)
        self.resources_fpath = get_resource_file_path(workspace_dir)
        self.resource_local_params_fpath = \
            get_resource_local_params_file_path(workspace_dir)
        self.local_params_fpath = get_local_params_file_path(workspace_dir)
        self.gitignore_fpath = join(workspace_dir, '.dataworkspace/.gitignore')
        self.snapshots_dir  = get_snapshot_dir_path(workspace_dir)
        self.snapshots_fpath =  get_snapshot_history_file_path(workspace_dir)
        self.workspace_name = workspace_name
        self.local_params = get_local_defaults(hostname)
        if isdir(self.dataworkspace_dir):
            raise actions.ConfigurationError(".dataworkspace already exists in %s" %
                                             dirname(self.dataworkspace_dir))

    def run(self):
        os.mkdir(self.dataworkspace_dir)
        os.mkdir(self.snapshots_dir)
        with open(self.config_fpath, 'w') as f:
            json.dump({'name':self.workspace_name, 'dws-version':__version__,
                       'global_params':get_all_defaults()},
                      f, indent=2)
        with open(self.resources_fpath, 'w') as f:
            json.dump([], f, indent=2)
        with open(self.local_params_fpath, 'w') as f:
            json.dump(self.local_params, f, indent=2)
        with open(self.resource_local_params_fpath, 'w') as f:
            json.dump({}, f, indent=2)
        with open(self.snapshots_fpath, 'w') as f:
            json.dump([], f, indent=2)
        with open(self.gitignore_fpath, 'w') as f:
            f.write("%s\n" % basename(self.local_params_fpath))
            f.write("%s\n" % basename(self.resource_local_params_fpath))
            f.write("current_lineage/\n")

    def __str__(self):
        return "Initialize .dataworkspace directory for workspace '%s'" %\
            self.workspace_name


def init_command(name, hostname, use_basic_resource_template,
                 batch=False, verbose=False):
    plan = []
    ns = actions.Namespace()
    workspace_dir = actions.CURR_DIR
    if actions.is_git_repo(workspace_dir):
        click.echo("Found a git repo, we will add to it")
    else:
        plan.append(actions.GitInit(ns, verbose, workspace_dir))
    step = MakeWorkSpaceConfig(ns, verbose, workspace_dir, name, hostname)
    config_fpath = step.config_fpath
    resources_fpath = step.resources_fpath
    snapshots_fpath = step.snapshots_fpath
    gitignore_fpath = step.gitignore_fpath
    plan.append(step)
    plan.append(actions.GitAdd(ns, verbose, workspace_dir,
                               [config_fpath, resources_fpath, snapshots_fpath,
                                gitignore_fpath]))
    plan.append(actions.GitCommit(ns, verbose, workspace_dir,
                                  'Initial version of data workspace'))
    actions.run_plan(plan, 'initialize a workspace at %s' % workspace_dir,
                     'initialized a workspace at %s' % workspace_dir,
                     batch=batch, verbose=verbose)
    if use_basic_resource_template:
        click.echo("Will now create sub-directory resources for "+
                   ", ".join(RESOURCE_ROLE_CHOICES))
        for role in RESOURCE_ROLE_CHOICES:
            add_command('git-subdirectory', role, role,
                        workspace_dir, batch, verbose,
                        join(workspace_dir, role),
                        # no prompt for subdirectory
                        False)
        click.echo("Finished initializing resources:")
        for role in RESOURCE_ROLE_CHOICES:
            click.echo("  %s: ./%s" %(role, role))




