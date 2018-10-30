import os
from os.path import isdir, join, dirname, basename
import json

import click

from dataworkspaces.resources.resource import \
    get_resource_file_path, get_local_params_file_path
import dataworkspaces.commands.actions as actions
from dataworkspaces import __version__

def get_config_file_path(workspace_dir):
    return join(workspace_dir, '.dataworkspace/config.json')

def get_snapshot_dir_path(workspace_dir):
    return join(workspace_dir, '.dataworkspace/snapshots')

def get_snapshot_history_file_path(workspace_dir):
    return join(get_snapshot_dir_path(workspace_dir),
                "snapshot_history.json")

class MakeWorkSpaceConfig(actions.Action):
    def __init__(self, ns, verbose, workspace_dir, workspace_name):
        super().__init__(ns, verbose)
        self.dataworkspace_dir =  join(workspace_dir, '.dataworkspace')
        self.config_fpath = get_config_file_path(workspace_dir)
        self.resources_fpath = get_resource_file_path(workspace_dir)
        self.local_params_fpath = get_local_params_file_path(workspace_dir)
        self.gitignore_fpath = join(workspace_dir, '.dataworkspace/.gitignore')
        self.snapshots_dir = get_snapshot_dir_path(workspace_dir)
        self.snapshots_fpath =  get_snapshot_history_file_path(workspace_dir)
        self.workspace_name = workspace_name
        if isdir(self.dataworkspace_dir):
            raise actions.ConfigurationError(".dataworkspace already exists in %s" %
                                             dirname(self.dataworkspace_dir))

    def run(self):
        os.mkdir(self.dataworkspace_dir)
        os.mkdir(self.snapshots_dir)
        with open(self.config_fpath, 'w') as f:
            json.dump({'name':self.workspace_name, 'dws-version':__version__}, f,
                      indent=2)
        with open(self.resources_fpath, 'w') as f:
            json.dump([], f, indent=2)
        with open(self.local_params_fpath, 'w') as f:
            json.dump({}, f, indent=2)
        with open(self.snapshots_fpath, 'w') as f:
            json.dump([], f, indent=2)
        with open(self.gitignore_fpath, 'w') as f:
            f.write("%s\n" % basename(self.local_params_fpath))

    def __str__(self):
        return "Initialize .dataworkspace directory for workspace '%s'" %\
            self.workspace_name


def init_command(name, batch=False, verbose=False):
    plan = []
    ns = actions.Namespace()
    if actions.is_git_repo(actions.CURR_DIR):
        click.echo("Found a git repo, we will add to it")
    else:
        plan.append(actions.GitInit(ns, verbose, actions.CURR_DIR))
    step = MakeWorkSpaceConfig(ns, verbose, actions.CURR_DIR, name)
    config_fpath = step.config_fpath
    resources_fpath = step.resources_fpath
    snapshots_fpath = step.snapshots_fpath
    gitignore_fpath = step.gitignore_fpath
    plan.append(step)
    plan.append(actions.GitAdd(ns, verbose, actions.CURR_DIR,
                               [config_fpath, resources_fpath, snapshots_fpath,
                                gitignore_fpath]))
    plan.append(actions.GitCommit(ns, verbose, actions.CURR_DIR,
                                  'Initial version of data workspace'))
    return actions.run_plan(plan, 'initialize a workspace at %s' % actions.CURR_DIR,
                            'initialized a workspace at %s' % actions.CURR_DIR,
                            batch=batch, verbose=verbose)




