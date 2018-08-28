import os
from os.path import isdir, join, dirname
import json

import click

import dataworkspaces.commands.actions as actions
from dataworkspaces import __version__

class MakeWorkSpaceConfig(actions.Action):
    def __init__(self, dataworkspace_dir, workspace_name, verbose):
        super().__init__(verbose)
        self.dataworkspace_dir =  dataworkspace_dir
        self.config_fpath = join(dataworkspace_dir, 'config.json')
        self.workspace_name = workspace_name
        if isdir(self.dataworkspace_dir):
            raise actions.ConfigurationError(".dataworkspace already exists in %s" %
                                             dirname(self.dataworkspace_dir))

    def run(self):
        os.mkdir(self.dataworkspace_dir)
        with open(self.config_fpath, 'w') as f:
            json.dump({'name':self.workspace_name, 'dws-version':__version__}, f,
                      indent=2)

    def __str__(self):
        return "Initialize configuration file for workspace '%s' at .dataworkspace/config.json" %\
            self.workspace_name


def init_command(name, batch=False, verbose=False):
    click.echo("init: name=%s" % name)
    plan = []
    if isdir(join(actions.CURR_DIR, '.git')):
        click.echo("Found a git repo, we will add to it")
    else:
        plan.append(actions.GitInit(actions.CURR_DIR, verbose=verbose))
    dataworkspace_dir = join(actions.CURR_DIR, '.dataworkspace')
    step = MakeWorkSpaceConfig(dataworkspace_dir, name, verbose=verbose)
    config_fpath = step.config_fpath
    plan.append(step)
    plan.append(actions.GitAdd(actions.CURR_DIR, config_fpath, verbose=verbose))
    plan.append(actions.GitCommit(actions.CURR_DIR,
                                  'Initial version of data workspace',
                                  verbose=verbose))
    return actions.run_plan(plan, 'initialize a workspace at %s' % actions.CURR_DIR,
                            'initialized a workspace at %s' % actions.CURR_DIR,
                            batch=batch, verbose=verbose)




