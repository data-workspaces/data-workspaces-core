
import click

import dataworkspaces.commands.actions as actions


def pull_command(workspace_dir, batch=False, verbose=False,
                 only=None, skip=None, only_workspace=False):
    plan = []
    ns = actions.Namespace()
    print("Pull not yet implemented!")



