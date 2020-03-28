# Copyright 2018,2019 by MPI-SWS and Data-ken Research. Licensed under Apache 2.0. See LICENSE.txt.


import click

from dataworkspaces.errors import ConfigurationError
from dataworkspaces.workspace import Workspace


def add_command(scheme: str, role: str, name: str, workspace: Workspace, *args):
    current_names = set(workspace.get_resource_names())
    if workspace.batch:
        if name == None:
            name = workspace.suggest_resource_name(scheme, role, *args)
        else:
            if name in current_names:
                raise ConfigurationError("Resource name '%s' already in use" % name)
    else:
        suggested_name = None
        while (name is None) or (name in current_names):
            if suggested_name == None:
                suggested_name = workspace.suggest_resource_name(scheme, role, *args)
            name = click.prompt(
                "Please enter a short, unique name for this resource", default=suggested_name
            )
            if name in current_names:
                click.echo("Resource name '%s' already in use." % name, err=True)

    workspace.add_resource(name, scheme, role, *args)
    workspace.save("add of %s" % name)
    click.echo("Successful added resource '%s' to workspace." % name)
