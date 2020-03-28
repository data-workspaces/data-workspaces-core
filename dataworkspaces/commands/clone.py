# Copyright 2018,2019 by MPI-SWS and Data-ken Research. Licensed under Apache 2.0. See LICENSE.txt.

import click
from dataworkspaces.workspace import clone_workspace


def clone_command(
    backend: str, hostname: str, batch: bool = False, verbose: bool = False, *args
) -> None:
    workspace = clone_workspace(backend, hostname, batch, verbose, *args)
    click.echo(
        "Completed initial clone of workspace %s, will check for resources to clone..."
        % workspace.name
    )

    rnames = [name for name in workspace.get_names_of_resources_with_local_state()]
    if len(rnames) == 0:
        click.echo("No resources with local state to clone.")
    else:
        click.echo("Will clone the following resources: %s" % ", ".join(rnames))
        for rname in rnames:
            workspace.clone_resource(rname)

    workspace.save("Clone")
    click.echo("Successfully completed clone of workspace %s." % workspace.name)
