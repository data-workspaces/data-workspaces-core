# Copyright 2018,2019 by MPI-SWS and Data-ken Research. Licensed under Apache 2.0. See LICENSE.txt.
from typing import Optional, List, cast
import click


from dataworkspaces.commands.push import build_resource_list
from dataworkspaces.errors import ConfigurationError, InternalError
from dataworkspaces.workspace import Workspace, SyncedWorkspaceMixin, CentralWorkspaceMixin


def _pull_and_clone_resources(workspace, only, skip):
    resource_list_names = build_resource_list(workspace, only, skip)
    clone_set = frozenset(workspace.get_names_for_resources_that_need_to_be_cloned())
    pull_resources = [
        workspace.get_resource(rn) for rn in resource_list_names if rn not in clone_set
    ]
    if len(pull_resources) > 0:
        click.echo("Updating resources: %s" % ", ".join([r.name for r in pull_resources]))
        workspace.pull_resources(pull_resources)
    else:
        click.echo("No resources to update.")
    clone_name_list = [rn for rn in resource_list_names if rn in clone_set]
    if len(clone_name_list) > 0:
        click.echo("Cloning new resources: %s" % ", ".join(clone_name_list))
        for rn in clone_name_list:
            workspace.clone_resource(rn)
    return len(pull_resources) + len(clone_name_list)


def pull_command(
    workspace: Workspace,
    only: Optional[List[str]] = None,
    skip: Optional[List[str]] = None,
    only_workspace: bool = False,
) -> int:

    if isinstance(workspace, SyncedWorkspaceMixin):
        # first, sync the workspace
        click.echo("Syncing workspace")
        mixin = workspace.pull_workspace()
        workspace = cast(Workspace, mixin)
        if not only_workspace:
            rcount = _pull_and_clone_resources(workspace, only, skip)
        else:
            rcount = 0
    elif isinstance(workspace, CentralWorkspaceMixin):
        if only_workspace:
            raise ConfigurationError(
                "--only-workspace not valid for central workspace %s" % workspace.name
            )
        rcount = _pull_and_clone_resources(workspace, only, skip)
    else:
        raise InternalError(
            "Workspace %s is neither a SyncedWorkspaceMixin nor a CentralWorkspaceMixin"
            % workspace.name
        )

    workspace.save("Pull command")
    return rcount
