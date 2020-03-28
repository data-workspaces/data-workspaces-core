# Copyright 2018,2019 by MPI-SWS and Data-ken Research. Licensed under Apache 2.0. See LICENSE.txt.

import click
from typing import Optional, List, cast

from dataworkspaces.errors import ConfigurationError
from dataworkspaces.workspace import (
    Workspace,
    LocalStateResourceMixin,
    SyncedWorkspaceMixin,
    CentralWorkspaceMixin,
    Resource,
)


def build_resource_list(
    workspace: Workspace, only: Optional[List[str]], skip: Optional[List[str]]
) -> List[str]:
    """Build up our resource name list for either push or pull commands.
    """
    if (only is not None) and (skip is not None):
        raise ConfigurationError("Cannot specify both --only and --skip")
    all_resource_names_set = frozenset(workspace.get_resource_names())
    local_state_names_set = frozenset(workspace.get_names_of_resources_with_local_state())
    if only is not None:
        only_set = frozenset(only)
        invalid = only_set.difference(all_resource_names_set)
        if len(invalid) > 0:
            raise ConfigurationError(
                "Invalid resource names were included with --only: %s" % ", ".join(sorted(invalid))
            )
        nonsync_rnames = only_set.difference(local_state_names_set)
        if len(nonsync_rnames) > 0:
            click.echo(
                "Skipping the following resources, which do not have local state: %s"
                % ", ".join(sorted(nonsync_rnames))
            )
        return [rn for rn in only if rn in local_state_names_set]
    elif skip is not None:
        skip_set = frozenset(skip)
        invalid = skip_set.difference(all_resource_names_set)
        if len(invalid) > 0:
            raise ConfigurationError(
                "Invalid resource names were included with --skip: %s" % ", ".join(sorted(invalid))
            )
        nonsync_rnames = all_resource_names_set.difference(skip_set).difference(
            local_state_names_set
        )
        if len(nonsync_rnames) > 0:
            click.echo(
                "Skipping the following resources, which do not have local state: %s"
                % ", ".join(sorted(nonsync_rnames))
            )
        skip_set = skip_set.union(nonsync_rnames)
        return [rn for rn in workspace.get_resource_names() if rn not in skip_set]
    else:
        nonsync_rnames = all_resource_names_set.difference(local_state_names_set)
        if len(nonsync_rnames) > 0:
            click.echo(
                "Skipping the following resources, which do not have local state: %s"
                % ", ".join(sorted(nonsync_rnames))
            )
        return [rn for rn in workspace.get_resource_names() if rn not in nonsync_rnames]


def push_command(
    workspace: Workspace,
    only: Optional[List[str]] = None,
    skip: Optional[List[str]] = None,
    only_workspace: bool = False,
) -> int:
    """Run the push command on the pushable resources and the workspace.
    """
    if only_workspace:
        if isinstance(workspace, CentralWorkspaceMixin):
            raise ConfigurationError(
                "--only-workspace not valid for central workspace %s" % workspace.name
            )
        resource_list = []  # type: List[LocalStateResourceMixin]
    else:
        resource_list = [
            cast(LocalStateResourceMixin, workspace.get_resource(rn))
            for rn in build_resource_list(workspace, only, skip)
        ]

    if isinstance(workspace, CentralWorkspaceMixin):
        if len(resource_list) == 0:
            click.echo("No resources to push.")
            return 0
        else:
            print(
                "Pushing resources: %s" % ", ".join([cast(Resource, r).name for r in resource_list])
            )
            workspace.push_resources(resource_list)
    elif isinstance(workspace, SyncedWorkspaceMixin):
        if len(resource_list) > 0:
            click.echo(
                "Pushing workspace and resources: %s"
                % ", ".join([cast(Resource, r).name for r in resource_list])
            )
        elif not only_workspace:
            click.echo("No resources to push, will still push workspace")
        else:
            click.echo("Pushing workspace.")
        workspace.push(resource_list)

    workspace.save("push command")
    return len(resource_list)
