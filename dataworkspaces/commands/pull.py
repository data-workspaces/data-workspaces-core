# Copyright 2018,2019 by MPI-SWS and Data-ken Research. Licensed under Apache 2.0. See LICENSE.txt.
from typing import Optional, List, cast
import click


from dataworkspaces.commands.push import build_resource_list
from dataworkspaces.errors import ConfigurationError, InternalError
from dataworkspaces.workspace import Workspace,\
    SyncedWorkspaceMixin, CentralWorkspaceMixin

# XXX need to port lineage
# class InvalidateLineage(actions.Action):
#     def __init__(self, ns, verbose, current_lineage_dir, pulled_resource_names):
#         super().__init__(ns, verbose)
#         self.current_lineage_dir = current_lineage_dir
#         self.pulled_resource_names = pulled_resource_names

#     def run(self):
#         LineageStoreCurrent.invalidate_fsstore_entries(self.current_lineage_dir,
#                                                        self.pulled_resource_names)

#     def __str__(self):
#         return 'Invalidate lineage for resources: %s' % \
#             ', '.join(self.pulled_resource_names)



def pull_command(workspace:Workspace, only:Optional[List[str]]=None,
                 skip:Optional[List[str]]=None, only_workspace:bool=False) -> int:

    rcount = 0
    if isinstance(workspace, SyncedWorkspaceMixin):
        # first, sync the workspace
        original_resource_set = frozenset(workspace.get_resource_names()) # capture before syncing
        click.echo("Syncing workspace")
        mixin = workspace.pull_workspace()
        workspace = cast(Workspace, mixin)
        if not only_workspace:
            resource_list = build_resource_list(workspace, only, skip)
            if len(resource_list)>0:
                click.echo("Updating resources: %s" % ', '.join([r.name for r in resource_list]))
                mixin.pull_resources(resource_list)
            else:
                click.echo("No resources to update.")
            local_state_resources = frozenset(workspace.get_names_of_resources_with_local_state())
            new_resource_names = (frozenset(workspace.get_resource_names()).difference(original_resource_set)).intersection(local_state_resources)
            if len(new_resource_names)>0:
                rcount += len(new_resource_names)
                click.echo("Cloning new resources: %s" % ', '.join(sorted(new_resource_names)))
                for rn in new_resource_names:
                    workspace.clone_resource(rn)
    elif isinstance(workspace, CentralWorkspaceMixin):
        if only_workspace:
            raise ConfigurationError("--only-workspace not valid for central workspace %s"%
                                     workspace.name)
        resource_list = build_resource_list(workspace, only, skip)
        if len(resource_list)>0:
            click.echo("Updating resources: %s" % ', '.join([r.name for r in resource_list]))
            workspace.pull_resources()
            rcount = len(resource_list)
        else:
            click.echo("No resources to update.")
        resources_to_be_cloned = frozenset(workspace.get_resources_that_need_to_be_cloned())\
                                    .intersection(
                                        frozenset(workspace.get_names_of_resources_with_local_state()))
        if len(resources_to_be_cloned)>0:
            click.echo("Cloning new resources: %s" % ', '.join(sorted(resources_to_be_cloned)))
            rcount += len(resources_to_be_cloned)
            for rname in resources_to_be_cloned:
                workspace.clone_resource(rname)
    else:
        raise InternalError("Workspace %s is neither a SyncedWorkspaceMixin nor a CentralWorkspaceMixin"%
                            workspace.name)
    workspace.save("Pull command")
    return rcount
