# Copyright 2018,2019 by MPI-SWS and Data-ken Research. Licensed under Apache 2.0. See LICENSE.txt.
from typing import Optional, List, cast
import click

from dataworkspaces.errors import ConfigurationError, UserAbort, InternalError, ApiParamError
from dataworkspaces.workspace import (
    Workspace,
    SnapshotWorkspaceMixin,
    SnapshotResourceMixin,
    ResourceRoles,
)


def restore_command(
    workspace: Workspace,
    tag_or_hash: str,
    only: Optional[List[str]] = None,
    leave: Optional[List[str]] = None,
    strict: bool = False,
) -> int:
    """Run the restore and return the number of resources affected.
    """
    if not isinstance(workspace, SnapshotWorkspaceMixin):
        raise ConfigurationError("Workspace %s does not support snapshots" % workspace.name)
    mixin = cast(SnapshotWorkspaceMixin, workspace)
    # First, find the history entry
    md = mixin.get_snapshot_by_tag_or_hash(tag_or_hash)

    # process the lists of resources
    current_names = set(workspace.get_resource_names())
    # get the non-null resources in snapshot
    snapshot_names = set(
        [rn for rn in md.restore_hashes.keys() if md.restore_hashes[rn] is not None]
    )
    all_names = current_names.union(snapshot_names)
    if (only is not None) and (leave is not None):
        raise ApiParamError("Cannot specify both only and leave for restore command.")
    elif only is not None:
        # For only, we will be a little stricter, as the user is explicitly
        # specifying the resources.
        restore_set = set(only)
        strict = True
    elif leave is not None:
        restore_set = all_names.difference(leave)
    else:
        restore_set = all_names

    # We need to remove result resources from the restore set, as we
    # do not restore them to their prior state.
    result_resources = {
        rname
        for rname in restore_set
        if workspace.get_resource_role(rname) == ResourceRoles.RESULTS
    }
    result_resources_in_restore_set = result_resources.intersection(restore_set)
    if len(result_resources_in_restore_set) > 0:
        if strict:
            raise ConfigurationError(
                "Restore set contains result resources, which cannot be restored. The following are result resources: %s"
                % ", ".join(result_resources_in_restore_set)
            )
        else:
            click.echo(
                "Skipping the restore of the following result resources, which are left in their latest state: %s"
                % ", ".join(result_resources_in_restore_set)
            )
            restore_set = restore_set.difference(result_resources)

    # error checking
    invalid = restore_set.difference(all_names)
    if len(invalid) > 0:
        raise ConfigurationError("Resource name(s) not found: %s" % ", ".join(sorted(invalid)))
    removed_names = restore_set.difference(current_names)
    if len(removed_names) > 0:
        if strict:
            raise ConfigurationError(
                "Resources have been removed from workspace or have no restore hash and strict mode is enabled."
                + " Removed resources: %s" % ", ".join(sorted(removed_names))
            )
        else:
            click.echo(
                "Skipping restore of resources that have been removed from workspace or have no restore hash: %s"
                % ", ".join(sorted(removed_names)),
                err=True,
            )
            restore_set = restore_set.difference(removed_names)
    added_names = restore_set.difference(snapshot_names)
    if len(added_names) > 0:
        if strict:
            raise ConfigurationError(
                "Resources have been added to workspace since restore, and strict mode enabled."
                + " Added resources: %s" % ", ".join(sorted(added_names))
            )
        else:
            click.echo(
                "Resources have been added to workspace since restore, will leave them as-is: %s"
                % ", ".join(sorted(added_names)),
                err=True,
            )
            restore_set = restore_set.difference(added_names)

    # get ordered list of names and resources as well as restore hashes
    restore_name_list = [rn for rn in workspace.get_resource_names() if rn in restore_set]
    if len(restore_name_list) == 0:
        click.echo("No resources to restore.")
        return 0
    restore_resource_list = [workspace.get_resource(rn) for rn in restore_name_list]
    for r in restore_resource_list:
        if not isinstance(r, SnapshotResourceMixin):
            raise InternalError(
                "Resource %s was in snapshot, but is not a SnapshotResourceMixin" % r.name
            )
    restore_hashes = {rn: md.restore_hashes[rn] for rn in restore_set}

    tagstr = " (%s)" % ",".join(md.tags) if len(md.tags) > 0 else ""
    click.echo("Restoring snapshot %s%s" % (md.hashval, tagstr))

    def fmt_rlist(rnames):
        if len(rnames) > 0:
            return ", ".join(rnames)
        else:
            return "None"

    click.echo("  Resources to restore: %s" % fmt_rlist(restore_name_list))
    names_to_leave = sorted(current_names.difference(restore_set))
    click.echo("  Resources to leave: %s" % fmt_rlist(names_to_leave))
    if not workspace.batch:
        # Unless in batch mode, we always want to ask for confirmation
        resp = input("Should I perform this restore? [Y/n]")
        if resp.lower() != "y" and resp != "":
            raise UserAbort()

    # do the work!
    mixin.restore(
        md.hashval, restore_hashes, cast(List[SnapshotResourceMixin], restore_resource_list)
    )
    workspace.save("Restore to %s" % md.hashval)

    return len(restore_name_list)
