# Copyright 2018,2019 by MPI-SWS and Data-ken Research. Licensed under Apache 2.0. See LICENSE.txt.
import click
from typing import cast


from dataworkspaces.errors import ConfigurationError, UserAbort
from dataworkspaces.workspace import Workspace, SnapshotWorkspaceMixin


def delete_snapshot_command(
    workspace: Workspace, tag_or_hash: str, no_include_resources: bool = True
) -> None:
    if not isinstance(workspace, SnapshotWorkspaceMixin):
        raise ConfigurationError("Workspace %s does not support snapshots." % workspace.name)
    mixin = cast(SnapshotWorkspaceMixin, workspace)
    md = mixin.get_snapshot_by_tag_or_hash(tag_or_hash)
    snapshot_name = (
        "%s (Tagged as: %s)" % (md.hashval[0:7], ", ".join(md.tags))
        if md.tags is not None
        else md.hashval
    )
    if not workspace.batch:
        if not click.confirm(
            "Should I delete snapshot %s? This is not reversible." % snapshot_name
        ):
            raise UserAbort()
    mixin.delete_snapshot(md.hashval, include_resources=not no_include_resources)
    workspace.save("Deleted snapshot %s" % snapshot_name)
    click.echo("Successfully deleted snapshot %s." % snapshot_name)
