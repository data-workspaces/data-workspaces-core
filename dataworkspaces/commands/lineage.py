"""Lineage related commands
"""
import click
from typing import Optional

from dataworkspaces.workspace import Workspace, SnapshotWorkspaceMixin, ResourceRoles
from dataworkspaces.errors import ConfigurationError
from dataworkspaces.utils.lineage_utils import make_simplified_lineage_graph_for_resource


def lineage_graph_command(
    workspace: Workspace,
    output_file: str,
    resource_name: Optional[str],
    snapshot: Optional[str],
    format="html",
    width: int = 1024,
    height: int = 800,
) -> None:
    if not isinstance(workspace, SnapshotWorkspaceMixin):
        raise ConfigurationError(
            "Workspace %s does not support snapshots and lineage" % workspace.name
        )
    if not workspace.supports_lineage():
        raise ConfigurationError("Workspace %s does not support lineage" % workspace.name)
    store = workspace.get_lineage_store()

    snapshot_hash = None  # type: Optional[str]
    if snapshot is not None:
        md = workspace.get_snapshot_by_tag_or_hash(snapshot)
        snapshot_hash = md.hashval
    if resource_name is not None:
        workspace.validate_resource_name(resource_name)
    else:
        for r in workspace.get_resource_names():
            if workspace.get_resource_role(r) == ResourceRoles.RESULTS:
                resource_name = r
                break
        if resource_name is None:
            raise ConfigurationError(
                "Did not find a results resource in workspace. If you want to graph the lineage of a non-results resource, use the --resource option."
            )
    make_simplified_lineage_graph_for_resource(
        workspace.get_instance(),
        store,
        resource_name,
        output_file,
        snapshot_hash=snapshot_hash,
        format=format,
        width=width,
        height=height,
    )
    if snapshot is None:
        click.echo("Wrote lineage for %s to %s" % (resource_name, output_file))
    else:
        click.echo(
            "Wrote lineage for %s as of snapshot %s to %s" % (resource_name, snapshot, output_file)
        )
