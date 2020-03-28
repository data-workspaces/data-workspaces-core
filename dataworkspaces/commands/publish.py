import click

from dataworkspaces.workspace import Workspace, SyncedWorkspaceMixin
from dataworkspaces.errors import ConfigurationError

# XXX remote_repository should be generalized based on the backend type
# and the variants of the publish command we eventually support.
def publish_command(workspace: Workspace, remote_repository: str) -> None:
    if isinstance(workspace, SyncedWorkspaceMixin):
        workspace.publish(remote_repository)
    else:
        raise ConfigurationError(
            "Workspace %s does not support publish command; only supported for synced workspaces"
            % workspace.name
        )

    click.echo("Set remote origin to %s" % remote_repository)
