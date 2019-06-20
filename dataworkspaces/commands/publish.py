
from dataworkspaces.utils.git_utils import set_remote_origin

def publish_command(workspace_dir, remote_repository, batch, verbose):
    set_remote_origin(workspace_dir, remote_repository, verbose=verbose)
    print("Set remote origin to %s" % remote_repository)
