import os
from os.path import curdir, abspath, expanduser
from typing import Optional
import click

from repo2docker.__main__ import make_r2d  # type: ignore

from dataworkspaces.workspace import Workspace
import dataworkspaces.backends.git as git_backend
from dataworkspaces.utils.git_utils import get_git_config_param, get_remote_origin_url
from dataworkspaces.utils.subprocess_utils import call_subprocess


def deploy_build_command(
    workspace: Workspace,
    image_name: Optional[str],
    force_rebuild: bool,
    git_user_email: Optional[str],
    git_user_name: Optional[str],
) -> None:
    target_repo_dir = "/home/%s/%s" % (os.environ["USER"], workspace.name)
    if image_name is None:
        image_name = workspace.name
    argv = ["--target-repo-dir", target_repo_dir, "--image-name", image_name, "--no-run"]
    if isinstance(workspace, git_backend.Workspace):
        workspace_dir = workspace.get_workspace_local_path_if_any()
        assert workspace_dir is not None
        user_email = (
            git_user_email
            if git_user_email
            else get_git_config_param(workspace_dir, "user.email", verbose=workspace.verbose)
        )
        user_name = (
            git_user_name
            if git_user_name
            else get_git_config_param(workspace_dir, "user.name", verbose=workspace.verbose)
        )
        argv.append(
            '--appendix=RUN git config --global user.email %s; git config --global user.name "%s"'
            % (user_email, user_name)
        )
        argv.append("dws+" + get_remote_origin_url(workspace_dir, verbose=workspace.verbose))
    else:
        # need to figure out how the clone url works for a non-git workspace
        assert 0, "build not yet implemented for non-git workspaces"

    if force_rebuild:
        click.echo("Forcing remove of image %s." % image_name)
        call_subprocess(
            ["docker", "image", "rm", "-f", "--no-prune", image_name],
            cwd=curdir,
            verbose=workspace.verbose,
        )
    if workspace.verbose:
        click.echo("Command args for repo2docker: %s" % repr(argv))
    r2d = make_r2d(argv=argv)
    r2d.initialize()
    r2d.start()
    click.echo("Build of image %s was successful." % image_name)


def deploy_run_command(
    workspace: Workspace, image_name: Optional[str], no_mount_ssh_keys: bool
) -> None:
    target_repo_dir = "/home/%s/%s" % (os.environ["USER"], workspace.name)
    if image_name is None:
        image_name = workspace.name
    argv = [
        "--target-repo-dir",
        target_repo_dir,
        "--image-name",
        image_name,
    ]
    if not no_mount_ssh_keys:
        dot_ssh = abspath(expanduser("~/.ssh"))
        argv.append("-v")
        argv.append("%s:/home/%s/.ssh" % (dot_ssh, os.environ["USER"]))
    if isinstance(workspace, git_backend.Workspace):
        workspace_dir = workspace.get_workspace_local_path_if_any()
        assert workspace_dir is not None
        argv.append("dws+" + get_remote_origin_url(workspace_dir, verbose=workspace.verbose))
    else:
        # need to figure out how the clone url works for a non-git workspace
        assert 0, "run build not yet implemented for non-git workspaces"
    if workspace.verbose:
        click.echo("Command args for repo2docker: %s" % repr(argv))
    r2d = make_r2d(argv=argv)
    r2d.initialize()
    r2d.run_image()
    click.echo("Run of image %s was successful." % image_name)
