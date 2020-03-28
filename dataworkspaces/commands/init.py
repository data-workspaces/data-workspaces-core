# Copyright 2018,2019 by MPI-SWS and Data-ken Research. Licensed under Apache 2.0. See LICENSE.txt.
import os
from os.path import abspath, expanduser, join
from typing import List, Optional

import click

from dataworkspaces.workspace import init_workspace, RESOURCE_ROLE_CHOICES
from dataworkspaces.errors import ConfigurationError


def init_command(
    name: str,
    hostname: str,
    create_resources: List[str],
    scratch_dir: Optional[str] = None,
    git_fat_remote: Optional[str] = None,
    git_fat_user: Optional[str] = None,
    git_fat_port: Optional[int] = None,
    git_fat_attributes: Optional[str] = None,
    git_lfs_attributes: Optional[str] = None,
    batch: bool = False,
    verbose: bool = False,
):
    if git_fat_attributes and git_lfs_attributes:
        raise ConfigurationError("Cannot specify git-fat and git-lfs for the same repository")
    workspace_dir = abspath(expanduser(os.curdir))
    if scratch_dir is None:
        scratch_dir = join(workspace_dir, "scratch")
    workspace = init_workspace(
        "dataworkspaces.backends.git",  # TODO: remove hardcoding
        name,
        hostname,
        batch,
        verbose,
        scratch_dir,
        workspace_dir,
        git_fat_remote,
        git_fat_user,
        git_fat_port,
        git_fat_attributes,
        git_lfs_attributes,
    )

    if len(create_resources) > 0:
        click.echo("Will now create sub-directory resources for " + ", ".join(create_resources))
        for role in create_resources:
            assert role in RESOURCE_ROLE_CHOICES, "bad role name %s" % role
            workspace.add_resource(
                role,
                "git-subdirectory",
                role,
                join(workspace_dir, role),
                confirm_subdir_create=False,
            )
        click.echo("Finished initializing resources:")
        for role in create_resources:
            click.echo("  %s: ./%s" % (role, role))

    workspace.save("workspace initialization")
    click.echo("Workspace %s initialized successfully." % name)
