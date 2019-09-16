#!/usr/bin/env python3
# Copyright 2018,2019 by MPI-SWS and Data-ken Research. Licensed under Apache 2.0. See LICENSE.txt.
"""
Command-line tool for data workspaces
"""

__all__ = ['cli']
import sys
import click
import re
from os.path import isdir, join, abspath, expanduser, basename, curdir
from typing import Optional
from argparse import Namespace
from collections.abc import Sequence

from dataworkspaces.commands.init import init_command
from dataworkspaces.commands.add import add_command
from dataworkspaces.commands.snapshot import snapshot_command
from dataworkspaces.commands.delete_snapshot import delete_snapshot_command
from dataworkspaces.commands.restore import restore_command
from dataworkspaces.commands.status import status_command
from dataworkspaces.commands.publish import publish_command
from dataworkspaces.commands.push import push_command
from dataworkspaces.commands.pull import pull_command
from dataworkspaces.commands.clone import clone_command
#from dataworkspaces.commands.run import run_command
from dataworkspaces.commands.diff import diff_command
from dataworkspaces.workspace import \
    RESOURCE_ROLE_CHOICES, ResourceRoles,\
    find_and_load_workspace, _find_containing_workspace
from dataworkspaces.errors import BatchModeError
from dataworkspaces.utils.param_utils import DEFAULT_HOSTNAME
from dataworkspaces.utils.regexp_utils import HOSTNAME_RE

CURR_DIR = abspath(expanduser(curdir))
CURR_DIRNAME=basename(CURR_DIR)
DWS_PATHDIR=_find_containing_workspace()

# we are going to store the verbose mode
# in a global here and wrap it in a function
# so that we can access it from __main__.
VERBOSE_MODE=False
def is_verbose_mode():
    global VERBOSE_MODE
    return VERBOSE_MODE



class WorkspaceDirParamType(click.ParamType):
    name = 'workspace-directory'

    def convert(self, value, param, ctx):
        path = abspath(expanduser(value))
        wspath = join(path, '.dataworkspace')
        if not isdir(path):
            self.fail("Directory '%s' does not exist" % value, param, ctx)
        elif not isdir(wspath):
            self.fail("No .dataworkspace directory found under '%s'. Did you run 'dws init'?"%
                      value, param, ctx)
        else:
            return path

WORKSPACE_PARAM = WorkspaceDirParamType()

class DirectoryParamType(click.ParamType):
    name = 'directory'

    def convert(self, value, param, ctx):
        path = abspath(expanduser(value))
        if not isdir(path):
            self.fail("Directory '%s' does not exist" % value, param, ctx)
        else:
            return path

DIRECTORY_PARAM = DirectoryParamType()


class RoleParamType(click.ParamType):
    name = 'role (one of %s)' % ', '.join(RESOURCE_ROLE_CHOICES)

    def convert(self, value, param, ctx):
        value = value.lower()
        if value in (ResourceRoles.SOURCE_DATA_SET, 's'):
            return ResourceRoles.SOURCE_DATA_SET
        elif value in (ResourceRoles.INTERMEDIATE_DATA, 'i'):
            return ResourceRoles.INTERMEDIATE_DATA
        elif value in (ResourceRoles.CODE, 'c'):
            return ResourceRoles.CODE
        elif value in (ResourceRoles.RESULTS, 'r'):
            return ResourceRoles.RESULTS
        else:
            self.fail("Invalid resource role. Must be one of: %s" %
                      ', '.join(RESOURCE_ROLE_CHOICES))
ROLE_PARAM = RoleParamType()

@click.group()
@click.option('-b', '--batch', default=False, is_flag=True,
              help="Run in batch mode, never ask for user inputs.")
@click.option('--verbose', default=False, is_flag=True,
              help="Print extra debugging information and ask for confirmation before running actions.")
@click.pass_context
def cli(ctx, batch, verbose):
    ctx.obj = Namespace()
    ctx.obj.batch = batch
    ctx.obj.verbose = verbose
    global VERBOSE_MODE
    VERBOSE_MODE = verbose



class HostParamType(click.ParamType):
    name = "HOSTNAME"
    #name = "Must start with a letter or number and only contain letters, numbers, '.', '_', or '-'"

    def convert(self, value, param, ctx):
        if not HOSTNAME_RE.match(value):
            self.fail("Must start with a letter or number and only contain letters, numbers, '.', '_', or '-'")
        return value

HOST_PARAM = HostParamType()

class ResourceParamType(click.ParamType):
    name = 'resources'

    def convert(self, value, param, ctx):
        parsed = []
        if isinstance(value, str):
            rl = value.lower().split(',')
        elif isinstance(value, Sequence):
            rl = value
        else:
            self.failed("Invalid resource role list '%s', must be a string or a sequence"
                        % str(value))
        for r in rl:
            if r=='all':
                return [r for r in RESOURCE_ROLE_CHOICES]
            elif r in (ResourceRoles.SOURCE_DATA_SET, 's'):
                parsed.append(ResourceRoles.SOURCE_DATA_SET)
            elif r in (ResourceRoles.INTERMEDIATE_DATA, 'i'):
                parsed.append(ResourceRoles.INTERMEDIATE_DATA)
            elif r in (ResourceRoles.CODE, 'c'):
                parsed.append(ResourceRoles.CODE)
            elif r in (ResourceRoles.RESULTS, 'r'):
                parsed.append(ResourceRoles.RESULTS)
            else:
                self.fail("Invalid resource role. Must be one of: %s, all" %
                          ', '.join(RESOURCE_ROLE_CHOICES))
        return parsed
RESOURCE_PARAM = ResourceParamType()


@click.command()
@click.option('--hostname', type=HOST_PARAM, default=None,
              help="Hostname to identify this machine in snapshot directory paths, "+
                   "defaults to " + DEFAULT_HOSTNAME)
@click.option('--create-resources', default=[], type=RESOURCE_PARAM,
              help="Initialize the workspace with subdirectories for the specified resource roles. Choices are 'all' or any comma-separated combination of %s."
              % ', '.join(RESOURCE_ROLE_CHOICES))
@click.option('--git-fat-remote', type=str,
              help="Initialize the workspace with the git-fat large file extension "+
                   "and use the specified URL for the remote datastore")
@click.option('--git-fat-user', type=str, default=None,
              help="Username for git fat remote (if not root)")
@click.option('--git-fat-port', type=int, default=None,
              help="Port number for git-fat remote (defaults to 22)")
@click.option('--git-fat-attributes', type=str, default=None,
              help="Comma-separated list of file patterns to manage under git-fat."+
              " For example --git-fat-attributes='*.gz,*.zip'. If you do not specify"+
              " here, you can always add the .gitattributes file later.")
@click.argument('name', default=CURR_DIRNAME)
@click.pass_context
def init(ctx, hostname, name, create_resources, git_fat_remote,
         git_fat_user, git_fat_port, git_fat_attributes):
    """Initialize a new workspace"""
    if hostname is None:
        if not ctx.obj.batch:
            hostname = click.prompt("What name do you want to use for this machine in snapshot directory paths?",
                                    default=DEFAULT_HOSTNAME, type=HOST_PARAM)
        else:
            hostname = DEFAULT_HOSTNAME
    if ((git_fat_user is not None) or (git_fat_port is not None) or
        (git_fat_attributes is not None)) and (git_fat_remote is None):
        raise click.BadOptionUsage(message="If you specify --git-fat-user, --git-fat-port-, or --git-fat-attributes, "+
                                   "you also need to specify --git-fat-remote",
                                   option_name='--git-fat-remote')
    init_command(name, hostname, create_resources, git_fat_remote,
                 git_fat_user, git_fat_port, git_fat_attributes,
                 batch=ctx.obj.batch, verbose=ctx.obj.verbose)


cli.add_command(init)


# The add command has subcommands for each resource type.
# This should be dynamically extensible, but we will hard
# code things for now.
@click.group()
@click.option('--workspace-dir', type=WORKSPACE_PARAM, default=DWS_PATHDIR)
@click.pass_context
def add(ctx, workspace_dir):
    """Add a data collection to the workspace as a resource. 
       Possible types of resources are ``git``, ``local-files``, or ``rclone``; these are subcommands of add."""
    ns = ctx.obj
    if workspace_dir is None:
        if ns.batch:
            raise BatchModeError("--workspace-dir")
        else:
            workspace_dir = click.prompt("Please enter the workspace root dir",
                                         type=WORKSPACE_PARAM)
        
    ns.workspace_dir = workspace_dir

cli.add_command(add)

@click.command(name='local-files')
@click.option('--role', type=ROLE_PARAM)
@click.option('--name', type=str, default=None,
              help="Short name for this resource")
@click.option('--compute-hash', is_flag=True, default=False,
              help="Compute hashes for all files. If this option is not set, we use a lightweight comparison of file sizes only.")
@click.argument('path', type=DIRECTORY_PARAM)
@click.pass_context
def local_files(ctx, role, name, path, compute_hash):
    """Add a local file directory (not managed by git) to the workspace. Subcommand of ``add``"""
    ns = ctx.obj
    if role is None:
        if ns.batch:
            raise BatchModeError("--role")
        else:
            role = click.prompt("Please enter a role for this resource, one of [s]ource-data, [i]ntermediate-data, [c]ode, or [r]esults", type=ROLE_PARAM)
    path = abspath(expanduser(path))
    workspace = find_and_load_workspace(ns.batch, ns.verbose, ns.workspace_dir)
    add_command('file', role, name, workspace, path, compute_hash)

add.add_command(local_files)

@click.command()
@click.option('--role', type=ROLE_PARAM)
@click.option('--name', type=str, default=None,
              help="Short name for this resource")
@click.option('--config', type=str, default=None,
              help="Configuration file for rclone")
@click.option('--compute-hash', is_flag=True, default=False,
              help="Compute hashes for all files")
@click.argument('source', type=str)
@click.argument('dest', type=str) # Currently, dest is required. Later: make dest optional and use the same path as remote?
@click.pass_context
def rclone(ctx, role, name, config, compute_hash, source, dest): 
    """Add an rclone-d repository as a resource to the workspace. Subcommand of ``add``"""
    ns = ctx.obj
    if role is None:
        if ns.batch:
            raise BatchModeError("--role")
        else:
            role = click.prompt("Please enter a role for this resource, one of [s]ource-data, [i]ntermediate-data, [c]ode, or [r]esults", type=ROLE_PARAM)
    rclone_re = r'.*:.*'
    if re.match(rclone_re, source) == None:
        raise click.BadOptionUsage(message="Source in rclone should be specified as remotename:filepath",
                                   option_name='source')
    dest = abspath(expanduser(dest))
    workspace = find_and_load_workspace(ns.batch, ns.verbose, ns.workspace_dir)
    add_command('rclone', role, name, workspace, source, dest, config, compute_hash)

add.add_command(rclone)

@click.command()
@click.option('--role', type=ROLE_PARAM)
@click.option('--name', type=str, default=None,
              help="Short name for this resource")
@click.option('--branch', type=str, default='master',
              help="Branch of the repo to use, defaults to master.")
@click.option('--read-only', '-r', is_flag=True, default=False,
              help='If specified, treat the origin repository as read-only and never push to it.')
@click.argument('path', type=str)
@click.pass_context
def git(ctx, role, name, branch, read_only, path): 
    """Add a local git repository as a resource. Subcommand of ``add``"""
    ns = ctx.obj
    if role is None:
        if ns.batch:
            raise BatchModeError("--role")
        else:
            role = click.prompt("Please enter a role for this resource, one of [s]ource-data, [i]ntermediate-data, [c]ode, or [r]esults", type=ROLE_PARAM)
    path = abspath(expanduser(path))
    workspace = find_and_load_workspace(ns.batch, ns.verbose, ns.workspace_dir)
    add_command('git', role, name, workspace, path, branch, read_only)

add.add_command(git)


@click.command()
@click.option('--workspace-dir', type=WORKSPACE_PARAM, default=DWS_PATHDIR)
@click.option('--message', '-m', type=str, default='',
              help="Message describing the snapshot")
@click.argument('tag', type=HOST_PARAM, default=None, required=False)
@click.pass_context
def snapshot(ctx, workspace_dir, message, tag):
    """Take a snapshot of the current workspace's state"""
    ns = ctx.obj
    if workspace_dir is None:
        if ns.batch:
            raise BatchModeError("--workspace-dir")
        else:
            workspace_dir = click.prompt("Please enter the workspace root dir",
                                         type=WORKSPACE_PARAM)
    workspace = find_and_load_workspace(ns.batch, ns.verbose, workspace_dir)
    snapshot_command(workspace, tag, message)

cli.add_command(snapshot)

@click.command()
@click.option('--workspace-dir', type=WORKSPACE_PARAM, default=DWS_PATHDIR)
@click.option('--no-include-resources', is_flag=True, default=False,
              help="If specified, do NOT include deleting an snapshot-specific content from resources.")
@click.argument('tag_or_hash', type=HOST_PARAM, default=None, required=True)
@click.pass_context
def delete_snapshot(ctx, workspace_dir:str, no_include_resources:bool,
                    tag_or_hash:str):
    """Delete the specified snapshot. This includes the metadata and lineage
    data for the snapshot. Unless --no-include-resources is specified, this
    also deletes any results data saved for the snapshot (under the
    snapshots subdirectory of a results resource)."""
    ns = ctx.obj
    if workspace_dir is None:
        if ns.batch:
            raise BatchModeError("--workspace-dir")
        else:
            workspace_dir = click.prompt("Please enter the workspace root dir",
                                         type=WORKSPACE_PARAM)
    workspace = find_and_load_workspace(ns.batch, ns.verbose, workspace_dir)
    delete_snapshot_command(workspace, tag_or_hash, no_include_resources)

cli.add_command(delete_snapshot)

@click.command()
@click.option('--workspace-dir', type=WORKSPACE_PARAM, default=DWS_PATHDIR)
@click.option('--only', type=str, default=None,
              metavar="RESOURCE1[,RESOURCE2,...]",
              help="Comma-separated list of resource names that you wish to revert to the specified snapshot. The rest will be left as-is.")
@click.option('--leave', type=str, default=None,
              metavar="RESOURCE1[,RESOURCE2,...]",
              help="Comma-separated list of resource names that you wish to leave in their current state. The rest will be restored to the specified snapshot.")
@click.option('--strict', is_flag=True, default=False,
              help="If specified, error out if unable to restore any of the requested resources "+
                   "(due to lack of a restore hash or removing the resource from workspace).")
@click.argument('tag_or_hash', type=str, default=None, required=True)
@click.pass_context
def restore(ctx, workspace_dir:str, only:Optional[str], leave:Optional[str], strict:bool, tag_or_hash:str):
    """Restore the workspace to a prior state"""
    ns = ctx.obj
    if (only is not None) and (leave is not None):
        raise click.BadOptionUsage(message="Please specify either --only or --leave, but not both",
                                   option_name='--only')
    if workspace_dir is None:
        if ns.batch:
            raise BatchModeError("--workspace-dir")
        else:
            workspace_dir = click.prompt("Please enter the workspace root dir",
                                         type=WORKSPACE_PARAM)
    workspace = find_and_load_workspace(ns.batch, ns.verbose, workspace_dir)
    restore_command(workspace, tag_or_hash,
                    only=only.split(',') if only else None,
                    leave=leave.split(',') if leave else None,
                    strict=strict)

cli.add_command(restore)


@click.command()
@click.option('--workspace-dir', type=WORKSPACE_PARAM, default=DWS_PATHDIR)
@click.option('--skip', type=str, default=None,
              metavar="RESOURCE1[,RESOURCE2,...]",
              help="Comma-separated list of resource names that you wish to skip when pushing. The rest will be pushed to their remote origins, if applicable.")
@click.argument('remote-repository', type=str, default=None, required=True)
@click.pass_context
def publish(ctx, workspace_dir, skip:str, remote_repository):
    """Add a remote Git repository as the origin for the workspace and
    do the initial push of the workspace and any other resources.
    """
    ns = ctx.obj
    if workspace_dir is None:
        if ns.batch:
            raise BatchModeError("--workspace-dir")
        else:
            workspace_dir = click.prompt("Please enter the workspace root dir",
                                         type=WORKSPACE_PARAM)
    workspace = find_and_load_workspace(ns.batch, ns.verbose, workspace_dir)
    publish_command(workspace, remote_repository)
    push_command(workspace, only=None, skip=skip.split(',') if skip else None, only_workspace=False)

cli.add_command(publish)


@click.command()
@click.option('--workspace-dir', type=WORKSPACE_PARAM, default=DWS_PATHDIR)
@click.option('--only', type=str, default=None,
              metavar="RESOURCE1[,RESOURCE2,...]",
              help="Comma-separated list of resource names that you wish to push to the origin, if applicable. The rest will be skipped.")
@click.option('--skip', type=str, default=None,
              metavar="RESOURCE1[,RESOURCE2,...]",
              help="Comma-separated list of resource names that you wish to skip when pushing. The rest will be pushed to their remote origins, if applicable.")
@click.option('--only-workspace', is_flag=True, default=False,
              help="Only push the workspace's metadata, skipping the individual resources")
@click.pass_context
def push(ctx, workspace_dir:str, only:Optional[str], skip:Optional[str], only_workspace:bool):
    """Push the state of the workspace and its resources to their origins."""
    ns = ctx.obj
    option_cnt = (1 if only is not None else 0) + (1 if skip is not None else 0) + \
                 (1 if only_workspace else 0)
    if option_cnt>1:
        raise click.BadOptionUsage(message="Please specify at most one of --only, --skip, or --only-workspace",
                                   option_name='--only')
    if workspace_dir is None:
        if ns.batch:
            raise BatchModeError("--workspace-dir")
        else:
            workspace_dir = click.prompt("Please enter the workspace root dir",
                                         type=WORKSPACE_PARAM)
    workspace = find_and_load_workspace(ns.batch, ns.verbose, workspace_dir)
    push_command(workspace,
                 only=only.split(',') if only else None,
                 skip=skip.split(',') if skip else None,
                 only_workspace=only_workspace)

cli.add_command(push)


@click.command()
@click.option('--workspace-dir', type=WORKSPACE_PARAM, default=DWS_PATHDIR)
@click.option('--only', type=str, default=None,
              metavar="RESOURCE1[,RESOURCE2,...]",
              help="Comma-separated list of resource names that you wish to pull from the origin, if applicable. The rest will be skipped.")
@click.option('--skip', type=str, default=None,
              metavar="RESOURCE1[,RESOURCE2,...]",
              help="Comma-separated list of resource names that you wish to skip when pulling. The rest will be pulled from their remote origins, if applicable.")
@click.option('--only-workspace', is_flag=True, default=False,
              help="Only pull the workspace's metadata, skipping the individual resources")
@click.pass_context
def pull(ctx, workspace_dir:str, only:Optional[str], skip:Optional[str], only_workspace:bool):
    """Pull the latest state of the workspace and its resources from their origins."""
    ns = ctx.obj
    option_cnt = (1 if only is not None else 0) + (1 if skip is not None else 0) + \
                 (1 if only_workspace else 0)
    if option_cnt>1:
        raise click.BadOptionUsage(message="Please specify at most one of --only, --skip, or --only-workspace",
                                   option_name='--only')
    if workspace_dir is None:
        if ns.batch:
            raise BatchModeError("--workspace-dir")
        else:
            workspace_dir = click.prompt("Please enter the workspace root dir",
                                         type=WORKSPACE_PARAM)
    workspace = find_and_load_workspace(ns.batch, ns.verbose, workspace_dir)
    pull_command(workspace,
                 only=only.split(',') if only else None,
                 skip=skip.split(',') if skip else None,
                 only_workspace=only_workspace)

cli.add_command(pull)


@click.command()
@click.option('--hostname', type=HOST_PARAM, default=None,
              help="Hostname to identify this machine in snapshot directory paths, "+
                   "defaults to " + DEFAULT_HOSTNAME)
@click.argument('repository', type=str, default=None, required=True)
@click.argument('directory', type=str, default=None, required=False)
@click.pass_context
def clone(ctx, hostname, repository, directory):
    """Clone the specified data workspace."""
    ns = ctx.obj
    if hostname is None:
        if not ns.batch:
            hostname = click.prompt("What name do you want to use for this machine in snapshot directory paths?",
                                    default=DEFAULT_HOSTNAME, type=HOST_PARAM)
        else:
            hostname = DEFAULT_HOSTNAME
    clone_command('dataworkspaces.backends.git', hostname, ns.batch, ns.verbose, repository, directory)

cli.add_command(clone)


@click.command()
@click.option('--workspace-dir', type=WORKSPACE_PARAM, default=DWS_PATHDIR)
@click.option('--history', is_flag=True, default=False, help='Show previous snapshots')
@click.option('--limit', type=int, default=None,
              help='Number of previous snapshots to show (most recent first)')
@click.pass_context
def status(ctx, workspace_dir, history, limit):
    """Show the history of snapshots"""
    ns = ctx.obj
    if workspace_dir is None:
        if ns.batch:
            raise BatchModeError("--workspace-dir")
        else:
            workspace_dir = click.prompt("Please enter the workspace root dir",
                                         type=WORKSPACE_PARAM)
    workspace = find_and_load_workspace(ns.batch, ns.verbose, workspace_dir)
    status_command(workspace, history, limit)

cli.add_command(status)


# Disable run command for now, until we better understand how it interacts with the
# Lineage API. TODO: re-enable with the proper integration.
# @click.command()
# @click.option('--workspace-dir', type=WORKSPACE_PARAM, default=DWS_PATHDIR)
# @click.option('--step-name', default=None,
#               help="Name of step to associate with this command (defaults to base name of command without file extension)")
# @click.option('--cwd', default=CURR_DIR,
#               type=click.Path(exists=True, file_okay=False, dir_okay=True, readable=True, resolve_path=True),
#               help="Directory to use as current working directory when running command (defaults to %s)"%CURR_DIR)
# @click.argument('command', type=str, required=True)
# @click.argument('command_arguments', type=str, nargs=-1)
# @click.pass_context
# def run(ctx, workspace_dir, step_name, cwd, command, command_arguments):
#     """Run a data pipeline step from the command line, recording lineage information."""
#     ns = ctx.obj
#     if workspace_dir is None:
#         if ns.batch:
#             raise BatchModeError("--workspace-dir")
#         else:
#             workspace_dir = click.prompt("Please enter the workspace root dir",
#                                          type=WORKSPACE_PARAM)
#     run_command(workspace_dir, step_name, cwd, command, command_arguments, ns.batch, ns.verbose)

# cli.add_command(run)


@click.command()
@click.option('--workspace-dir', type=WORKSPACE_PARAM, default=DWS_PATHDIR)
@click.argument('snapshot_or_tag1', metavar='SNAPSHOT_OR_TAG1', type=str)
@click.argument('snapshot_or_tag2', metavar='SNAPSHOT_OR_TAG2', type=str)
@click.pass_context
def diff(ctx, workspace_dir, snapshot_or_tag1, snapshot_or_tag2):
    """List differences between two snapshots"""
    ns = ctx.obj
    if workspace_dir is None:
        if ns.batch:
            raise BatchModeError("--workspace-dir")
        else:
            workspace_dir = click.prompt("Please enter the workspace root dir",
                                         type=WORKSPACE_PARAM)
    workspace = find_and_load_workspace(ns.batch, ns.verbose, workspace_dir)
    diff_command(workspace, snapshot_or_tag1, snapshot_or_tag2)

cli.add_command(diff)

# from .commands.show import show_command
# @click.command()
# @click.option('--workspace-dir', type=WORKSPACE_PARAM, default=DWS_PATHDIR)
# @click.pass_context
# def show(ctx, workspace_dir, limit):
#     """Show the current state of the workspace"""
#     ns = ctx.obj
#     show_command(workspace_dir, limit, ns.batch, ns.verbose)
# 
# cli.add_command(show)


if __name__=='__main__':
    cli()
    sys.exit(0)

cli.add_command(snapshot)
