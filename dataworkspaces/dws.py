#!/usr/bin/env python3
"""
Command-line tool for data workspaces
"""

__all__ = ['cli']
import sys
import click
import os.path

from .commands.init import init_command

@click.group()
@click.option('-b', '--batch', default=False, is_flag=True,
              help="Run in batch mode, never ask for user inputs")
@click.option('--verbose', default=False, is_flag=True,
              help="Print extra debugging information")
def cli(batch, verbose):
    pass

CURR_DIRNAME=os.path.basename(os.path.abspath(os.path.expanduser(os.path.curdir)))

@click.command()
@click.argument('name', default=CURR_DIRNAME)
def init(name, batch=False, verbose=False):
    """Initialize a new workspace"""
    init_command(name, batch=batch, verbose=verbose)


cli.add_command(init)

@click.command()
def clone():
    """Initialize a workspace from a remote source"""
    pass

cli.add_command(clone)

@click.command()
def add():
    """Add data to the workspace"""
    pass

cli.add_command(add)

@click.command()
def snapshot():
    """Take a snapshot of the current workspace's state"""
    pass

cli.add_command(snapshot)

@click.command()
def restore():
    """Restore the workspace to a prior state"""
    pass

cli.add_command(restore)


if __name__=='__main__':
    cli()
    sys.exit(0)

cli.add_command(snapshot)
