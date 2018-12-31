"""Utilities related to calling subprocesses
"""
from subprocess import run, PIPE
import os
from os.path import abspath, expanduser, join, isfile
import click

def call_subprocess(args, cwd, verbose=False):
    """Call an executable as a child process. Returns the standard output.
    If it fails, we will print
    an error and allow CalledProcessError to be thrown.
    """
    if verbose:
        click.echo(" ".join(args) + " [run in %s]" % cwd)
    cp = run(args, cwd=cwd, encoding='utf-8', stdout=PIPE, stderr=PIPE)
    cp.check_returncode()
    if verbose:
        click.echo(cp.stdout)
    return cp.stdout # can ignore if you don't need this.

def call_subprocess_for_rc(args, cwd, verbose=False):
    """Call an executable as a child process. Returns the return
    code of the call.
    """
    if verbose:
        click.echo(" ".join(args) + " [run in %s]" % cwd)
    cp = run(args, cwd=cwd, encoding='utf-8', stdout=PIPE, stderr=PIPE)
    if verbose:
        click.echo(cp.stdout)
        click.echo("%s exited with %d" % (args[0], cp.returncode))
    return cp.returncode


def find_exe(exe_name, additional_search_locations=[]):
    """Find an executable, first checking in the current path
    (as would be done by the shell), and then searching including
    any directories specified by additional_search_locations.
    If found, returns the full path to the executable. Otherwise,
    returns None.
    """
    for dirpath in (os.environ["PATH"].split(os.pathsep)+additional_search_locations):
        fpath = join(dirpath, exe_name)
        if isfile(fpath) and os.access(fpath, os.X_OK):
            return fpath
    return None

STANDARD_EXE_SEARCH_LOCATIONS=['/usr/bin', '/usr/local/bin',
                               abspath(expanduser('~/bin'))]

