#!/usr/bin/env python3
"""
Python 3 wrapper over git-fat (which is python2-only)
"""
import os
import sys
from os.path import dirname, abspath, expanduser, isfile, join, exists, curdir
import subprocess
import click

from dataworkspaces.errors import ConfigurationError, InternalError

THIS_FILES_DIRPATH=dirname(abspath(expanduser(__file__)))

STANDARD_EXE_SEARCH_LOCATIONS=['/usr/bin', '/usr/local/bin',
                               abspath(expanduser('~/bin'))]


def _is_python2(exe_path, verbose=False):
    try:
        cp = subprocess.run([exe_path, '--version'], cwd=THIS_FILES_DIRPATH,
                            stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                            encoding='utf-8')
        cp.check_returncode()
        output = cp.stdout
    except Exception as e:
        if verbose:
            click.echo("Got an error when trying to run python at %s: %s"%
                       (exe_path, e))
        return False
    if output.startswith('Python 2.7') or output.startswith('Python 2.6'):
        return True
    else:
        return False

NOT_FOUND_ERRORMSG=\
"""Did not find a Python 2 executable, which is required to run git-fat.
Looked for executables in the directories %s.
Please make sure that you have Python 2.7 or Python 2.6 installed and
the conrtaining directory referenced on your PATH environment variable.
"""

def find_python2_exe():
    dirpaths = os.environ['PATH'].split(os.pathsep)
    for dirpath in STANDARD_EXE_SEARCH_LOCATIONS:
        if dirpath not in dirpaths:
            dirpaths.append(dirpath)

    for dirpath in dirpaths:
        for exe_name in ['python2.7', 'python2', 'python']:
            exe_path = join(dirpath, exe_name)
            if isfile(exe_path) and os.access(exe_path, os.X_OK):
                if _is_python2(exe_path):
                    return exe_path
    raise ConfigurationError(NOT_FOUND_ERRORMSG%', '.join(dirpaths))

def run_git_fat(python2_exe, args, cwd=curdir, verbose=False):
    fat_script = join(THIS_FILES_DIRPATH, 'git-fat')
    if not exists(fat_script):
        raise InternalError("Missing %s" % fat_script)
    cmd = [python2_exe, fat_script]+args
    if verbose:
        click.echo("%s from %s" % (' '.join(cmd), cwd))
        env = os.environ.copy()
        env['GIT_FAT_VERBOSE'] = "1"
    else:
        env = None
    cp = subprocess.run(cmd, cwd=cwd, env=env)
    try:
        cp.check_returncode()
    except Exception as e:
        raise InternalError("git-fat execution with args '%s' failed." %
                            ' '.join(args)) from e

def main(args=sys.argv[1:]):
    """Just run git-fat with the command line arguments.
    """
    python2=find_python2_exe()
    run_git_fat(python2, args)


if __name__=='__main__':
    sys.exit(main())
