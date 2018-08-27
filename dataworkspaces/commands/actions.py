"""
Actions are the steps of a command that change the state of a system.
A typical command is implemented by checking the environment to see what
needs to be done, and building up a list of actions to be run, called
a *plan*. We then confirm with the user that they want to run the actions
and execute them via run_plan()
"""

import os
from os.path import isfile, abspath, expanduser, join
import sys
import click
from subprocess import run, CalledProcessError, PIPE


############################################################################
#                           Error class definitions                        #
############################################################################
class ConfigurationError(Exception):
    pass


class UserAbort(Exception):
    pass

############################################################################
#                           Helper functions                               #
############################################################################


def call_subprocess(args, cwd, verbose=False):
    """Call an executable as a child process. If it fails, we will print
    an error and allow CalledProcessError to be thrown.
    """
    if verbose:
        click.echo(" ".join(args) + " [run in %s]" % cwd)
    cp = run(args, cwd=cwd, encoding='utf-8', stdout=PIPE, stderr=PIPE)
    cp.check_returncode()
    if verbose:
        click.echo(cp.stdout)

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

GIT_EXE_PATH=find_exe('git', additional_search_locations=STANDARD_EXE_SEARCH_LOCATIONS)

CURR_DIR = abspath(expanduser(os.curdir))


############################################################################
#                            Actions                                       #
############################################################################
# Individual action definitions

class Action:
    """Base class for all actions"""
    def __init__(self, verbose):
        self.verbose = verbose

    def precheck(self):
        pass

    def run(self):
        pass
    def __str__(self):
        return "This should explain what the actions will do"

class GitInit(Action):
    def __init__(self, base_directory, verbose):
        super().__init__(verbose)
        self.base_directory = base_directory

    def precheck(self):
        if GIT_EXE_PATH is None:
            raise ConfigurationError("git executable not found.")

    def run(self):
        call_subprocess([GIT_EXE_PATH, 'init'], self.base_directory, verbose=self.verbose)

    def __str__(self):
        return "Run 'git init' in %s to initialize a git repository" % self.base_directory



############################################################################
#               Action composition and execution                           #
############################################################################

def run_plan(plan, what, what_past_tense, batch=False, verbose=False, dry_run=False):
    if len(plan)==0:
        print("Nothing to do for %s" % what)
        return
    if not batch:
        print("Here are the planned actions to %s:" % what)
        for action in plan:
            print("  %s" % action)
        print()
    # if dry_run:
    #     print("--dry-run specified, exiting without doing anything")
    #     return 0
    if not batch:
        resp = input("Should I perform these actions? [y/n]")
    else:
        resp = 'y'
    if resp.lower()=='y':
        for (i, cmd) in enumerate(plan):
            print("%d. %s" % (i+1, cmd))
            cmd.run()
        print("Have now successfully %s" % what_past_tense)
    else:
        raise UserAbort()

