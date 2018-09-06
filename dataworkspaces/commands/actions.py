"""
Actions are the steps of a command that change the state of a system.
A typical command is implemented by checking the environment to see what
needs to be done, and building up a list of actions to be run, called
a *plan*. We then confirm with the user that they want to run the actions
and execute them via run_plan()
"""

import os
from os.path import isfile, abspath, expanduser, join, isabs, isdir
import sys
import click
from subprocess import run, PIPE

from dataworkspaces.errors import ConfigurationError, InternalError, UserAbort


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

def is_git_repo(dirpath):
    if isdir(join(dirpath, '.git')):
        return True
    else:
        return False


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
        """Do any prechecks here...
        """
        self.verbose = verbose

    def run(self):
        pass
    def __str__(self):
        return "This should explain what the actions will do"

class GitBaseAction(Action):
    """Base class for git actions
    """
    def __init__(self, base_directory, verbose):
        super().__init__(verbose)
        self.base_directory = base_directory
        if GIT_EXE_PATH is None:
            raise ConfigurationError("git executable not found.")


class GitInit(GitBaseAction):
    def run(self):
        call_subprocess([GIT_EXE_PATH, 'init'], self.base_directory, verbose=self.verbose)

    def __str__(self):
        return "Run 'git init' in %s to initialize a git repository" % self.base_directory

class GitAdd(GitBaseAction):
    def __init__(self, base_directory, files_to_add, verbose):
        super().__init__(base_directory, verbose)
        self.files_to_add = []
        for file_to_add in files_to_add:
            if isabs(file_to_add):
                if not file_to_add.startswith(base_directory):
                    raise InternalError("git add file '%s' is not within repository %s" %
                                        (file_to_add, base_directory))
                else:
                    file_to_add = file_to_add[len(base_directory)+1:]
            self.files_to_add.append(file_to_add)

    def run(self):
        call_subprocess([GIT_EXE_PATH, 'add'] + self.files_to_add,
                        self.base_directory, verbose=self.verbose)

    def __str__(self):
        return "Add files to git repository at %s: %s" % \
            (self.base_directory, ', '.join(self.files_to_add))


class GitCommit(GitBaseAction):
    def __init__(self, base_directory, message, verbose):
        super().__init__(base_directory, verbose)
        self.message = message

    def run(self):
        call_subprocess([GIT_EXE_PATH, 'commit', '-m', self.message],
                        self.base_directory, verbose=self.verbose)

    def __str__(self):
        return "Git commit in %s" % self.base_directory



############################################################################
#               Action composition and execution                           #
############################################################################

def run_plan(plan, what, what_past_tense, batch=False, verbose=False, dry_run=False):
    if len(plan)==0:
        print("Nothing to do for %s" % what)
        return
    if verbose and (not batch):
        print("Here are the planned actions to %s:" % what)
        for action in plan:
            print("  %s" % action)
        print()
    # if dry_run:
    #     print("--dry-run specified, exiting without doing anything")
    #     return 0
    if verbose and (not batch):
        resp = input("Should I perform these actions? [y/n]")
    else:
        resp = 'y'
    if resp.lower()=='y':
        for (i, cmd) in enumerate(plan):
            if verbose:
                print("%d. %s" % (i+1, cmd))
            cmd.run()
        print("Have now successfully %s" % what_past_tense)
    else:
        raise UserAbort()

