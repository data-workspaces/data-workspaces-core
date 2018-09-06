"""
Actions are the steps of a command that change the state of a system.
A typical command is implemented by checking the environment to see what
needs to be done, and building up a list of actions to be run, called
a *plan*. We then confirm with the user that they want to run the actions
and execute them via run_plan()
"""

import os
from os.path import isfile, abspath, expanduser, join, isabs, isdir, dirname
import sys
import click
from subprocess import run, PIPE

from dataworkspaces.errors import ConfigurationError, InternalError, UserAbort


############################################################################
#                           Helper functions                               #
############################################################################


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

def git_make_filelist_relative(base_directory, files_to_add):
    def mapfile(f):
        if isabs(f):
            if f.startswith(base_directory):
                return f[len(base_directory)+1:]
            else:
                raise InternalError("File '%s' is not within repository %s" %
                                    (f, base_directory))
        else:
            return f
    return [mapfile(f) for f in files_to_add]

class GitAdd(GitBaseAction):
    """Add a list of files to the pending commit.
    """
    def __init__(self, base_directory, files_to_add, verbose):
        super().__init__(base_directory, verbose)
        self.files_to_add = git_make_filelist_relative(base_directory,
                                                       files_to_add)

    def run(self):
        call_subprocess([GIT_EXE_PATH, 'add'] + self.files_to_add,
                        self.base_directory, verbose=self.verbose)

    def __str__(self):
        return "Add files to git repository at %s: %s" % \
            (self.base_directory, ', '.join(self.files_to_add))

class GitAddDeferred(GitBaseAction):
    """Add a list of files to the pending commit, where the list is
    determined by evaluating get_filelist_fn in run().
    """
    def __init__(self, base_directory, get_filelist_fn, verbose):
        super().__init__(base_directory, verbose)
        self.get_filelist_fn = get_filelist_fn

    def run(self):
        files_to_add = git_make_filelist_relative(self.base_directory,
                                                  self.get_filelist_fn())
        assert isinstance(files_to_add, list)
        call_subprocess([GIT_EXE_PATH, 'add'] + files_to_add,
                        self.base_directory, verbose=self.verbose)

    def __str__(self):
        return "Add files to git repository at %s" % \
            self.base_directory


class GitCommit(GitBaseAction):
    """Commit the current transaction. Message can be a string
    or a callable to be evaluated in run()"""
    def __init__(self, base_directory, message, verbose):
        super().__init__(base_directory, verbose)
        self.message = message

    def run(self):
        if callable(self.message):
            message = self.message()
        else:
            message = self.message
        call_subprocess([GIT_EXE_PATH, 'commit', '-m', message],
                        self.base_directory, verbose=self.verbose)

    def __str__(self):
        return "Git commit in %s" % self.base_directory


class GitHashObject(Action):
    """Run git hash-object on a file to compute its hash and
    store the result in self.hash_value.
    """
    def __init__(self, filepath, verbose):
        super().__init__(verbose)
        if GIT_EXE_PATH is None:
            raise ConfigurationError("git executable not found.")
        self.filepath = filepath
        self.hash_value = None

    def run(self):
        stdout = call_subprocess([GIT_EXE_PATH, 'hash-object', self.filepath],
                                 cwd=dirname(self.filepath), verbose=self.verbose)
        self.hash_value = stdout.strip()

    def __str__(self):
        return "Compute hash for file %s" % self.filepath



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

