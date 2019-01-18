# Copyright 2018,2019 by MPI-SWS and Data-ken Research. Licensed under Apache 2.0. See LICENSE.txt.
"""
Actions are the steps of a command that change the state of a system.
A typical command is implemented by checking the environment to see what
needs to be done, and building up a list of actions to be run, called
a *plan*. We then confirm with the user that they want to run the actions
and execute them via run_plan()
"""

import os
from os.path import isfile, abspath, expanduser, join, isabs, isdir, dirname, exists
import click
from subprocess import run, PIPE
import re
from tempfile import NamedTemporaryFile

from dataworkspaces.errors import ConfigurationError, InternalError, UserAbort


############################################################################
#                           Helper functions                               #
############################################################################

# For backward compatibility. TODO: replace with direct calls
from dataworkspaces.utils.subprocess_utils import \
    call_subprocess, call_subprocess_for_rc, find_exe

from dataworkspaces.utils.git_utils import \
    is_git_repo, GIT_EXE_PATH, HASH_RE

CURR_DIR = abspath(expanduser(os.curdir))


def write_and_hash_file(write_fn, filename_template, verbose):
    """Write a file to a temporary location, take its hash
    and rename to filename_template, replacing <HASHVAL> with
    the hash. Returns hashval, the snapshot filename, and
    a boolean indicating whether this is a new snapshot
    (may end up with a snapshot matching a previous one).
    """
    with NamedTemporaryFile(delete=False) as f:
        tfilename = f.name
    try:
        write_fn(tfilename)
        stdout = call_subprocess([GIT_EXE_PATH, 'hash-object', tfilename],
                                 cwd=dirname(tfilename), verbose=verbose)
        hashval = stdout.strip()
        target_name = filename_template.replace("<HASHVAL>", hashval)
        assert target_name!=filename_template
        if exists(target_name):
            return (hashval, target_name, False)
        else:
            os.rename(tfilename, target_name)
            return (hashval, target_name, True)
    finally:
        if exists(tfilename):
            os.remove(tfilename)


############################################################################
#                         Action Base Classes                              #
############################################################################

class Promise:
    """Used to promise a namespace value that will be populated during the run phase. """
    def __init__(self, expected_type, action_name):
        self.expected_type = expected_type
        self.action_name = action_name

    def __str__(self):
        return "Promise(%s to be provided by action '%s')" % (self.expected_type, self.action_name)

class Namespace(dict):
    """Shared values between actions, accessible as object attributes.
    If a value won't be provided until the run() phase, use make_promise().
    Each value can only be written once.
    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def __getattr__(self, name):
        if name in self:
            v = self[name]
            return v
            # if isinstance(v, Promise):
            #     raise AttributeError("Attribute '%s' is still a promise from '%s' for a %s" % (name, v.action_name, v.expected_type))
            # else:
            #     return v
        else:
            raise AttributeError("No such attribute: " + name)

    def __setattr__(self, name, value):
        if name in self:
            old = self[name]
            if isinstance(old, Promise):
                if isinstance(value, old.expected_type):
                    self[name] = value
                else:
                    raise AttributeError("Attribute '%s' is given value '%s' of type %s, inconsistent with promised type %s"%
                                         (name, value, type(value), old.expected_type))
            else:
                raise AttributeError("Attempt to overwrite attribute '%s'. Old value: %s, new value: %s" %
                                     (name, old, value))
        else:
            self[name] = value

    def make_promise(self, key, expected_type, action_name):
        self.__setattr__(key, Promise(expected_type, action_name))

    def verify_promise(self, name, expected_type):
        """Verify that we have a promise (or the exact value)for the key with the expected type"""
        if name in self:
            v = self[name]
            if isinstance(v, Promise):
                if not issubclass(expected_type, v.expected_type):
                    raise AttributeError("Did not find a promise or value for a %s in key '%s', promised type is %s"%
                                         (expected_type, name, v.expected_type))
            elif not isinstance(v, expected_type):
                raise AttributeError("Did not find a promise or value for a %s in key '%s', instead found value '%s'"%
                                     (expected_type, name, v))
        else:
            raise AttributeError("Did not find a promise or value for a %s in key '%s'" %
                                 (expected_type, name))
class NamespaceRef:
    """A reference to a namespace property. Will evaluate
    that the namespace contains at least a promise to the property
    at constuction time. Calling the namespace ref yields the actual value from
    the namespace.
    """
    def __init__(self, property_name, expected_type, ns):
        self.property_name = property_name
        self.expected_type = expected_type
        self.ns = ns
        ns.verify_promise(property_name, expected_type)

    def __call__(self):
        return self.ns[self.property_name]

    def __str__(self):
        return "Value stored in namespace property %s" % self.xproperty_name

    def __repr__(self):
        return "NamespaceRef(%s)" % self.property_name

def wrap_value(v):
    """If callable, return itself. Otherwise wrap in a
    callable."""
    if callable(v):
        return v
    else:
        return lambda:v

def requires_from_ns(property_name, expected_type):
    """Decorator for an action's __init__ method to declare
    a property requirement.
    """
    def _requires(init_method):
        def wrapper(self, ns, *args, **kwargs):
            ns.verify_promise(property_name, expected_type)
            return init_method(self, ns, *args, **kwargs)
        return wrapper
    return _requires

def provides_to_ns(property_name, expected_type):
    """Decorator for an action's __init__ method to declare
    a property promise.
    """
    def _provides(init_method):
        def wrapper(self, ns, *args, **kwargs):
            ns.make_promise(property_name, expected_type, self.__class__.__name__)
            return init_method(self, ns, *args, **kwargs)
        return wrapper
    return _provides

class Action:
    """Base class for all actions"""
    def __init__(self, ns, verbose):
        """ns is a Namespace object for sharing values between actions.
        Do any prechecks here...
        """
        self.ns = ns
        self.verbose = verbose

    def run(self):
        pass
    def __str__(self):
        return "This should explain what the actions will do"


############################################################################
#                            Actions                                       #
############################################################################
# Individual action definitions

class GitBaseAction(Action):
    """Base class for git actions
    """
    def __init__(self, ns, verbose, base_directory):
        super().__init__(ns, verbose)
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
    """Add a list of files to the pending commit. files_to_add can
    be a list or a NamespaceRef
    """
    def __init__(self, ns, verbose, base_directory, files_to_add):
        super().__init__(ns, verbose, base_directory)
        self.files_to_add = wrap_value(files_to_add)

    def run(self):
        files_to_add_relative = \
            git_make_filelist_relative(self.base_directory,
                                       self.files_to_add())
        call_subprocess([GIT_EXE_PATH, 'add'] + files_to_add_relative,
                        self.base_directory, verbose=self.verbose)

    def __str__(self):
        filelist = ', '.join(self.files_to_add) if not callable(self.files_to_add) \
                   else self.files_to_add()
        return "Add files to git repository at %s: %s" % \
            (self.base_directory, filelist)

class GitCommit(GitBaseAction):
    """Commit the current transaction. The commit message
    can be a string or a namespace reference"""
    def __init__(self, ns, verbose, base_directory, commit_message):
        super().__init__(ns, verbose, base_directory)
        self.commit_message = wrap_value(commit_message)

    def run(self):
        message = self.commit_message()
        call_subprocess([GIT_EXE_PATH, 'commit', '-m', message],
                        self.base_directory, verbose=self.verbose)

    def __str__(self):
        return "Git commit in %s" % self.base_directory


class GitHashObject(Action):
    """Run git hash-object on a file to compute its hash and
    store the result in self.hash_value.
    """
    def __init__(self, ns, verbose, filepath):
        super().__init__(ns, verbose)
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
        for (i, action) in enumerate(plan):
            print("  %d. %s" % (i+1, action))
        print()
    # if dry_run:
    #     print("--dry-run specified, exiting without doing anything")
    #     return 0
    if verbose and (not batch):
        resp = input("Should I perform these actions? [Y/n]")
    else:
        resp = 'y'
    if resp.lower()=='y' or resp=='':
        for (i, action) in enumerate(plan):
            if verbose:
                print("%d. %s" % (i+1, action))
            action.run()
        print("Have now successfully %s" % what_past_tense)
    else:
        raise UserAbort()

