"""
Utility functions related to interacting with git
"""
from os.path import isdir, join, dirname
from subprocess import run, PIPE
import re

import click

from .subprocess_utils import \
    find_exe, STANDARD_EXE_SEARCH_LOCATIONS, call_subprocess,\
    call_subprocess_for_rc
from .misc import remove_dir_if_empty
from dataworkspaces.errors import ConfigurationError, InternalError

def is_git_repo(dirpath):
    if isdir(join(dirpath, '.git')):
        return True
    else:
        return False


GIT_EXE_PATH=find_exe('git', additional_search_locations=STANDARD_EXE_SEARCH_LOCATIONS)
if GIT_EXE_PATH is None:
    raise ConfigurationError("Could not find a git executable!")

HASH_RE = re.compile(r'^[0-9a-fA-F]+$')

def is_a_git_hash(s):
    return len(s)==40 and (HASH_RE.match(s) is not None)


def is_git_dirty(cwd):
    """See if the git repo is dirty. We are looking for untracked
    files, changes in staging, and changes in the working directory.
    """
    if GIT_EXE_PATH is None:
        raise ConfigurationError("git executable not found")
    cmd = [GIT_EXE_PATH, 'status', '--porcelain']
    p = run(cmd, cwd=cwd, stdout=PIPE, encoding='utf-8')
    for line in p.stdout.split('\n'):
        if len(line)<2:
            continue
        if (line[0] in ('?', 'D', 'M', 'A'))  or (line[1] in ('?', 'D', 'M', 'A')):
            return True
    if p.returncode==0:
        return False
    else:
        raise ConfigurationError("Problem invoking %s status on %s" %
                                 (GIT_EXE_PATH, cwd))


def is_git_subdir_dirty(cwd, subdir):
    """See if the git repo is dirty. We are looking for untracked
    files, changes in staging, and changes in the working directory.
    """
    cmd = [GIT_EXE_PATH, 'status', '--porcelain', subdir]
    p = run(cmd, cwd=cwd, stdout=PIPE, encoding='utf-8')
    for line in p.stdout.split('\n'):
        if len(line)<2:
            continue
        if (line[0] in ('?', 'D', 'M', 'A'))  or (line[1] in ('?', 'D', 'M', 'A')):
            return True
    if p.returncode==0:
        return False
    else:
        raise ConfigurationError("Problem invoking %s status %s on %s" % (GIT_EXE_PATH, subdir, cwd))


def is_git_staging_dirty(cwd, subdir=None):
    """See if the git repo as uncommited changes in staging. If the
    subdirectory is specified, then we only look within that subdirectory
    """
    cmd = [GIT_EXE_PATH, 'status', '--porcelain']
    if subdir is not None:
        cmd.append(subdir)
    p = run(cmd, cwd=cwd, stdout=PIPE, encoding='utf-8')
    for line in p.stdout.split('\n'):
        if len(line)<2:
            continue
        if (line[0] in ('D', 'M', 'A')):
            return True
    if p.returncode==0:
        return False
    else:
        raise ConfigurationError("Problem invoking %s status on %s" % (GIT_EXE_PATH, cwd))

def commit_changes_in_repo(local_path, message, remove_empty_dirs=False,
                           verbose=False):
    """Figure out what has changed in the working tree relative to
    HEAD and get those changes into HEAD. We only commit if there
    is something to be done.
    """
    status = call_subprocess([GIT_EXE_PATH, 'status', '--porcelain'],
                                     cwd=local_path, verbose=verbose)
    maybe_delete_dirs = []
    need_to_commit = False
    for line in status.split('\n'):
        if len(line)<2:
            continue
        relpath = line[2:].strip()
        if line[1]=='?':
            call_subprocess([GIT_EXE_PATH, 'add', relpath],
                             cwd=local_path, verbose=verbose)
            need_to_commit = True
        elif line[1]=='D':
            call_subprocess([GIT_EXE_PATH, 'rm', relpath],
                            cwd=local_path, verbose=verbose)
            maybe_delete_dirs.append(dirname(join(local_path, relpath)))
            need_to_commit = True
        elif line[1]=='M':
            call_subprocess([GIT_EXE_PATH, 'add', relpath],
                            cwd=local_path, verbose=verbose)
            need_to_commit = True
        elif line[0] in ('?', 'A', 'D', 'M'):
            need_to_commit = True
            if line[0]=='D':
                maybe_delete_dirs.append(dirname(join(local_path, relpath)))
        elif verbose:
            click.echo("Skipping git status line: '%s'" % line)
    if remove_empty_dirs:
        for d in maybe_delete_dirs:
            remove_dir_if_empty(d, local_path, verbose=verbose)
    if need_to_commit:
        call_subprocess([GIT_EXE_PATH, 'commit', '-m', message],
                        cwd=local_path, verbose=verbose)


def checkout_and_apply_commit(local_path, commit_hash, verbose=False):
    """Checkout the commit and apply the changes to HEAD.
    """
    # make sure the repo is in a committed state
    commit_changes_in_repo(local_path,
                           "Commit state of repo prior to restore of %s" %
                           commit_hash,
                           verbose=verbose)
    # make sure there are actually differences between the commits
    if call_subprocess_for_rc([GIT_EXE_PATH, 'diff', '--exit-code', '--quiet',
                               'HEAD', commit_hash],
                              cwd=local_path, verbose=verbose)==0:
        if verbose:
            click.echo("No changes for %s between HEAD and %s" %
                       (local_path, commit_hash))
        return
    # ok, there are, apply the changes
    cmdstr = "%s diff HEAD %s | %s apply" % (GIT_EXE_PATH, commit_hash, GIT_EXE_PATH)
    if verbose:
        click.echo(cmdstr + "[run in %s]" % local_path)
    cp = run(cmdstr, cwd=local_path, shell=True)
    cp.check_returncode()
    commit_changes_in_repo(local_path, 'Revert to commit %s' % commit_hash,
                           remove_empty_dirs=True, verbose=verbose)


def commit_changes_in_repo_subdir(local_path, subdir, message,
                                  remove_empty_dirs=False, verbose=False):
    """For only the specified subdirectory, figure out what has changed in
    the working tree relative to HEAD and get those changes into HEAD. We
    only commit if there is something to be done.
    """
    if not subdir.endswith('/'):
        subdir = subdir + '/'
    status = call_subprocess([GIT_EXE_PATH, 'status', '--porcelain',
                                      subdir],
                                     cwd=local_path, verbose=verbose)
    maybe_delete_dirs = []
    need_to_commit = False
    for line in status.split('\n'):
        if len(line)<2:
            continue
        # first character is the staging area status, second character
        # is the working tree status, and rest is the relative path.
        relpath = line[2:].strip()
        if not relpath.startswith(subdir):
            raise InternalError("Git status line not in subdirectory %s: %s"%
                                (subdir, line))
        elif line[1]=='?':
            call_subprocess([GIT_EXE_PATH, 'add', relpath],
                            cwd=local_path, verbose=verbose)
            need_to_commit = True
        elif line[1]=='D':
            call_subprocess([GIT_EXE_PATH, 'rm', relpath],
                            cwd=local_path, verbose=verbose)
            maybe_delete_dirs.append(dirname(join(local_path, relpath)))
            need_to_commit = True
        elif line[1]=='M':
            call_subprocess([GIT_EXE_PATH, 'add', relpath],
                            cwd=local_path, verbose=verbose)
            need_to_commit = True
        elif line[0] in ('?', 'A', 'D', 'M'):
            need_to_commit = True
            if line[0]=='D':
                maybe_delete_dirs.append(dirname(join(local_path, relpath)))
        elif verbose:
            click.echo("Skipping git status line: '%s'" % line)
    if remove_empty_dirs:
        for d in maybe_delete_dirs:
            remove_dir_if_empty(d, join(local_path, subdir), verbose=verbose)
    if need_to_commit:
        call_subprocess([GIT_EXE_PATH, 'commit', '--only', subdir,
                         '-m', message],
                        cwd=local_path, verbose=verbose)


def checkout_subdir_and_apply_commit(local_path, subdir, commit_hash, verbose=False):
    """Checkout the commit and apply the changes to HEAD, just for a specific
    subdirectory in the repo.
    """
    commit_changes_in_repo_subdir(local_path, subdir,
                                  "Commit state of repo prior to restore of %s" %
                                  commit_hash,
                                  verbose=verbose)
    # make sure there are actually differences between the commits
    if call_subprocess_for_rc([GIT_EXE_PATH, 'diff', '--exit-code', '--quiet',
                               'HEAD', commit_hash, subdir],
                              cwd=local_path, verbose=verbose)==0:
        if verbose:
            click.echo("No changes for %s in %s between HEAD and %s" %
                       (local_path, subdir, commit_hash))
        return
    # ok, there are, apply the changes
    cmdstr = "%s diff HEAD %s %s | %s apply" % (GIT_EXE_PATH, commit_hash, subdir, GIT_EXE_PATH)
    if verbose:
        click.echo(cmdstr + "[run in %s]" % local_path)
    cp = run(cmdstr, cwd=local_path, shell=True)
    cp.check_returncode()
    commit_changes_in_repo_subdir(local_path, subdir,
                                  'Revert to commit %s' % commit_hash,
                                  remove_empty_dirs=True,
                                  verbose=verbose)


def is_file_tracked_by_git(filepath, cwd, verbose):
    cmd = [GIT_EXE_PATH, 'ls-files', '--error-unmatch', filepath]
    rc = call_subprocess_for_rc(cmd, cwd, verbose=verbose)
    return rc==0


def get_local_head_hash(git_root, verbose=False):
    hashval = call_subprocess([GIT_EXE_PATH, 'rev-parse', 'HEAD'],
                              cwd=git_root, verbose=verbose)
    return hashval.strip()

def get_remote_head_hash(cwd, branch, verbose):
    cmd = [GIT_EXE_PATH, 'ls-remote', 'origin', '-h', 'refs/heads/'+branch]
    try:
        output = call_subprocess(cmd, cwd, verbose).split('\n')[0].strip()
        if output=='':
            return None # remote has not commits
        else:
            hashval = output.split()[0]
            return hashval
    except Exception as e:
        raise ConfigurationError("Problem in accessing remote repository associated with '%s'" %
                                 cwd) from e
