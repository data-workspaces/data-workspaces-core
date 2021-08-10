# Copyright 2018,2019 by MPI-SWS and Data-ken Research. Licensed under Apache 2.0. See LICENSE.txt.
"""
Utility functions related to interacting with git
"""
from os.path import isdir, join, dirname, exists
from subprocess import run, PIPE
import shutil
import re
import tempfile
import json
from typing import Any, List

import click

from .subprocess_utils import find_exe, call_subprocess, call_subprocess_for_rc
from .file_utils import remove_dir_if_empty
from dataworkspaces.errors import ConfigurationError, InternalError, UserAbort


def is_git_repo(dirpath):
    if isdir(join(dirpath, ".git")):
        return True
    else:
        return False


GIT_EXE_PATH = find_exe("git", "Please make sure that you have git installed on your machine.")


def is_git_dirty(cwd):
    """See if the git repo is dirty. We are looking for untracked
    files, changes in staging, and changes in the working directory.
    """
    if GIT_EXE_PATH is None:
        raise ConfigurationError("git executable not found")
    cmd = [GIT_EXE_PATH, "status", "--porcelain"]
    p = run(cmd, cwd=cwd, stdout=PIPE, encoding="utf-8")
    for line in p.stdout.split("\n"):
        if len(line) < 2:
            continue
        if (line[0] in ("?", "D", "M", "A")) or (line[1] in ("?", "D", "M", "A")):
            return True
    if p.returncode == 0:
        return False
    else:
        raise ConfigurationError("Problem invoking %s status on %s" % (GIT_EXE_PATH, cwd))


def is_git_subdir_dirty(cwd, subdir):
    """See if the git repo is dirty. We are looking for untracked
    files, changes in staging, and changes in the working directory.
    """
    cmd = [GIT_EXE_PATH, "status", "--porcelain", subdir]
    p = run(cmd, cwd=cwd, stdout=PIPE, encoding="utf-8")
    for line in p.stdout.split("\n"):
        if len(line) < 2:
            continue
        if (line[0] in ("?", "D", "M", "A")) or (line[1] in ("?", "D", "M", "A")):
            return True
    if p.returncode == 0:
        return False
    else:
        raise ConfigurationError(
            "Problem invoking %s status %s on %s" % (GIT_EXE_PATH, subdir, cwd)
        )


def is_git_staging_dirty(cwd, subdir=None):
    """See if the git repo as uncommited changes in staging. If the
    subdirectory is specified, then we only look within that subdirectory
    """
    cmd = [GIT_EXE_PATH, "status", "--porcelain"]
    if subdir is not None:
        cmd.append(subdir)
    p = run(cmd, cwd=cwd, stdout=PIPE, encoding="utf-8")
    for line in p.stdout.split("\n"):
        if len(line) < 2:
            continue
        if line[0] in ("D", "M", "A"):
            return True
    if p.returncode == 0:
        return False
    else:
        raise ConfigurationError("Problem invoking %s status on %s" % (GIT_EXE_PATH, cwd))


def echo_git_status_for_user(cwd):
    """Run git status and echo to the user.
    """
    if GIT_EXE_PATH is None:
        raise ConfigurationError("git executable not found")
    cmd = [GIT_EXE_PATH, "status"]
    # p = run(cmd, cwd=cwd, stdout=PIPE, encoding="utf-8")
    p = run(cmd, cwd=cwd, encoding="utf-8")
    # for line in p.stdout.split("\n"):
    #     click.echo(line)
    if p.returncode != 0:
        raise ConfigurationError("Problem invoking %s status on %s" % (GIT_EXE_PATH, cwd))


def is_pull_needed_from_remote(cwd: str, branch: str, verbose: bool) -> bool:
    """Do check whether we need a pull, we get the hash of the HEAD
    of the remote's master branch. Then, we see if we have this object locally.
    """
    hashval = get_remote_head_hash(cwd, branch, verbose)
    if hashval is None:
        return False
    # cmd = [GIT_EXE_PATH, 'show', '--oneline', hashval]
    cmd = [GIT_EXE_PATH, "cat-file", "-e", hashval + "^{commit}"]
    rc = call_subprocess_for_rc(cmd, cwd, verbose=verbose)
    return rc != 0


def git_init(repo_dir, verbose=False):
    call_subprocess([GIT_EXE_PATH, "init"], cwd=repo_dir, verbose=verbose)


def git_add(repo_dir: str, relative_paths: List[str], verbose: bool = False) -> None:
    call_subprocess([GIT_EXE_PATH, "add"] + relative_paths, cwd=repo_dir, verbose=verbose)


def git_commit(repo_dir: str, message: str, verbose: bool = False) -> None:
    """Unconditional git commit
    """
    call_subprocess([GIT_EXE_PATH, "commit", "-m", message], cwd=repo_dir, verbose=verbose)


def get_branch_info(local_path, verbose=False):
    data = call_subprocess([GIT_EXE_PATH, "branch"], cwd=local_path, verbose=verbose)
    current = None
    other = []
    for line in data.split("\n"):
        line = line.strip()
        if len(line) == 0:
            continue
        if line.startswith("*"):
            assert current is None
            current = line[2:]
        else:
            other.append(line)
    if current is None:
        raise InternalError(
            "Problem obtaining branch information for local git repo at %s" % local_path
        )
    else:
        return (current, other)


def switch_git_branch(local_path, branch, verbose):
    try:
        call_subprocess([GIT_EXE_PATH, "checkout", branch], cwd=local_path, verbose=verbose)
    except Exception as e:
        raise ConfigurationError(
            "Unable to switch git repo at %s to branch %s" % (local_path, branch)
        ) from e


def switch_git_branch_if_needed(local_path, branch, verbose, ok_if_not_present=False):
    (current, others) = get_branch_info(local_path, verbose)
    if branch == current:
        return
    else:
        if (branch not in others) and (not ok_if_not_present):
            raise InternalError(
                "Trying to switch to branch %s not in repo at %s" % (branch, others)
            )
        switch_git_branch(local_path, branch, verbose)


def git_remove_subtree(
    repo_dir: str, relative_path: str, remove_history: bool = False, verbose: bool = False
) -> None:
    if remove_history:
        # removing history is problematic, as you need to --force the
        # next time you do a push. That also implies that you do a pull before
        # running the delete. See
        # https://help.github.com/en/articles/removing-sensitive-data-from-a-repository
        # for details.
        assert 0, "removing history not currently supported"
        if is_git_staging_dirty(repo_dir):
            # The history rewrite will fail if the repo is dirty, so
            # we will commit first.
            call_subprocess(
                [
                    GIT_EXE_PATH,
                    "commit",
                    "-m",
                    "commit before removing %s and its history" % relative_path,
                ],
                cwd=repo_dir,
                verbose=verbose,
            )
        call_subprocess(
            [
                GIT_EXE_PATH,
                "filter-branch",
                "--index-filter",
                "%s rm --cached --ignore-unmatch -rf %s" % (GIT_EXE_PATH, relative_path),
                "--prune-empty",
                "-f",
                "HEAD",
            ],
            cwd=repo_dir,
            verbose=verbose,
        )
    else:
        call_subprocess([GIT_EXE_PATH, "rm", "-rf", relative_path], cwd=repo_dir, verbose=verbose)


def git_remove_file(
    repo_dir: str, relative_path: str, remove_history: bool = False, verbose: bool = False
) -> None:
    if remove_history:
        # removing history is problematic, as you need to --force the
        # next time you do a push. That also implies that you do a pull before
        # running the delete. See
        # https://help.github.com/en/articles/removing-sensitive-data-from-a-repository
        # for details.
        assert 0, "removing history not currently supported"
        if is_git_staging_dirty(repo_dir):
            # The history rewrite will fail if the repo is dirty, so
            # we will commit first.
            call_subprocess(
                [
                    GIT_EXE_PATH,
                    "commit",
                    "-m",
                    "commit before removing %s and its history" % relative_path,
                ],
                cwd=repo_dir,
                verbose=verbose,
            )
        call_subprocess(
            [
                GIT_EXE_PATH,
                "filter-branch",
                "--index-filter",
                "%s rm --cached --ignore-unmatch  %s" % (GIT_EXE_PATH, relative_path),
                "--prune-empty",
                "-f",
                "HEAD",
            ],
            cwd=repo_dir,
            verbose=verbose,
        )
    else:
        call_subprocess([GIT_EXE_PATH, "rm", relative_path], cwd=repo_dir, verbose=verbose)


def commit_changes_in_repo(local_path, message, remove_empty_dirs=False, verbose=False):
    """Figure out what has changed in the working tree relative to
    HEAD and get those changes into HEAD. We only commit if there
    is something to be done.
    """
    status = call_subprocess(
        [GIT_EXE_PATH, "status", "--porcelain"], cwd=local_path, verbose=verbose
    )
    maybe_delete_dirs = []
    need_to_commit = False
    for line in status.split("\n"):
        if len(line) < 2:
            continue
        relpath = line[2:].strip()
        if line[1] == "?":
            call_subprocess([GIT_EXE_PATH, "add", relpath], cwd=local_path, verbose=verbose)
            need_to_commit = True
        elif line[1] == "D":
            call_subprocess([GIT_EXE_PATH, "rm", relpath], cwd=local_path, verbose=verbose)
            maybe_delete_dirs.append(dirname(join(local_path, relpath)))
            need_to_commit = True
        elif line[1] == "M":
            call_subprocess([GIT_EXE_PATH, "add", relpath], cwd=local_path, verbose=verbose)
            need_to_commit = True
        elif line[0] in ("?", "A", "D", "M"):
            need_to_commit = True
            if line[0] == "D":
                maybe_delete_dirs.append(dirname(join(local_path, relpath)))
        elif verbose:
            click.echo("Skipping git status line: '%s'" % line)
    if remove_empty_dirs:
        for d in maybe_delete_dirs:
            remove_dir_if_empty(d, local_path, verbose=verbose)
    if need_to_commit:
        call_subprocess([GIT_EXE_PATH, "commit", "-m", message], cwd=local_path, verbose=verbose)


def checkout_and_apply_commit(local_path, commit_hash, verbose=False):
    """Checkout the commit and apply the changes to HEAD.
    """
    # make sure the repo is in a committed state
    commit_changes_in_repo(
        local_path, "Commit state of repo prior to restore of %s" % commit_hash, verbose=verbose
    )
    # make sure there are actually differences between the commits
    if (
        call_subprocess_for_rc(
            [GIT_EXE_PATH, "diff", "--exit-code", "--quiet", "HEAD", commit_hash],
            cwd=local_path,
            verbose=verbose,
        )
        == 0
    ):
        if verbose:
            click.echo("No changes for %s between HEAD and %s" % (local_path, commit_hash))
        return
    # ok, there are, apply the changes
    cmdstr = "%s diff HEAD %s | %s apply" % (GIT_EXE_PATH, commit_hash, GIT_EXE_PATH)
    if verbose:
        click.echo(cmdstr + "[run in %s]" % local_path)
    cp = run(cmdstr, cwd=local_path, shell=True)
    cp.check_returncode()
    commit_changes_in_repo(
        local_path, "Revert to commit %s" % commit_hash, remove_empty_dirs=True, verbose=verbose
    )


def commit_changes_in_repo_subdir(
    local_path, subdir, message, remove_empty_dirs=False, verbose=False
):
    """For only the specified subdirectory, figure out what has changed in
    the working tree relative to HEAD and get those changes into HEAD. We
    only commit if there is something to be done.
    """
    if not subdir.endswith("/"):
        subdir = subdir + "/"
    status = call_subprocess(
        [GIT_EXE_PATH, "status", "--porcelain", subdir], cwd=local_path, verbose=verbose
    )
    maybe_delete_dirs = []
    need_to_commit = False
    for line in status.split("\n"):
        if len(line) < 2:
            continue
        # first character is the staging area status, second character
        # is the working tree status, and rest is the relative path.
        relpath = line[2:].strip()
        if not relpath.startswith(subdir):
            raise InternalError("Git status line not in subdirectory %s: %s" % (subdir, line))
        elif line[1] == "?":
            call_subprocess([GIT_EXE_PATH, "add", relpath], cwd=local_path, verbose=verbose)
            need_to_commit = True
        elif line[1] == "D":
            call_subprocess([GIT_EXE_PATH, "rm", relpath], cwd=local_path, verbose=verbose)
            maybe_delete_dirs.append(dirname(join(local_path, relpath)))
            need_to_commit = True
        elif line[1] == "M":
            call_subprocess([GIT_EXE_PATH, "add", relpath], cwd=local_path, verbose=verbose)
            need_to_commit = True
        elif line[0] in ("?", "A", "D", "M"):
            need_to_commit = True
            if line[0] == "D":
                maybe_delete_dirs.append(dirname(join(local_path, relpath)))
        elif verbose:
            click.echo("Skipping git status line: '%s'" % line)
    if remove_empty_dirs:
        for d in maybe_delete_dirs:
            remove_dir_if_empty(d, join(local_path, subdir), verbose=verbose)
    if need_to_commit:
        call_subprocess(
            [GIT_EXE_PATH, "commit", "--only", subdir, "-m", message],
            cwd=local_path,
            verbose=verbose,
        )


def checkout_subdir_and_apply_commit(local_path, subdir, commit_hash, verbose=False):
    """Checkout the commit and apply the changes to HEAD, just for a specific
    subdirectory in the repo.
    """
    commit_changes_in_repo_subdir(
        local_path,
        subdir,
        "Commit state of repo prior to restore of %s" % commit_hash,
        verbose=verbose,
    )
    # make sure there are actually differences between the commits
    if (
        call_subprocess_for_rc(
            [GIT_EXE_PATH, "diff", "--exit-code", "--quiet", "HEAD", commit_hash, subdir],
            cwd=local_path,
            verbose=verbose,
        )
        == 0
    ):
        if verbose:
            click.echo(
                "No changes for %s in %s between HEAD and %s" % (local_path, subdir, commit_hash)
            )
        return
    # ok, there are, apply the changes
    cmdstr = "%s diff HEAD %s %s | %s apply" % (GIT_EXE_PATH, commit_hash, subdir, GIT_EXE_PATH)
    if verbose:
        click.echo(cmdstr + "[run in %s]" % local_path)
    cp = run(cmdstr, cwd=local_path, shell=True)
    cp.check_returncode()
    commit_changes_in_repo_subdir(
        local_path,
        subdir,
        "Revert to commit %s" % commit_hash,
        remove_empty_dirs=True,
        verbose=verbose,
    )


def is_file_tracked_by_git(filepath, cwd, verbose):
    cmd = [GIT_EXE_PATH, "ls-files", "--error-unmatch", filepath]
    rc = call_subprocess_for_rc(cmd, cwd, verbose=verbose)
    return rc == 0


def get_local_head_hash(git_root, verbose=False):
    hashval = call_subprocess([GIT_EXE_PATH, "rev-parse", "HEAD"], cwd=git_root, verbose=verbose)
    return hashval.strip()


def get_remote_head_hash(cwd, branch, verbose):
    cmd = [GIT_EXE_PATH, "ls-remote", "origin", "-h", "refs/heads/" + branch]
    try:
        output = call_subprocess(cmd, cwd, verbose).split("\n")[0].strip()
        if output == "":
            return None  # remote has not commits
        else:
            hashval = output.split()[0]
            return hashval
    except Exception as e:
        raise ConfigurationError(
            "Problem in accessing remote repository associated with '%s'" % cwd
        ) from e


LS_TREE_RE = re.compile(r"^\d+\s+tree\s([0-9a-f]{40})\s+(\w+.*)$")


def get_subdirectory_hash(repo_dir, relpath, verbose=False):
    """Get the subdirectory hash for the HEAD revision of the
    specified path. This matches the hash that git is storing
    internally. You should be able to run: git cat-file -p HASH
    to see a listing of the contents.
    """
    cmd = [GIT_EXE_PATH, "ls-tree", "-t", "HEAD", relpath]
    if verbose:
        click.echo("%s [run in %s]" % (" ".join(cmd), repo_dir))
    cp = run(cmd, cwd=repo_dir, encoding="utf-8", stdout=PIPE, stderr=PIPE)
    cp.check_returncode()
    for line in cp.stdout.split("\n"):
        m = LS_TREE_RE.match(line)
        if m is None:
            continue
        hashval = m.group(1)
        subdir = m.group(2)
        if subdir == relpath:
            return hashval
    raise InternalError("Did not find subdirectory '%s' in git ls-tree" % relpath)


def get_remote_origin_url(repo_dir: str, verbose: bool) -> str:
    try:
        url = call_subprocess(
            [GIT_EXE_PATH, "config", "--get", "remote.origin.url"], cwd=repo_dir, verbose=verbose
        )
        return url.strip()
    except Exception as e:
        raise ConfigurationError(
            "Problem getting remote origin from repository at %s. Do you have a remote origin configured?"
            % repo_dir
        ) from e


def get_json_file_from_remote(relpath: str, repo_dir: str, verbose: bool) -> Any:
    """Download a JSON file from the remote master, parse it,
    and return it.
    """
    remote_url = get_remote_origin_url(repo_dir, verbose)
    tdir = None
    try:
        with tempfile.TemporaryDirectory() as tdir:
            # Issue #30 - we wanted to use the git-archive command,
            # but it is not supported by GitHub.
            call_subprocess(
                [GIT_EXE_PATH, "clone", "--depth=1", remote_url, "root"], cwd=tdir, verbose=verbose
            )
            with open(join(join(tdir, "root"), relpath), "r") as f:
                return json.load(f)
    except Exception as e:
        if (tdir is not None) and isdir(tdir):
            shutil.rmtree(tdir)
        raise ConfigurationError("Problem retrieving file %s from remote" % relpath) from e


def set_remote_origin(repo_dir, remote_url, verbose):
    call_subprocess(
        [GIT_EXE_PATH, "remote", "add", "origin", remote_url], cwd=repo_dir, verbose=verbose
    )


def get_git_config_param(repo_dir, param_name, verbose):
    param_val = call_subprocess([GIT_EXE_PATH, "config", param_name], cwd=repo_dir, verbose=verbose)
    return param_val.strip()


def ensure_entry_in_gitignore(
    repo_dir: str,
    gitignore_rel_path: str,
    entry: str,
    match_independent_of_slashes=False,
    commit: bool = False,
    verbose=False,
) -> bool:
    """Ensure that the specified entry is in the specified .gitignore file.

    Entries can have a leading slash (refers to an absolute path within the repo)
    and a trailing slash (matches only a directory, not a file).
    If match_independent_of_slashes is True, we match an existing
    entry, even if it differs on leading and/or trailing slashes. Otherwise,
    it must be an exact match.

    If a change was made, and commit is specified, commit the change. Otherwise,
    just add the file to the staging.

    Returns True if a change was made, False otherwise.
    """

    def strip_slashes(e):
        if len(e) == 0:
            return ""
        if e.startswith("/"):
            e = e[1:]
        if e.endswith("/"):
            e = e[:-1]
        assert len(e) > 0
        return e

    entry_wo_slashes = strip_slashes(entry)
    abs_file_path = join(repo_dir, gitignore_rel_path)
    if exists(abs_file_path):
        last_has_newline = True
        with open(abs_file_path, "r") as f:
            for line in f:
                if line.endswith("\n"):
                    last_has_newline = True
                else:
                    last_has_newline = False
                line = line.rstrip()
                if line == entry or (
                    match_independent_of_slashes and strip_slashes(line) == entry_wo_slashes
                ):
                    return False  # entry already present, nothing to do
        with open(abs_file_path, "a") as f:
            if not last_has_newline:
                f.write("\n")
            f.write(entry + "\n")
    else:
        with open(abs_file_path, "a") as f:
            f.write(entry + "\n")
    call_subprocess([GIT_EXE_PATH, "add", gitignore_rel_path], cwd=repo_dir, verbose=verbose)
    if commit:
        call_subprocess(
            [GIT_EXE_PATH, "commit", "-m", "Add .gitignore entry for %s" % entry],
            cwd=repo_dir,
            verbose=verbose,
        )
    return True


def verify_git_config_initialized(cwd: str, batch: bool = False, verbose: bool = False):
    """When trying to clone or initialize a new git repo, git requires
    that user.name and user.email are set. If not, it will error
    out. We verify that they are set. If not, and in interactive
    mode, we can ask the user and set them. If not, and in batch mode,
    we will error out explaining the issue.
    """
    rc = call_subprocess_for_rc([GIT_EXE_PATH, "config", "user.name"], cwd=cwd, verbose=verbose)
    name_set = True if rc == 0 else False
    rc = call_subprocess_for_rc([GIT_EXE_PATH, "config", "user.email"], cwd=cwd, verbose=verbose)
    email_set = True if rc == 0 else False
    if name_set and email_set:
        if verbose:
            click.echo("Successfully verified that git user.name and user.email are configured.")
        return
    need_to_set = (["user.name"] if not name_set else []) + (
        ["user.email"] if not email_set else []
    )
    if batch:
        raise ConfigurationError(
            "Git is not fully configured, need to set the Git config parameter%s %s before running."
            % ("s" if len(need_to_set) == 2 else "", " and ".join(need_to_set))
        )
    # if we get here, we're going to interactively ask the user for the git config parameters
    for param in need_to_set:
        click.echo(
            "Git is not fully configured - you need to set the Git config parameter '%s'." % param
        )
        value = click.prompt("Please enter a value for %s or return to abort" % param, default="")
        if value == "":
            raise UserAbort("Need to configure git parameter %s" % param)
        call_subprocess(
            [GIT_EXE_PATH, "config", "--global", param, value], cwd=cwd, verbose=verbose
        )
        click.echo("Successfully set Git parameter %s" % param)
