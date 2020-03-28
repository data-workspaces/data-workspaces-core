"""Utilities for git repos that use git-lfs for support of large
files.
"""
import os
from os.path import join, exists
from typing import Optional
from pathlib import Path

import click

from dataworkspaces.utils.git_utils import is_git_repo, git_add, git_commit
from dataworkspaces.utils.subprocess_utils import find_exe, call_subprocess


def _does_attributes_file_reference_lfs(fpath) -> bool:
    with open(fpath, "r") as f:
        for line in f:
            words = line.split()
            if len(words) < 2:
                continue
            # start with index 1 to skip the pattern
            for attr in words[1:]:
                if attr.endswith("=lfs"):
                    return True
        return False


def is_a_git_lfs_repo(repo_dir: str, recursive: bool = True) -> bool:
    assert is_git_repo(repo_dir), "%s is not a git repo" % repo_dir
    if recursive:
        for (dirpath, filenames, dirnames) in os.walk(repo_dir):
            fname = join(dirpath, ".gitattributes")
            if exists(fname):
                if _does_attributes_file_reference_lfs(fname):
                    return True
        return False
    else:  # just check the root
        fname = join(repo_dir, ".gitattributes")
        if exists(fname):
            return _does_attributes_file_reference_lfs(fname)
        else:
            return False


def is_git_lfs_installed_for_user(home_dir: str = str(Path.home())) -> bool:
    git_config = join(home_dir, ".gitconfig")
    if not exists(git_config):
        return False
    with open(git_config, "r") as f:
        for line in f:
            if line.strip() == '[filter "lfs"]':
                return True
        return False


# Utility funtions for issue #12 - if a repo is git-lfs enabled, and git-lfs is not in the path,
# git add will fail silently for filter calls (e.g. in git add). We explicitly check that
# the executable is in the path in situations where we will call git as a subprocess.

GIT_LFS_ERRMSG = "git-lfs does not seem to be installed on your system. Install it or, if it is already installed, make sure that it is in your PATH."


def find_git_lfs_in_path() -> str:
    """Validate that git-lfs is in the path, asssuming we already know that this
    is a git-lfs repo.
    If the executable is not found, throw a configuration error. We need to do this, as git itself
    will not return an error return code if a filter (e.g. git-lfs) is not found.

    Retuns the path to the git-lfs executable.
    """
    return find_exe("git-lfs", GIT_LFS_ERRMSG, additional_search_locations=[])


def ensure_git_lfs_installed_for_user(lfs_exe, verbose: bool = False) -> bool:
    """Run the install operation if necessary. Returns true if it was necessary,
    false otherwise.
    """
    if not is_git_lfs_installed_for_user():
        click.echo("Git lfs not installed for your account, installing...")
        call_subprocess([lfs_exe, "install"], cwd=str(Path.home()), verbose=verbose)
        return True
    elif verbose:
        click.echo("git-lfs is installed for this user")
    return False


def init_git_lfs(
    workspace_dir: str, git_lfs_attributes: Optional[str] = None, verbose: bool = False
):
    """Called during the dws init if the repo has references to lfs in its
    .gitattributes files or if the user requested lfs file wildcares via
    git_lfs_attributes
    """
    lfs_exe = find_git_lfs_in_path()
    ensure_git_lfs_installed_for_user(lfs_exe, verbose=verbose)
    if git_lfs_attributes:
        for extn in git_lfs_attributes.split(","):
            call_subprocess([lfs_exe, "track", extn], cwd=workspace_dir, verbose=verbose)
        git_add(workspace_dir, [".gitattributes"], verbose=verbose)
        git_commit(workspace_dir, "added git-lfs attributes", verbose=verbose)


def ensure_git_lfs_configured_if_needed(repo_dir: str, verbose: bool = False) -> None:
    """If this repo uses git-lfs, then 1) validate that git-lfs is in the path,
    and 2) run git-lfs install for the user, if needed.
    If the repo uses git-lfs, but we cannot find the executable,
    throw a configuration error. We need to do this, as git itself
    will not return an error return code if a filter (e.g. git-lfs) is not found.
    """
    if not is_a_git_lfs_repo(repo_dir, recursive=True):
        return
    lfs_exe = find_exe("git-lfs", GIT_LFS_ERRMSG, additional_search_locations=[])
    need_to_download = ensure_git_lfs_installed_for_user(lfs_exe, verbose=verbose)
    if need_to_download:
        # If the user wasn't configured for git-lfs when cloning, we need to
        # explicitly download the files.
        call_subprocess([lfs_exe, "fetch"], cwd=repo_dir, verbose=verbose)
        call_subprocess([lfs_exe, "checkout"], cwd=repo_dir, verbose=verbose)
