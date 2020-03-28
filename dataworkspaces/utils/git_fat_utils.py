"""Utilities for git repos that use git-fat for support of large
files.
"""
from os.path import join, exists, isdir
from typing import Optional, cast

from dataworkspaces.errors import ConfigurationError
from dataworkspaces.utils.git_utils import is_git_repo, git_add, git_commit
from dataworkspaces.utils.subprocess_utils import find_exe
from dataworkspaces.utils.regexp_utils import RSYNC_RE, FPATH_RE, USERNAME_RE


def get_dot_gitfat_file_path(workspace_dir: str) -> str:
    return join(workspace_dir, ".gitfat")


def is_a_git_fat_repo(repo_dir: str) -> bool:
    assert is_git_repo(repo_dir), "%s is not a git repo" % repo_dir
    return exists(get_dot_gitfat_file_path(repo_dir))


def has_git_fat_been_initialized(repo_dir: str) -> bool:
    return isdir(join(repo_dir, ".git/fat"))


# Utility funtions for issue #12 - if a repo is git-fat enabled, and git-fat is not in the path,
# git add will fail silently for filter calls (e.g. in git add). We explicitly check that
# the executable is in the path in situations where we will call git as a subprocess.

GIT_FAT_ERRMSG = "Ensure that the dataworkspaces package is installed and that you have activated your virtual environment (if any)."


def validate_git_fat_in_path() -> None:
    """Validate that git-fat is in the path, asssuming we already know that this
    is a git-fat repo.
    If the executable is not found, throw a configuration error. We need to do this, as git itself
    will not return an error return code if a filter (e.g. git-fat) is not found.
    """
    find_exe("git-fat", GIT_FAT_ERRMSG, additional_search_locations=[])


def validate_git_fat_in_path_if_needed(repo_dir: str) -> None:
    """Validate that git-fat is in the path, if this repo is git-fat enabled.
    Otherwise, throw a configuration error. We need to do this, as git itself
    will not return an error return code if a filter (e.g. git-fat) is not found.
    """
    if not is_a_git_fat_repo(repo_dir):
        return
    find_exe("git-fat", GIT_FAT_ERRMSG, additional_search_locations=[])


def run_git_fat_pull_if_needed(repo_dir: str, verbose: bool) -> None:
    """When restoring resources from snapshots or running a pull, we need to
    also run git-fat pull, if the repo is a git-fat repo
    """
    if not is_a_git_fat_repo(repo_dir):
        return
    else:
        import dataworkspaces.third_party.git_fat as git_fat

        python2_exe = git_fat.find_python2_exe()
        git_fat.run_git_fat(python2_exe, ["pull"], cwd=repo_dir, verbose=verbose)


def run_git_fat_push_if_needed(repo_dir: str, verbose: bool) -> None:
    """Push the git-fat updates for the repo, if it is git fat-enabled
    """
    if not is_a_git_fat_repo(repo_dir):
        return
    else:
        import dataworkspaces.third_party.git_fat as git_fat

        python2_exe = git_fat.find_python2_exe()
        git_fat.run_git_fat(python2_exe, ["push"], cwd=repo_dir, verbose=verbose)


def setup_git_fat_for_repo(
    repo_dir: str,
    git_fat_remote: str,
    git_fat_user: Optional[str] = None,
    git_fat_port: Optional[int] = None,
    git_fat_attributes: Optional[str] = None,
    verbose: bool = False,
) -> None:
    """Setup git fat and all the associated configuration files
    for a repository
    """
    validate_git_fat_in_path()
    dot_git_fat_fpath = get_dot_gitfat_file_path(repo_dir)
    files_to_add = [
        ".gitfat",
    ]
    dot_git_attributes_fpath = None  # type: Optional[str]
    if git_fat_attributes:
        dot_git_attributes_fpath = join(repo_dir, ".gitattributes")
        files_to_add.append(".gitattributes")
    if (RSYNC_RE.match(git_fat_remote) is None) and (FPATH_RE.match(git_fat_remote) is None):
        raise ConfigurationError(
            (
                "'%s' is not a valid remote address for rsync (used by git-fat). "
                + "Please use the format HOSTNAME:/PATH"
            )
            % git_fat_remote
        )
    if git_fat_user is not None and USERNAME_RE.match(git_fat_user) is None:
        raise ConfigurationError("'%s' is not a valid remote username for git-fat" % git_fat_user)
    import dataworkspaces.third_party.git_fat as git_fat

    python2_exe = git_fat.find_python2_exe()
    # click.echo("Initializing git-fat with remote %s" % git_fat_remote)
    with open(dot_git_fat_fpath, "w") as f:
        f.write("[rsync]\nremote = %s\n" % git_fat_remote)
        if git_fat_user:
            f.write("sshuser = %s\n" % git_fat_user)
        if git_fat_port:
            f.write("sshport = %s\n" % git_fat_port)
    if git_fat_attributes is not None:
        with open(cast(str, dot_git_attributes_fpath), "w") as f:
            for extn in git_fat_attributes.split(","):
                f.write("%s filter=fat -crlf\n" % extn)
    git_fat.run_git_fat(python2_exe, ["init"], cwd=repo_dir, verbose=verbose)
    git_add(repo_dir, files_to_add, verbose)
    git_commit(repo_dir, "initialized git-fat with remote %s" % git_fat_remote, verbose)
