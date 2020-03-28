# Copyright 2018,2019 by MPI-SWS and Data-ken Research. Licensed under Apache 2.0. See LICENSE.txt.
"""
Resource for git repositories
"""
import subprocess
import os
from os.path import (
    realpath,
    basename,
    isdir,
    join,
    dirname,
    exists,
    abspath,
    expanduser,
    commonpath,
    isabs,
)
import shutil
import stat
import click
import json
from typing import Set, Pattern, Union, Optional, Tuple

from dataworkspaces.errors import ConfigurationError, InternalError
from dataworkspaces.utils.subprocess_utils import call_subprocess, call_subprocess_for_rc
from dataworkspaces.utils.git_utils import (
    is_git_dirty,
    is_file_tracked_by_git,
    get_local_head_hash,
    commit_changes_in_repo,
    checkout_and_apply_commit,
    GIT_EXE_PATH,
    is_git_repo,
    commit_changes_in_repo_subdir,
    checkout_subdir_and_apply_commit,
    get_subdirectory_hash,
    is_pull_needed_from_remote,
    git_remove_subtree,
    git_commit,
    is_git_staging_dirty,
)
from dataworkspaces.utils.git_fat_utils import (
    is_a_git_fat_repo,
    has_git_fat_been_initialized,
    validate_git_fat_in_path,
    validate_git_fat_in_path_if_needed,
)
from dataworkspaces.utils.git_lfs_utils import ensure_git_lfs_configured_if_needed
from dataworkspaces.workspace import (
    Resource,
    ResourceFactory,
    ResourceRoles,
    RESOURCE_ROLE_PURPOSES,
    LocalStateResourceMixin,
    FileResourceMixin,
    SnapshotResourceMixin,
    JSONDict,
    JSONList,
    Workspace,
)
import dataworkspaces.backends.git as git_backend
from dataworkspaces.utils.file_utils import (
    LocalPathType,
    does_subpath_exist,
    get_subpath_from_absolute,
)
from dataworkspaces.utils.param_utils import BoolType, AbspathType, StringType, RelpathType

from dataworkspaces.utils.snapshot_utils import move_current_files_local_fs


def git_move_and_add(srcabspath, destabspath, git_root, verbose):
    """
    Move a file that might or might not be tracked by git to
    a new location (snapshot directory), set it to read-only and make sure
    that it is now tracked by git.
    """
    assert srcabspath.startswith(git_root)
    assert destabspath.startswith(git_root)
    srcrelpath = srcabspath[len(git_root) + 1 :]
    destrelpath = destabspath[len(git_root) + 1 :]
    if is_file_tracked_by_git(srcrelpath, git_root, verbose):
        call_subprocess(
            [GIT_EXE_PATH, "mv", srcrelpath, destrelpath], cwd=git_root, verbose=verbose
        )
    else:
        # file is not tracked by git yet, just move and then add to git
        os.rename(join(git_root, srcrelpath), join(git_root, destrelpath))
    # either way, we change the permissions and then do an add at the end
    mode = os.stat(destabspath)[stat.ST_MODE]
    os.chmod(destabspath, mode & ~stat.S_IWUSR & ~stat.S_IWGRP & ~stat.S_IWOTH)
    call_subprocess([GIT_EXE_PATH, "add", destrelpath], cwd=git_root, verbose=verbose)


class GitResourceBase(Resource, LocalStateResourceMixin, FileResourceMixin, SnapshotResourceMixin):
    def __init__(
        self,
        resource_type: str,
        name: str,
        role: str,
        workspace: Workspace,
        local_path: str,
        export: bool,
        repo_dir: str,
    ):
        super().__init__(resource_type, name, role, workspace)

        # define and validate the parameters
        self.param_defs.define(
            "export",
            default_value=False,
            optional=True,
            help="True if metadata for export should be added each snapshot",
            is_global=True,
            ptype=BoolType(),
        )
        self.export = self.param_defs.get("export", export)  # type: bool
        self.param_defs.define(
            "local_path",
            default_value=None,
            optional=False,
            help="Always points to the root of this resource "
            + "(not necessarily the root of the repo)",
            is_global=False,
            ptype=AbspathType(),
        )
        self.local_path = self.param_defs.get("local_path", local_path)  # type: str

        self.repo_dir = repo_dir  # The root of the repo.

    def get_local_path_if_any(self):
        return self.local_path

    def validate_subpath_exists(self, subpath: str) -> None:
        super().validate_subpath_exists(subpath)

    def delete_snapshot(
        self, workspace_snapshot_hash: str, resource_restore_hash: str, relative_path: str
    ) -> None:
        snapshot_dir_path = join(self.local_path, relative_path)
        if isdir(snapshot_dir_path):
            if self.workspace.verbose:
                print(
                    "Deleting snapshot directory %s from resource %s" % (relative_path, self.name)
                )
            subpath_relative_to_repo = get_subpath_from_absolute(self.repo_dir, snapshot_dir_path)
            assert subpath_relative_to_repo is not None
            git_remove_subtree(
                self.repo_dir, subpath_relative_to_repo, verbose=self.workspace.verbose
            )
            git_commit(
                self.repo_dir, "Deleted %s" % snapshot_dir_path, verbose=self.workspace.verbose
            )

    def does_subpath_exist(
        self, subpath: str, must_be_file: bool = False, must_be_directory: bool = False
    ) -> bool:
        return does_subpath_exist(self.local_path, subpath, must_be_file, must_be_directory)

    def upload_file(self, src_local_path: str, rel_dest_path: str) -> None:
        """Copy a local file to the specified path in the
        resource. This may be a local copy or an upload, depending
        on the resource implmentation
        """
        abs_dest_path = join(self.local_path, rel_dest_path)
        parent_dir = dirname(abs_dest_path)
        if not exists(src_local_path):
            raise ConfigurationError("Source file %s does not exist" % src_local_path)
        if not exists(parent_dir):
            os.makedirs(parent_dir)
        shutil.copyfile(src_local_path, abs_dest_path)
        rel_to_repo_path = get_subpath_from_absolute(self.repo_dir, abs_dest_path)
        assert rel_to_repo_path is not None
        call_subprocess(
            [GIT_EXE_PATH, "add", rel_to_repo_path],
            cwd=self.repo_dir,
            verbose=self.workspace.verbose,
        )
        if is_git_staging_dirty(self.repo_dir, rel_to_repo_path):
            call_subprocess(
                [GIT_EXE_PATH, "commit", "-m", "Added %s" % rel_to_repo_path],
                cwd=self.repo_dir,
                verbose=self.workspace.verbose,
            )
        if self.workspace.verbose:
            click.echo("%s: Copied file to %s" % (self.name, rel_dest_path))

    def read_results_file(self, subpath: str) -> JSONDict:
        """Read and parse json results data from the specified path
        in the resource. If the path does not exist or is not a file
        throw a ConfigurationError.
        """
        path = os.path.join(self.local_path, subpath)
        if not os.path.isfile(path):
            raise ConfigurationError(
                "subpath %s does not exist or is not a file in resource %s" % (subpath, self.name)
            )
        with open(path, "r") as f:
            try:
                return json.load(f)
            except Exception as e:
                raise ConfigurationError(
                    "Parse error when reading %s in resource %s" % (subpath, self.name)
                ) from e


def get_workspace_dir(workspace: Workspace) -> str:
    workspace_local_path = workspace.get_workspace_local_path_if_any()
    return workspace_local_path if workspace_local_path is not None else abspath(expanduser("~"))


class GitRepoResource(GitResourceBase):
    def __init__(
        self,
        name: str,
        role: str,
        workspace: Workspace,
        remote_origin_url: str,
        relative_local_path: Optional[str],
        local_path: str,
        branch: str,
        read_only: bool,
        export: bool,
    ):
        super().__init__(
            "git", name, role, workspace, local_path, export=export, repo_dir=local_path
        )

        # handle parameters
        self.param_defs.define(
            "remote_origin_url",
            default_value=None,
            optional=False,
            help="URL of the remote git repo",
            is_global=True,
            ptype=StringType(),
        )
        self.remote_origin_url = self.param_defs.get(
            "remote_origin_url", remote_origin_url
        )  # type: str
        self.param_defs.define(
            "relative_local_path",
            default_value=None,
            optional=True,
            help="Local path of repo relative to the workspace",
            is_global=True,
            ptype=RelpathType(),
        )
        self.relative_local_path = self.param_defs.get(
            "relative_local_path", relative_local_path
        )  # type: Optional[str]
        self.param_defs.define(
            "branch",
            default_value=None,
            optional=False,
            help="Git branch to use",
            is_global=True,
            ptype=StringType(),
        )
        self.branch = self.param_defs.get("branch", branch)  # type: str
        self.param_defs.define(
            "read_only",
            default_value=False,
            optional=True,
            help="If True, than no pushes are done for this repo",
            is_global=True,
            ptype=BoolType(),
        )
        self.read_only = self.param_defs.get("read_only", read_only)  # type: bool

    def get_local_params(self):
        use_relative = (
            True
            if self.relative_local_path
            and join(get_workspace_dir(self.workspace), self.relative_local_path) == self.local_path
            else False
        )
        params = super().get_local_params()
        params["local_path"] = self.relative_local_path if use_relative else self.local_path
        return params

    def results_move_current_files(
        self, rel_dest_root: str, exclude_files: Set[str], exclude_dirs_re: Pattern
    ):
        switch_git_branch_if_needed(self.local_path, self.branch, self.workspace.verbose)
        validate_git_fat_in_path_if_needed(self.local_path)
        moved_files = move_current_files_local_fs(
            self.name,
            self.local_path,
            rel_dest_root,
            exclude_files,
            exclude_dirs_re,
            move_fn=lambda src, dest: git_move_and_add(
                src, dest, self.local_path, self.workspace.verbose
            ),
            verbose=self.workspace.verbose,
        )
        # If there were no files in the results dir, then we do not
        # create a subdirectory for this snapshot
        if len(moved_files) > 0:
            call_subprocess(
                [GIT_EXE_PATH, "commit", "-m", "Move current results to %s" % rel_dest_root],
                cwd=self.local_path,
                verbose=self.workspace.verbose,
            )

    def add_results_file(self, data: Union[JSONDict, JSONList], rel_dest_path: str) -> None:
        """Save JSON results data to the specified path in the resource.
        """
        assert self.role == ResourceRoles.RESULTS
        switch_git_branch_if_needed(self.local_path, self.branch, self.workspace.verbose)
        abs_dest_path = join(self.local_path, rel_dest_path)
        parent_dir = dirname(abs_dest_path)
        if not exists(parent_dir):
            os.makedirs(parent_dir)
        with open(abs_dest_path, "w") as f:
            json.dump(data, f, indent=2)
        call_subprocess(
            [GIT_EXE_PATH, "add", rel_dest_path],
            cwd=self.local_path,
            verbose=self.workspace.verbose,
        )
        call_subprocess(
            [GIT_EXE_PATH, "commit", "-m", "Added %s" % rel_dest_path],
            cwd=self.local_path,
            verbose=self.workspace.verbose,
        )

    def snapshot_precheck(self):
        validate_git_fat_in_path_if_needed(self.local_path)

    def snapshot(self):
        # Todo: handle tags
        commit_changes_in_repo(
            self.local_path, "autocommit ahead of snapshot", verbose=self.workspace.verbose
        )
        switch_git_branch_if_needed(self.local_path, self.branch, self.workspace.verbose)
        hashval = get_local_head_hash(self.local_path, self.workspace.verbose)
        return (hashval, hashval)

    def restore_precheck(self, hashval):
        rc = call_subprocess_for_rc(
            [GIT_EXE_PATH, "cat-file", "-e", hashval + "^{commit}"],
            cwd=self.local_path,
            verbose=self.workspace.verbose,
        )
        if rc != 0:
            raise ConfigurationError("No commit found with hash '%s' in %s" % (hashval, str(self)))
        if is_a_git_fat_repo(self.local_path):
            import dataworkspaces.third_party.git_fat as git_fat

            self.python2_exe = git_fat.find_python2_exe()
            self.uses_git_fat = True
            validate_git_fat_in_path()
        else:
            self.uses_git_fat = False

    def restore(self, hashval):
        commit_changes_in_repo(
            self.local_path, "auto-commit ahead of restore", verbose=self.workspace.verbose
        )
        switch_git_branch_if_needed(self.local_path, self.branch, self.workspace.verbose)
        checkout_and_apply_commit(self.local_path, hashval, verbose=self.workspace.verbose)
        if self.uses_git_fat:
            # since the restored repo might have different git-fat managed files, we run
            # a pull to get them.
            import dataworkspaces.third_party.git_fat as git_fat

            git_fat.run_git_fat(
                self.python2_exe, ["pull"], cwd=self.local_path, verbose=self.workspace.verbose
            )

    def push_precheck(self):
        if self.read_only:
            return
        if is_git_dirty(self.local_path):
            raise ConfigurationError(
                "Git repo at %s has uncommitted changes. Please commit your changes before pushing."
                % self.local_path
            )
        if is_pull_needed_from_remote(self.local_path, self.branch, self.workspace.verbose):
            raise ConfigurationError(
                "Resource '%s' requires a pull from the remote origin before pushing." % self.name
            )
        if is_a_git_fat_repo(self.local_path):
            import dataworkspaces.third_party.git_fat as git_fat

            self.python2_exe = git_fat.find_python2_exe()
            self.uses_git_fat = True
        else:
            self.uses_git_fat = False

    def push(self):
        """Push to remote origin, if any"""
        if self.read_only:
            click.echo("Skipping push of resource %s, as it is read-only" % self.name)
            return
        switch_git_branch_if_needed(self.local_path, self.branch, self.workspace.verbose)
        call_subprocess(
            [GIT_EXE_PATH, "push", "origin", self.branch],
            cwd=self.local_path,
            verbose=self.workspace.verbose,
        )
        if self.uses_git_fat:
            import dataworkspaces.third_party.git_fat as git_fat

            git_fat.run_git_fat(
                self.python2_exe, ["push"], cwd=self.local_path, verbose=self.workspace.verbose
            )

    def pull_precheck(self):
        if is_git_dirty(self.local_path):
            raise ConfigurationError(
                "Git repo at %s has uncommitted changes. Please commit your changes before pulling."
                % self.local_path
            )
        if is_a_git_fat_repo(self.local_path):
            import dataworkspaces.third_party.git_fat as git_fat

            self.python2_exe = git_fat.find_python2_exe()
            self.uses_git_fat = True
        else:
            self.uses_git_fat = False

    def pull(self):
        """Pull from remote origin, if any"""
        switch_git_branch_if_needed(self.local_path, self.branch, self.workspace.verbose)
        call_subprocess(
            [GIT_EXE_PATH, "pull", "origin", "master"],
            cwd=self.local_path,
            verbose=self.workspace.verbose,
        )
        if self.uses_git_fat:
            import dataworkspaces.third_party.git_fat as git_fat

            git_fat.run_git_fat(
                self.python2_exe, ["pull"], cwd=self.local_path, verbose=self.workspace.verbose
            )

    def __str__(self):
        return "Git repository %s in role '%s'" % (self.local_path, self.role)


def get_remote_origin(local_path, verbose=False):
    args = [GIT_EXE_PATH, "config", "--get", "remote.origin.url"]
    if verbose:
        click.echo(" ".join(args) + " [run in %s]" % local_path)
    cp = subprocess.run(
        args, cwd=local_path, encoding="utf-8", stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )
    if cp.returncode != 0:
        click.echo("Remote origin not found for git repo at %s" % local_path)
        return None
    return cp.stdout.rstrip()


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


class GitLocalPathType(LocalPathType):
    def __init__(self, remote_url: str, verbose: bool):
        super().__init__()
        self.remote_url = remote_url
        self.verbose = verbose

        def convert(self, value, param, ctx):
            rv = super().convert(value, param, ctx)
            if isdir(rv):
                if not isdir(join(rv, ".git")):
                    self.fail(
                        '%s "%s" exists, but is not a git repository' % (self.path_type, rv),
                        param,
                        ctx,
                    )
                remote = get_remote_origin(rv, verbose=self.verbose)
                if remote != self.remote_url:
                    self.fail(
                        '%s "%s" is a git repo with remote origin "%s", but dataworkspace has remote "%s"'
                        % (self.path_type, rv, self.remote_url, remote),
                        param,
                        ctx,
                    )
            return rv


class GitRepoFactory(ResourceFactory):
    def from_command_line(self, role, name, workspace, local_path, branch, read_only, export):
        """Instantiate a resource object from the add command's
        arguments"""
        print(
            "from_command_line branch=%s, read_only=%s, export=%s" % (branch, read_only, export)
        )  # XXX
        workspace.validate_local_path_for_resource(name, local_path)
        lpr = realpath(local_path)
        wspath = (
            realpath(workspace.get_workspace_local_path_if_any())
            if workspace.get_workspace_local_path_if_any() is not None
            else None
        )
        if not is_git_repo(local_path):
            if (
                isinstance(workspace, git_backend.Workspace)
                and wspath is not None
                and lpr.startswith(wspath)
            ):
                if branch != "master":
                    raise ConfigurationError(
                        "Only the branch 'master' is available for resources that are within the workspace's git repository"
                    )
                elif read_only:
                    raise ConfigurationError(
                        "The --read-only parameter is only valid for separate git repositories, not subdirectories."
                    )
                return GitRepoSubdirFactory().from_command_line(
                    role, name, workspace, local_path, export=export
                )
            else:
                raise ConfigurationError(local_path + " is not a git repository")
        # The local path is a git repo. Double-check that it isn't already part
        # of the workspace's repo. If it is, you will get an error when cloning.
        if (
            isinstance(workspace, git_backend.Workspace)
            and wspath is not None
            and lpr.startswith(wspath)
            and is_file_tracked_by_git(
                local_path, workspace.get_workspace_local_path_if_any(), verbose=workspace.verbose
            )
        ):
            raise ConfigurationError(
                "%s is a git repository, but also part of the parent workspace's repo"
                % (local_path)
            )
        validate_git_fat_in_path_if_needed(local_path)
        ensure_git_lfs_configured_if_needed(local_path, verbose=workspace.verbose)
        remote_origin = get_remote_origin(local_path, verbose=workspace.verbose)
        (current, others) = get_branch_info(local_path, workspace.verbose)
        if branch != current and branch not in others:
            raise ConfigurationError(
                "Requested branch '%s' is not available for git repository at %s"
                % (branch, local_path)
            )
        if is_git_dirty(local_path) and branch != current:
            click.echo(
                "WARNING: Git repo is currently on branch %s and branch %s was requested. However, the current branch has uncommitted changes. Will skip changing the branch after adding the repo to workspace."
                % (current, branch)
            )
        else:
            switch_git_branch(local_path, branch, workspace.verbose)
        workspace_dir = get_workspace_dir(workspace)
        relative_local_path = None  # type: Optional[str]
        if commonpath([workspace_dir, local_path]) == workspace_dir:
            relative_local_path = get_subpath_from_absolute(workspace_dir, local_path)
        return GitRepoResource(
            name,
            role,
            workspace,
            remote_origin,
            relative_local_path,
            local_path,
            branch,
            read_only,
            export,
        )

    def from_json(self, params, local_params, workspace):
        """Instantiate a resource object from the parsed resources.json file"""
        assert params["resource_type"] == "git"
        local_path = local_params["local_path"]
        if not isabs(local_path):
            local_path = join(get_workspace_dir(workspace), local_path)
        return GitRepoResource(
            params["name"],
            params["role"],
            workspace,
            params["remote_origin_url"],
            params.get("relative_local_path", None),
            local_path,
            params["branch"],
            params.get("read_only", False),
            params.get("export", False),
        )

    def has_local_state(self):
        return True

    def clone(self, params, workspace):
        assert params["resource_type"] == "git"
        rname = params["name"]
        remote_origin_url = params["remote_origin_url"]
        relative_local_path = params.get("relative_local_path", None)
        workspace_dir = get_workspace_dir(workspace)
        default_local_path = (
            join(workspace_dir, relative_local_path)
            if relative_local_path
            else join(workspace_dir, rname)
        )
        branch = params["branch"]
        read_only = params.get("read_only", False)
        if not workspace.batch:
            # ask the user for a local path
            local_path = click.prompt(
                "Git resource '%s' is being added to your workspace. Where do you want to clone it?"
                % rname,
                default=default_local_path,
                type=GitLocalPathType(remote_origin_url, workspace.verbose),
            )
        else:
            if isdir(default_local_path):
                if not isdir(join(default_local_path, ".git")):
                    raise ConfigurationError(
                        "Unable to add resource '%s' as default local path '%s' exists but is not a git repository."
                        % (rname, default_local_path)
                    )
                remote = get_remote_origin(default_local_path, workspace.verbose)
                if remote != remote_origin_url:
                    raise ConfigurationError(
                        "Unable to add resource '%s' as remote origin in local path '%s' is %s, but data workspace has '%s'"
                        % (rname, default_local_path, remote, remote_origin_url)
                    )
            local_path = default_local_path
        parent = dirname(local_path)
        if not exists(local_path):
            # cloning a fresh repository
            cmd = [GIT_EXE_PATH, "clone", remote_origin_url, basename(local_path)]
            call_subprocess(cmd, parent, workspace.verbose)
        else:
            # the repo already exists locally, and we've alerady verified that then
            # remote is correct
            cmd = [GIT_EXE_PATH, "pull", "origin", "master"]
            call_subprocess(cmd, local_path, workspace.verbose)
        switch_git_branch_if_needed(local_path, branch, workspace.verbose, ok_if_not_present=True)
        ensure_git_lfs_configured_if_needed(local_path, verbose=workspace.verbose)
        if is_a_git_fat_repo(local_path) and not has_git_fat_been_initialized(local_path):
            import dataworkspaces.third_party.git_fat as git_fat

            python2_exe = git_fat.find_python2_exe()
            git_fat.run_git_fat(python2_exe, ["init"], cwd=local_path, verbose=workspace.verbose)
            git_fat.run_git_fat(python2_exe, ["pull"], cwd=local_path, verbose=workspace.verbose)

        return GitRepoResource(
            rname,
            params["role"],
            workspace,
            remote_origin_url,
            relative_local_path,
            local_path,
            branch,
            read_only,
            params.get("export", False),
        )

    def suggest_name(self, workspace, role, local_path, branch, read_only, export):
        return basename(local_path)


def _get_workspace_dir_for_git_backend(workspace):
    """This is used by the git-subdirectory resources, which only work with the
    git backend for the workspace...
    """
    if not isinstance(workspace, git_backend.Workspace):
        raise ConfigurationError(
            "Git subdirectory resources are only supported with the Git workspace backend."
        )
    workspace_dir = workspace.get_workspace_local_path_if_any()
    assert workspace_dir is not None
    return workspace_dir


class GitRepoResultsSubdirResource(GitResourceBase):
    """Resource for a subdirectory of the workspace for when it is
     in the results role.
    """

    def __init__(self, name: str, workspace: Workspace, relative_path: str, export: bool):
        # only valid when workspace has git backend
        workspace_dir = _get_workspace_dir_for_git_backend(workspace)
        super().__init__(
            "git-subdirectory",
            name,
            ResourceRoles.RESULTS,
            workspace,
            join(workspace_dir, relative_path),
            export=export,
            repo_dir=workspace_dir,
        )
        self.workspace_dir = workspace_dir
        self.param_defs.define(
            "relative_path",
            default_value=None,
            optional=False,
            help="Path of resource's directory relative to the workspace root",
            is_global=True,
            ptype=RelpathType(),
        )
        self.relative_path = self.param_defs.get("relative_path", relative_path)  # type: str

    def results_move_current_files(
        self, rel_dest_root: str, exclude_files: Set[str], exclude_dirs_re: Pattern
    ):
        validate_git_fat_in_path_if_needed(self.workspace_dir)
        moved_files = move_current_files_local_fs(
            self.name,
            self.local_path,
            rel_dest_root,
            exclude_files,
            exclude_dirs_re,
            move_fn=lambda src, dest: git_move_and_add(
                src, dest, self.local_path, self.workspace.verbose
            ),
            verbose=self.workspace.verbose,
        )
        # If there were no files in the results dir, then we do not
        # create a subdirectory for this snapshot
        if len(moved_files) > 0:
            call_subprocess(
                [
                    GIT_EXE_PATH,
                    "commit",
                    "--only",
                    self.relative_path,
                    "-m",
                    "Move current results to %s" % rel_dest_root,
                ],
                cwd=self.workspace_dir,
                verbose=self.workspace.verbose,
            )

    def snapshot_precheck(self):
        validate_git_fat_in_path_if_needed(self.workspace_dir)

    def snapshot(self):
        # The subdirectory hash is used for comparison and the head
        # hash used for restoring
        return (
            get_subdirectory_hash(
                self.workspace_dir, self.relative_path, verbose=self.workspace.verbose
            ),
            get_local_head_hash(self.workspace_dir, verbose=self.workspace.verbose),
        )

    def restore_precheck(self, hashval):
        raise ConfigurationError(
            "Git subdirectory resource '%s' should not be included in restore set" % self.name
        )

    def restore(self, hashval):
        raise InternalError(
            "Should never call restore on a git subdirectory resource (%s)" % self.name
        )

    def add_results_file(self, data: Union[JSONDict, JSONList], rel_dest_path: str) -> None:
        """Save JSON results data to the specified path in the resource.
        """
        abs_dest_path = join(self.local_path, rel_dest_path)
        parent_dir = dirname(abs_dest_path)
        if not exists(parent_dir):
            os.makedirs(parent_dir)
        with open(abs_dest_path, "w") as f:
            json.dump(data, f, indent=2)
        rel_to_repo_path = join(self.relative_path, rel_dest_path)
        call_subprocess(
            [GIT_EXE_PATH, "add", rel_to_repo_path],
            cwd=self.workspace_dir,
            verbose=self.workspace.verbose,
        )
        call_subprocess(
            [GIT_EXE_PATH, "commit", "-m", "Added %s" % rel_to_repo_path],
            cwd=self.workspace_dir,
            verbose=self.workspace.verbose,
        )

    def push_precheck(self):
        if not exists(self.local_path):
            raise ConfigurationError(
                "Missing directory %s for resource %s" % (self.local_path, self.name)
            )
        if is_git_dirty(self.workspace_dir):
            raise ConfigurationError(
                "Git repo at %s has uncommitted changes. Please commit your changes before pushing."
                % self.workspace_dir
            )
        if is_pull_needed_from_remote(self.workspace_dir, "master", self.workspace.verbose):
            raise ConfigurationError(
                "Resource '%s' requires a pull from the remote origin before pushing." % self.name
            )

    def push(self):
        """Push to remote origin, if any"""
        pass  # push will happen at workspace level

    def pull_precheck(self):
        if is_git_dirty(self.local_path):
            raise ConfigurationError(
                "Git repo at %s has uncommitted changes. Please commit your changes before pulling."
                % self.workspace_dir
            )

    def pull(self):
        """Pull from remote origin, if any"""
        pass  # pull will happen at workspace level

    def __str__(self):
        return "Git repository subdirectory %s in role '%s'" % (self.relative_path, self.role)


class GitRepoSubdirResource(GitResourceBase):
    """Resource for a subdirectory of the workspace for when it is NOT
    in the results role.
    """

    def __init__(
        self, name: str, role: str, workspace: Workspace, relative_path: str, export: bool
    ):
        assert role != ResourceRoles.RESULTS
        workspace_dir = _get_workspace_dir_for_git_backend(workspace)
        super().__init__(
            "git-subdirectory",
            name,
            role,
            workspace,
            join(workspace_dir, relative_path),
            export=export,
            repo_dir=workspace_dir,
        )
        self.workspace_dir = workspace_dir
        self.param_defs.define(
            "relative_path",
            default_value=None,
            optional=False,
            help="Path of resource's directory relative to the workspace root",
            is_global=True,
            ptype=RelpathType(),
        )
        self.relative_path = self.param_defs.get("relative_path", relative_path)  # type: str

    def results_move_current_files(
        self, rel_dest_root: str, exclude_files: Set[str], exclude_dirs_re: Pattern
    ):
        raise InternalError(
            "res<ults_move_current_files should not be called for %s" % self.__class__.__name__
        )

    def add_results_file(self, data, rel_dest_path) -> None:
        """Copy a results file from the temporary location to
        the specified path in the resource. Caller responsible for cleanup.
        """
        raise InternalError(
            "add_results_file should not be called for %s" % self.__class__.__name__
        )

    def snapshot_precheck(self):
        validate_git_fat_in_path_if_needed(self.workspace_dir)

    def snapshot(self) -> Tuple[Optional[str], Optional[str]]:
        """Returns (cmopare_hash, restore_hash)
        """
        # Todo: handle tags
        commit_changes_in_repo_subdir(
            self.workspace_dir,
            self.relative_path,
            "autocommit ahead of snapshot",
            verbose=self.workspace.verbose,
        )
        return (
            get_subdirectory_hash(
                self.workspace_dir, self.relative_path, verbose=self.workspace.verbose
            ),
            get_local_head_hash(self.workspace_dir, verbose=self.workspace.verbose),
        )

    def restore_precheck(self, hashval):
        validate_git_fat_in_path_if_needed(self.workspace_dir)
        rc = call_subprocess_for_rc(
            [GIT_EXE_PATH, "cat-file", "-e", hashval + "^{commit}"],
            cwd=self.workspace_dir,
            verbose=self.workspace.verbose,
        )
        if rc != 0:
            raise ConfigurationError("No commit found with hash '%s' in %s" % (hashval, str(self)))

    def restore(self, hashval):
        commit_changes_in_repo_subdir(
            self.workspace_dir,
            self.relative_path,
            "auto-commit ahead of restore",
            verbose=self.workspace.verbose,
        )
        checkout_subdir_and_apply_commit(
            self.workspace_dir, self.relative_path, hashval, verbose=self.workspace.verbose
        )

    def push_precheck(self):
        if not exists(self.local_path):
            raise ConfigurationError(
                "Missing directory %s for resource %s" % (self.local_path, self.name)
            )
        if is_git_dirty(self.local_path):
            raise ConfigurationError(
                "Git repo at %s has uncommitted changes. Please commit your changes before pushing."
                % self.local_path
            )
        if is_pull_needed_from_remote(self.local_path, "master", self.workspace.verbose):
            raise ConfigurationError(
                "Resource '%s' requires a pull from the remote origin before pushing." % self.name
            )

    def push(self):
        """Push to remote origin, if any"""
        pass  # push will happen at workspace level
        # actions.call_subprocess([GIT_EXE_PATH, 'push', 'origin', 'master'],
        #                         cwd=self.local_path, verbose=self.verbose)

    def pull_precheck(self):
        if is_git_dirty(self.local_path):
            raise ConfigurationError(
                "Git repo at %s has uncommitted changes. Please commit your changes before pulling."
                % self.local_path
            )

    def pull(self):
        """Pull from remote origin, if any"""
        pass  # pull will happen at workspace level
        # actions.call_subprocess([GIT_EXE_PATH, 'pull', 'origin', 'master'],
        #                         cwd=self.local_path, verbose=self.verbose)

    def __str__(self):
        return "Git repository subdirectory %s in role '%s'" % (self.relative_path, self.role)


CONFIRM_SUBDIR_MSG = (
    "The subdirectory %s does not currently exist, but must be a part of the workspace's git repo in order"
    + " to be used as a resource. Do you want it to be created and added to git?"
)


def create_results_subdir(workspace_dir, full_path, relative_path, role, verbose):
    os.makedirs(full_path)
    with open(join(full_path, "README.txt"), "w") as f:
        f.write("This directory is for %s.\n" % RESOURCE_ROLE_PURPOSES[role])
        f.write(
            "This README file ensures the directory is added to the git repository, as git does not support empty directories.\n"
        )
    call_subprocess([GIT_EXE_PATH, "add", relative_path], cwd=workspace_dir, verbose=verbose)
    call_subprocess(
        [GIT_EXE_PATH, "commit", "-m", "Add %s to repo for storing results" % relative_path],
        cwd=workspace_dir,
        verbose=verbose,
    )
    click.echo("Added %s to git repository" % relative_path)


class GitRepoSubdirFactory(ResourceFactory):
    """This is a version of a git repo resource where we are just
    storing in a subdirectory of a repo rather than the full repo.
    This is currently only valid if we are storing as a subdir of the
    main data workspace repo.
    """

    def from_command_line(
        self, role, name, workspace, local_path, confirm_subdir_create=True, export=False
    ):
        """Instantiate a resource object from the add command's
        arguments"""
        if is_git_repo(local_path):
            raise InternalError(
                "Local path '%s'is a git repo, should not be using GitRepoSubdirFactory"
                % local_path
            )
        lpr = realpath(local_path)
        workspace_dir = _get_workspace_dir_for_git_backend(workspace)
        validate_git_fat_in_path_if_needed(workspace_dir)
        ensure_git_lfs_configured_if_needed(workspace_dir, verbose=workspace.verbose)
        wdr = realpath(workspace_dir)
        if not lpr.startswith(wdr):
            raise ConfigurationError(
                "Git subdirectories can only be used as resources when under the workspace repo."
            )
        relative_path = lpr[len(wdr) + 1 :]
        if not exists(local_path):
            if not confirm_subdir_create:
                create_results_subdir(
                    workspace_dir, local_path, relative_path, role, workspace.verbose
                )
            elif not workspace.batch:
                click.confirm(CONFIRM_SUBDIR_MSG % relative_path, abort=True)
                create_results_subdir(
                    workspace_dir, local_path, relative_path, role, workspace.verbose
                )
            else:
                raise ConfigurationError(
                    "Did not find '%s'. Cannot create a resource from a git subdirectory if the directory does not already exist."
                    % local_path
                )
        if role == ResourceRoles.RESULTS:
            return GitRepoResultsSubdirResource(name, workspace, relative_path, export)
        else:
            return GitRepoSubdirResource(name, role, workspace, relative_path, export)

    def from_json(self, params, local_params, workspace):
        """Instantiate a resource object from the parsed resources.json file"""
        assert params["resource_type"] == "git-subdirectory"
        if params["role"] == ResourceRoles.RESULTS:
            return GitRepoResultsSubdirResource(
                params["name"], workspace, params["relative_path"], params.get("export", False)
            )
        else:
            return GitRepoSubdirResource(
                params["name"],
                params["role"],
                workspace,
                params["relative_path"],
                params.get("export", False),
            )

    def clone(self, params, workspace):
        assert params["resource_type"] == "git-subdirectory"
        rname = params["name"]
        role = params["role"]
        relative_path = params["relative_path"]
        if not isinstance(workspace, git_backend.Workspace):
            raise ConfigurationError(
                "Git subdirectory resources are only supported with the Git workspace backend."
            )
        workspace_dir = workspace.get_workspace_local_path_if_any()
        assert workspace_dir is not None
        # this should be redundant, as we already intialized for the parent repo, but run just in case.
        ensure_git_lfs_configured_if_needed(workspace_dir, verbose=workspace.verbose)
        local_path = join(workspace_dir, relative_path)
        if not exists(local_path):
            # this subdirectory most have been created in the remote
            # resource. We can just wait for the "git pull" to populate the
            # the contents, but will create a placeholder so our checks pass.
            os.mkdir(local_path)
        if role == ResourceRoles.RESULTS:
            return GitRepoResultsSubdirResource(
                rname, workspace, relative_path, params.get("export", False)
            )
        else:
            return GitRepoSubdirResource(
                rname, role, workspace, relative_path, params.get("export", False)
            )

    def has_local_state(self) -> bool:
        return True

    def suggest_name(self, local_path, *args):
        return basename(local_path)
