"""
Resource for git repositories
"""
import subprocess
from os.path import realpath, basename, isdir, join, dirname, exists, basename
import re

import click

from dataworkspaces.errors import ConfigurationError
import dataworkspaces.commands.actions as actions
from .resource import Resource, ResourceFactory, LocalPathType
from .results_utils import move_current_files_local_fs


def is_git_dirty(cwd):
    if actions.GIT_EXE_PATH is None:
        raise actions.ConfigurationError("git executable not found")
    cmd = [actions.GIT_EXE_PATH, 'diff', '--exit-code', '--quiet']
    result = subprocess.run(cmd, stdout=subprocess.PIPE, cwd=cwd)
    return result.returncode!=0

def is_pull_needed_from_remote(cwd, verbose):
    """Do check whether we need a pull, we get the hash of the HEAD
    of the remote's master branch. Then, we see if we have this object locally.
    """
    cmd = [actions.GIT_EXE_PATH, 'ls-remote', 'origin', '-h', 'refs/heads/master']
    try:
        hashval = actions.call_subprocess(cmd, cwd, verbose).rstrip()
        if hashval=='':
            return False # remote has not commits
        else:
            hashval = hashval.split()[0]
    except Exception as e:
        raise ConfigurationError("Problem in accessing remote repository associated with '%s'" %
                                 cwd) from e
    #cmd = [actions.GIT_EXE_PATH, 'show', '--oneline', hashval]
    cmd = [actions.GIT_EXE_PATH, 'cat-file', '-e', hashval+'^{commit}']
    rc = actions.call_subprocess_for_rc(cmd, cwd, verbose=verbose)
    return rc!=0


DOT_GIT_RE=re.compile(re.escape('.git'))

class GitRepoResource(Resource):
    def __init__(self, name, role, workspace_dir, remote_origin_url,
                 local_path, verbose=False):
        super().__init__('git', name, role, workspace_dir)
        self.local_path = local_path
        self.remote_origin_url = remote_origin_url
        self.verbose = verbose

    def to_json(self):
        d = super().to_json()
        d['remote_origin_url'] = self.remote_origin_url
        return d

    def local_params_to_json(self):
        return {'local_path':self.local_path}

    def get_local_path_if_any(self):
        return self.local_path

    def add_prechecks(self):
        pass

    def add(self):
        pass

    def add_from_remote(self):
        parent = dirname(self.local_path)
        if not exists(self.local_path):
            # cloning a fresh repository
            cmd = [actions.GIT_EXE_PATH, 'clone', self.remote_origin_url, basename(self.local_path)]
            actions.call_subprocess(cmd, parent, self.verbose)
        else:
            # the repo already exists locally, and we've alerady verified that then
            # remote is correct
            cmd = [actions.GIT_EXE_PATH, 'pull', 'origin', 'master']
            actions.call_subprocess(cmd, self.local_path, self.verbose)

    def results_move_current_files(self, rel_dest_root, exclude_files,
                                   exclude_dirs_re):
        def git_move(srcpath, destpath):
            actions.call_subprocess([actions.GIT_EXE_PATH, 'mv',
                                     srcpath, destpath],
                                    cwd=self.local_path,
                                    verbose=self.verbose)
        moved_files = move_current_files_local_fs(
            self.name, self.local_path, rel_dest_root,
            exclude_files,
            [exclude_dirs_re, DOT_GIT_RE],
            move_fn=git_move,
            verbose=self.verbose)
        # If there were no files in the results dir, then we do not
        # create a subdirectory for this snapshot
        if len(moved_files)>0:
            actions.call_subprocess([actions.GIT_EXE_PATH, 'commit',
                                     '-m', "Move current results to %s" % rel_dest_root],
                                    cwd=self.local_path,
                                    verbose=self.verbose)

    def snapshot_prechecks(self):
        if is_git_dirty(self.local_path):
            raise ConfigurationError(
                "Git repo at %s has uncommitted changes. Please commit your changes before taking a snapshot" %
                self.local_path)

    def snapshot(self):
        # Todo: handle tags
        hashval = actions.call_subprocess([actions.GIT_EXE_PATH, 'rev-parse',
                                           'HEAD'],
                                          cwd=self.local_path, verbose=False)
        return hashval.strip()

    def restore_prechecks(self, hashval):
        rc = actions.call_subprocess_for_rc([actions.GIT_EXE_PATH, 'cat-file', '-e',
                                     hashval+"^{commit}"],
                                            cwd=self.local_path,
                                            verbose=self.verbose)
        if rc!=0:
            raise ConfigurationError("No commit found with hash '%s' in %s" %
                                     (hashval, str(self)))
    def restore(self, hashval):
        actions.call_subprocess([actions.GIT_EXE_PATH, 'reset', '--hard', hashval],
                                cwd=self.local_path, verbose=self.verbose)

    def push_prechecks(self):
        if is_git_dirty(self.local_path):
            raise ConfigurationError(
                "Git repo at %s has uncommitted changes. Please commit your changes before pushing." %
                self.local_path)
        if is_pull_needed_from_remote(self.local_path, self.verbose):
            raise ConfigurationError("Resource '%s' requires a pull from the remote origin before pushing." %
                                     self.name)

    def push(self):
        """Push to remote origin, if any"""
        actions.call_subprocess([actions.GIT_EXE_PATH, 'push', 'origin', 'master'],
                                cwd=self.local_path, verbose=self.verbose)

    def pull_prechecks(self):
        if is_git_dirty(self.local_path):
            raise ConfigurationError(
                "Git repo at %s has uncommitted changes. Please commit your changes before pulling." %
                self.local_path)

    def pull(self):
        """Pull from remote origin, if any"""
        actions.call_subprocess([actions.GIT_EXE_PATH, 'pull', 'origin', 'master'],
                                cwd=self.local_path, verbose=self.verbose)

    def __str__(self):
        return "Git repository %s in role '%s'" % (self.local_path, self.role)

def get_remote_origin(local_path, verbose=False):
    args = [actions.GIT_EXE_PATH, 'config', '--get', 'remote.origin.url']
    if verbose:
        click.echo(" ".join(args) + " [run in %s]" % local_path)
    cp = subprocess.run(args, cwd=local_path, encoding='utf-8',
                        stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if cp.returncode!=0:
        click.echo("Remote origin not found for git repo at %s" % local_path)
        return None
    return cp.stdout.rstrip()


class GitLocalPathType(LocalPathType):
    def __init__(self, remote_url, verbose):
        super().__init__()
        self.remote_url = remote_url
        self.verbose = verbose

        def convert(self, value, param, ctx):
            rv = super().convert(value, param, ctx)
            if isdir(rv):
                if not isdir(join(rv, '.git')):
                    self.fail('%s "%s" exists, but is not a git repository' % (self.path_type, rv),
                              param, ctx)
                remote = get_remote_origin(rv, verbose=self.verbose)
                if remote!=self.remote_url:
                    self.fail('%s "%s" is a git repo with remote origin "%s", but dataworkspace has remote "%s"'%
                              (self.path_type, rv), param, ctx)
            return rv


class GitRepoFactory(ResourceFactory):
    def from_command_line(self, role, name, workspace_dir, batch, verbose,
                          local_path):
        """Instantiate a resource object from the add command's
        arguments"""
        if not actions.is_git_repo(local_path):
            raise ConfigurationError(local_path + ' is not a git repository')
        if realpath(local_path)==realpath(workspace_dir):
            raise ConfigurationError("Cannot add the entire workspace as a git resource")
        remote_origin = get_remote_origin(local_path, verbose=verbose)
        return GitRepoResource(name, role, workspace_dir,
                               remote_origin, local_path, verbose)

    def from_json(self, json_data, local_params, workspace_dir, batch, verbose):
        """Instantiate a resource object from the parsed resources.json file"""
        assert json_data['resource_type']=='git'
        return GitRepoResource(json_data['name'], json_data['role'],
                               workspace_dir, json_data['remote_origin_url'],
                               local_params['local_path'],
                               verbose)

    def from_json_remote(self, json_data, workspace_dir, batch, verbose):
        assert json_data['resource_type']=='git'
        rname = json_data['name']
        remote_origin_url = json_data['remote_origin_url']
        default_local_path = join(workspace_dir, rname)
        if not batch:
            # ask the user for a local path
            local_path = \
                click.prompt("Git resource '%s' is being added to your workspace. Where do you want to clone it?"%
                             rname,
                             default=default_local_path, type=GitLocalPathType(remote_origin_url, verbose))
        else:
            if isdir(default_local_path):
                if not isdir(join(default_local_path, '.git')):
                    raise ConfigurationError("Unable to add resource '%s' as default local path '%s' exists but is not a git repository."%
                                             (rname, default_local_path))
                remote = get_remote_origin(default_local_path, verbose)
                if remote!=remote_origin_url:
                    raise ConfigurationError("Unable to add resource '%s' as remote origin in local path '%s' is %s, but data workspace has '%s'"%
                                             (rname, default_local_path, remote, remote_origin_url))
            local_path = default_local_path
        return GitRepoResource(rname, json_data['role'],
                               workspace_dir, remote_origin_url,
                               local_path, verbose)

    def suggest_name(self, local_path):
        return basename(local_path)

