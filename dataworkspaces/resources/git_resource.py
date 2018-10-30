"""
Resource for git repositories
"""
import subprocess
from os.path import realpath, basename
import re

from dataworkspaces.errors import ConfigurationError
import dataworkspaces.commands.actions as actions
from .resource import Resource, ResourceFactory
from .results_utils import move_current_files_local_fs


def is_git_dirty(cwd):
    if actions.GIT_EXE_PATH is None:
        raise actions.ConfigurationError("git executable not found")
    cmd = [actions.GIT_EXE_PATH, 'diff', '--exit-code', '--quiet']
    result = subprocess.run(cmd, stdout=subprocess.PIPE, cwd=cwd)
    return result.returncode!=0

DOT_GIT_RE=re.compile(re.escape('.git'))

class GitRepoResource(Resource):
    def __init__(self, name, role, workspace_dir, local_path, verbose=False):
        super().__init__('git', name, role, workspace_dir)
        self.local_path = local_path
        self.verbose = verbose

    def to_json(self):
        d = super().to_json()
        d['local_path'] = self.local_path
        return d

    def local_params_to_json(self):
        return {'local_path':self.local_path}

    def get_local_path_if_any(self):
        return self.local_path
    
    def add_prechecks(self):
        if not actions.is_git_repo(self.local_path):
            raise ConfigurationError(self.local_path + ' is not a git repository')
        if realpath(self.local_path)==realpath(self.workspace_dir):
            raise ConfigurationError("Cannot add the entire workspace as a git resource")

    def add(self):
        pass

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
            raise ConfigurationEror("No commit found with hash '%s' in %s" %
                                    (hashval, str(self)))
    def restore(self, hashval):
        actions.call_subprocess([actions.GIT_EXE_PATH, 'reset', '--hard', hashval],
                                cwd=self.local_path, verbose=self.verbose)

    def __str__(self):
        return "Git repository %s in role '%s'" % (self.local_path, self.role)


class GitRepoFactory(ResourceFactory):
    def from_command_line(self, role, name, workspace_dir, batch, verbose,
                          local_path):
        """Instantiate a resource object from the add command's
        arguments"""
        url = 'git:' + local_path
        return GitRepoResource(name, role, workspace_dir,
                               local_path, verbose)

    def from_json(self, json_data, local_params, workspace_dir, batch, verbose):
        """Instantiate a resource object from the parsed resources.json file"""
        assert json_data['resource_type']=='git'
        return GitRepoResource(json_data['name'], json_data['role'],
                               workspace_dir, json_data['local_path'],
                               verbose)

    def suggest_name(self, local_path):
        return basename(local_path)

