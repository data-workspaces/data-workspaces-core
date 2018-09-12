"""
Resource for git repositories
"""
import subprocess
from os.path import realpath, basename

from dataworkspaces.errors import ConfigurationError
import dataworkspaces.commands.actions as actions
from .resource import Resource, ResourceFactory


def is_git_dirty(cwd):
    if actions.GIT_EXE_PATH is None:
        raise actions.ConfigurationError("git executable not found")
    cmd = [actions.GIT_EXE_PATH, 'diff', '--exit-code', '--quiet']
    result = subprocess.run(cmd, stdout=subprocess.PIPE, cwd=cwd)
    return result.returncode!=0


class GitRepoResource(Resource):
    def __init__(self, name, url, role, workspace_dir, local_path):
        super().__init__('git', name, url, role, workspace_dir)
        self.local_path = local_path


    def to_json(self):
        d = super().to_json()
        d['local_path'] = self.local_path
        return d

    def add_prechecks(self):
        if not actions.is_git_repo(self.local_path):
            raise ConfigurationError(self.local_path + ' is not a git repository')
        if realpath(self.local_path)==realpath(self.workspace_dir):
            raise ConfigurationError("Cannot add the entire workspace as a git resource")

    def add(self):
        pass

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

    def __str__(self):
        return "Git repository %s in role '%s'" % (self.local_path, self.role)

class GitRepoFactory(ResourceFactory):
    def from_command_line(self, role, name, workspace_dir, batch, verbose,
                          local_path):
        """Instantiate a resource object from the add command's
        arguments"""
        url = 'git:' + local_path
        return GitRepoResource(name, url, role, workspace_dir,
                               local_path)

    def from_json(self, json_data, workspace_dir, batch, verbose):
        """Instantiate a resource object from the parsed resources.json file"""
        assert json_data['resource_type']=='git'
        return GitRepoResource(json_data['name'], json_data['url'], json_data['role'],
                               workspace_dir, json_data['local_path'])

    def suggest_name(self, local_path):
        return basename(local_path)

