"""
Classes for resources
"""
import subprocess

import actions

class ResourceRoles:
    SOURCE_DATA_SET='SourceDataSet'
    INTERMEDIATE_DATA='IntermediateData'
    CODE='Code'
    RESULTS='ResultsData'

class Resource:
    """Base class for all resources"""
    def get_role(self):
        pass

    def to_json(self):
        """Return a json (unserialized) representation of this
        resource for the resources file.
        """
        pass

    def add_prechecks(self, dws_basedir):
        pass

    def add(self, dws_basedir):
        pass

    def snapshot_prechecks(self, dws_basedir):
        pass

    def snapshot(self, dws_basedir):
        pass

def is_git_dirty(cwd):
    if actions.GIT_EXE_PATH is None:
        raise actions.ConfigurationError("git executable not found")
    cmd = [actions.GIT_EXE_PATH, 'diff', '--exit-code', '--quiet']
    result = subprocess.run(cmd, stdout=PIPE, cwd=cwd)
    return result.returncode!=0


class GitRepoResource(Resource):
    def __init__(self, local_directory, role):
        self.local_directory = local_directory
        self.role = role

    def get_role(self):
        return self.role

    def to_json(self):
        return {
            'resource_type': 'git',
            'role':self.role,
            'local_directory':self.local_directory
        }

    def add_prechecks(self, dws_basedir):
        if not actions.is_git_repo(self.local_directory):
            raise actions.ConfigurationError(self.local_directory + ' is not a git repository')
        if is_git_dirty(self.local_directory):
            raise actions.ConfigurationError(
                "Git repo at %s has uncommitted changes. Please commit your changes before adding to workspace" %
                self.local_directory)

    def add(self, dws_basedir):
        pass

    def snapshot_prechecks(self, dws_basedir):
        if is_git_dirty(self.local_directory):
            raise actions.ConfigurationError(
                "Git repo at %s has uncommitted changes. Please commit your changes before taking a snapshot" %
                self.local_directory)

    def snapshot(self, dws_basedir):
        # Todo: handle tags
        pass

    def __str__(self):
        return "Git repository %s in role %s" % (self.local_directory, self.role)

