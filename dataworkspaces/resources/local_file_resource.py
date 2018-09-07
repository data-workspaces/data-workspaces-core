"""
Resource for files living in a local directory 
"""
import subprocess
from errno import EEXIST
import os
import os.path 

from dataworkspaces.errors import ConfigurationError
import dataworkspaces.commands.actions as actions
from .resource import Resource, ResourceFactory
from . import hashtree

LOCAL_FILE = 'file'

class LocalFileResource(Resource):
    def __init__(self, url, role, workspace_dir, local_path):
        super().__init__(LOCAL_FILE, url, role, workspace_dir)
        self.local_path = local_path


    def to_json(self):
        return {
            'resource_type': LOCAL_FILE,
            'role':self.role,
            'url':self.url,
            'local_path':self.local_path
        }

    def add_prechecks(self):
        if not(os.path.isdir(self.local_path)):
            raise ConfigurationError(self.local_path + ' does not exist')
        if not(os.access(self.local_path, os.R_OK)): 
            raise ConfigurationError(self.local_path + ' does not have read permission')
        if os.path.realpath(self.local_path)==os.path.realpath(self.workspace_dir):
            raise ConfigurationError("Cannot add the entire workspace as a file resource")


    def add(self):
        try:
            rsrcdir = os.path.abspath(self.workspace_dir + 'resources/' + self.role + '/' + self.local_path)
            os.makedirs(rsrcdir)
        except OSError as exc:
            if exc.errno == EEXIST and os.path.isdir(rsrcdir):
                pass
            else: raise

    def snapshot_prechecks(self):
        pass

    def snapshot(self):
        rsrcdir = os.path.abspath(self.workspace_dir + 'resources/' + self.role + '/' + self.local_path)
        h = hashtree.generate_hashes(rsrcdir, self.local_path)
        return h.strip()

    def __str__(self):
        return "Local directory %s in role '%s'" % (self.local_path, self.role)

class LocalFileFactory(ResourceFactory):
    def from_command_line(self, role, workspace_dir, batch, verbose,
                          local_path):
        """Instantiate a resource object from the add command's arguments"""
        url = LOCAL_FILE + ':' + local_path
        return LocalFileResource(url, role, workspace_dir, local_path)

    def from_json(self, json_data, workspace_dir, batch, verbose):
        """Instantiate a resource object from the parsed resources.json file"""
        assert json_data['resource_type']==LOCAL_FILE
        return LocalFileResource(json_data['url'], json_data['role'], workspace_dir, json_data['local_path'])

