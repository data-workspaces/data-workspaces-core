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
    def __init__(self, name, role, workspace_dir, local_path, ignore=[]):
        super().__init__(LOCAL_FILE, name, role, workspace_dir)
        self.local_path = local_path
        self.ignore = ignore

    def to_json(self):
        d = super().to_json()
        d['local_path'] = self.local_path
        return d

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
        h = hashtree.generate_hashes(rsrcdir, self.local_path, ignore=self.ignore)
        return h.strip()

    def restore_prechecks(self, hashval):
        rsrcdir = os.path.abspath(self.workspace_dir + 'resources/' + self.role + '/' + self.local_path)
        rc = hashtree.check_hashes(hashval, rsrcdir, self.local_path, ignore=self.ignore)
        if not rc:
            raise ConfigurationError("Local file structure not compatible with saved hash")

    def restore(self, hashval):
        pass # local files: do nothing to restore

    def __str__(self):
        return "Local directory %s in role '%s'" % (self.local_path, self.role)

class LocalFileFactory(ResourceFactory):
    def from_command_line(self, role, name, workspace_dir, batch, verbose,
                          local_path):
        """Instantiate a resource object from the add command's arguments"""
        return LocalFileResource(name, role, workspace_dir, local_path)

    def from_json(self, json_data, local_params, workspace_dir, batch, verbose):
        """Instantiate a resource object from the parsed resources.json file"""
        assert json_data['resource_type']==LOCAL_FILE
        return LocalFileResource(json_data['name'],
                                 json_data['role'], workspace_dir, json_data['local_path'])

    def suggest_name(self, local_path):
        return os.path.basename(local_path)

