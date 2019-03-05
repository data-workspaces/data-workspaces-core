# Copyright 2018,2019 by MPI-SWS and Data-ken Research. Licensed under Apache 2.0. See LICENSE.txt.
"""
Resource for files living in a local directory 
"""
from errno import EEXIST
import os
import os.path 

from dataworkspaces.errors import ConfigurationError
from dataworkspaces.utils.subprocess_utils import call_subprocess
from dataworkspaces.utils.git_utils import GIT_EXE_PATH, is_git_staging_dirty
from .resource import Resource, ResourceFactory, ResourceRoles
from . import hashtree
from .snapshot_utils import move_current_files_local_fs


LOCAL_FILE = 'file'

class LocalFileResource(Resource):
    def __init__(self, name, role, workspace_dir, local_path, ignore=[]):
        super().__init__(LOCAL_FILE, name, role, workspace_dir)
        self.local_path = local_path
        self.ignore = ignore
        self.rsrcdir = os.path.abspath(self.workspace_dir + '/.dataworkspace/' + LOCAL_FILE + '/' + self.role + '/' + self.name)
        self.rsrcdir_relative = '.dataworkspace/' +LOCAL_FILE + '/' + self.role + '/' + self.name

    def to_json(self):
        d = super().to_json()
        d['local_path'] = self.local_path
        return d

    def get_local_path_if_any(self):
        return self.local_path
    
    def add_prechecks(self):
        if not(os.path.isdir(self.local_path)):
            raise ConfigurationError(self.local_path + ' does not exist')
        if not(os.access(self.local_path, os.R_OK)): 
            raise ConfigurationError(self.local_path + ' does not have read permission')
        if os.path.realpath(self.local_path)==os.path.realpath(self.workspace_dir):
            raise ConfigurationError("Cannot add the entire workspace as a file resource")


    def add(self):
        try:
            os.makedirs(self.rsrcdir)
            with open(os.path.join(self.rsrcdir, 'dummy.txt'), 'w') as f:
                f.write("Placeholder to ensure directory is added to git\n")
            call_subprocess([GIT_EXE_PATH, 'add',
                             self.rsrcdir_relative],
                                    cwd=self.workspace_dir)
            call_subprocess([GIT_EXE_PATH, 'commit', '-m',
                             "Adding resource %s" % self.name],
                                    cwd=self.workspace_dir)
        except OSError as exc:
            if exc.errno == EEXIST and os.path.isdir(self.rsrcdir):
                pass
            else: raise
    
    def add_from_remote(self):
        self.add()

    def snapshot_prechecks(self):
        pass


    def results_move_current_files(self, rel_dest_root, exclude_files,
                                   exclude_dirs_re):
        move_current_files_local_fs(self.name, self.local_path, rel_dest_root, exclude_files, exclude_dirs_re) 

    def snapshot(self):
        # rsrcdir = os.path.abspath(self.workspace_dir + 'resources/' + self.role + '/' + self.name)
        h = hashtree.generate_hashes(self.rsrcdir, self.local_path, ignore=self.ignore)
        assert os.path.exists(os.path.join(self.rsrcdir, h))
        if is_git_staging_dirty(self.workspace_dir, subdir=self.rsrcdir_relative):
            call_subprocess([GIT_EXE_PATH, 'commit', '-m',
                             "Add snapshot hash files for resource %s" % self.name],
                            cwd=self.workspace_dir, verbose=False)
        return h

    def add_results_file(self, temp_path, rel_dest_path):
        """Move a results file from the temporary location to
        the specified path in the resource.
        """
        assert self.role==ResourceRoles.RESULTS
        assert os.path.exists(temp_path)
        abs_dest_path = os.path.join(self.local_path, rel_dest_path)
        parent_dir = os.path.dirname(abs_dest_path)
        if not os.path.isdir(parent_dir):
            os.makedirs(parent_dir)
        os.rename(temp_path, abs_dest_path)

    def restore_prechecks(self, hashval):
        rc = hashtree.check_hashes(hashval, self.rsrcdir, self.local_path, ignore=self.ignore)
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

    def from_json_remote(self, json_data, workspace_dir, batch, verbose):
        """Instantiate a resource object from the parsed resources.json file"""
        assert json_data['resource_type']==LOCAL_FILE
        # XXX need to convert local path to be stored in local params
        return LocalFileResource(json_data['name'],
                                 json_data['role'], workspace_dir, json_data['local_path'])

    def suggest_name(self, local_path):
        return os.path.basename(local_path)

