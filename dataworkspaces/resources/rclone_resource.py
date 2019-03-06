# Copyright 2018 by MPI-SWS and Data-ken Research. Licensed under Apache 2.0. See LICENSE.txt.
"""
Resource for files copied in by rclone 
"""
from errno import EEXIST
import os
import os.path 
import stat

from dataworkspaces.errors import ConfigurationError
from dataworkspaces.utils.subprocess_utils import call_subprocess
from dataworkspaces.utils.git_utils import GIT_EXE_PATH, is_git_staging_dirty
from .resource import Resource, ResourceFactory
from . import hashtree
from .snapshot_utils import move_current_files_local_fs

from . import rclone

LOCAL_FILE = 'rclone'

"""
dws add rclone [options] remote local

See 
"""
class RcloneResource(Resource):
    def __init__(self, name, role, workspace_dir, remote_origin, local_path, config=None, compute_hash=False, ignore=[], verbose=False):
        super().__init__(LOCAL_FILE, name, role, workspace_dir)
        (self.remote_name, rpath) = remote_origin.split(':')
        self.remote_path = os.path.abspath(rpath)
        self.remote_origin = self.remote_name + ':' + self.remote_path
        self.local_path = os.path.abspath(local_path)
        self.compute_hash = compute_hash
        self.ignore = ignore
        self.verbose = verbose
        self.config = config
        if config:
            self.rclone = rclone.RClone(cfgfile=self.config)
        else:
            self.rclone = rclone.RClone()

        self.rsrcdir = os.path.abspath(self.workspace_dir + '/.dataworkspace/' + LOCAL_FILE + '/' + self.role + '/' + self.name)
        self.rsrcdir_relative = '.dataworkspace/' + LOCAL_FILE + '/' + self.role + '/' + self.name

    def to_json(self):
        d = super().to_json()
        d['remote_origin'] = self.remote_origin
        d['local_path'] = self.local_path
        d['config'] = self.config
        d['compute_hash'] = self.compute_hash
        return d

    def local_params_to_json(self):
        return {'local_path' : self.local_path,
                'remote_origin' : self.remote_origin,
                'config' : self.config,
                'compute_hash' : self.compute_hash }

    def get_local_path_if_any(self):
        return self.local_path
    
    def add_prechecks(self):
        # if not(os.path.isdir(self.local_path)):
        #    raise ConfigurationError(self.local_path + ' does not exist')
        if os.path.exists(self.local_path) and not(os.access(self.local_path, os.W_OK)): 
            raise ConfigurationError(self.local_path + ' does not have write permission')
        if os.path.realpath(self.local_path)==os.path.realpath(self.workspace_dir):
            raise ConfigurationError("Cannot add the entire workspace as a file resource")
        known_remotes = self.rclone.listremotes()
        if self.remote_name not in known_remotes:
            raise ConfigurationError("Remote '" + self.remote_name + "' not found by rclone")
        
    def add(self):
        print("rclone: Add is called")
        self.add_from_remote()

    def add_from_remote(self):
        print("In rclone:add")
        if not (os.path.exists(self.local_path)):
            os.makedirs(self.local_path)
        if not (os.path.exists(self.rsrcdir)):
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
        ret = self.rclone.copy(self.remote_origin, self.local_path)
        if ret['code'] != 0:
            raise Exception("rclone copy raised error %d: %s" % (ret['code'], ret['err']))
        # mark the files as readonly
        print('Marking files as readonly')
        for (dirpath, dirnames, filenames) in os.walk(self.local_path):
            for f_name in filenames:
                abspath = os.path.abspath(os.path.join(dirpath, f_name))
                mode = os.stat(abspath)[stat.ST_MODE]
                os.chmod(abspath, mode & ~stat.S_IWUSR & ~stat.S_IWGRP & ~stat.S_IWOTH)

    def snapshot_prechecks(self):
        pass


    def results_move_current_files(self, rel_dest_root, exclude_files,
                                   exclude_dirs_re):
        move_current_files_local_fs(self.name, self.local_path, rel_dest_root, exclude_files, exclude_dirs_re) 

    def snapshot(self):
        # rsrcdir = os.path.abspath(self.workspace_dir + 'resources/' + self.role + '/' + self.name)
        # h = hashtree.generate_hashes(self.rsrcdir, self.local_path, ignore=self.ignore)
        # assert os.path.exists(os.path.join(self.rsrcdir, h))
        # if is_git_staging_dirty(self.workspace_dir, subdir=self.rsrcdir_relative):
        #     call_subprocess([GIT_EXE_PATH, 'commit', '-m',
        #                      "Add snapshot hash files for resource %s" % self.name],
        #                     cwd=self.workspace_dir, verbose=False)
        # return h
        print("In snapshot: ", self.remote_name,  self.remote_path, self.local_path)
        if self.compute_hash:
            (ret, out) = self.rclone.check(self.remote_origin, self.local_path, flags=['--one-way']) 
        else:
            (ret, out) = self.rclone.check(self.remote_origin, self.local_path, flags=['--one-way', '--size-only']) 
        print('Snapshot returns ', ret, out)
        return ret

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
        pass
        # rc = hashtree.check_hashes(hashval, self.rsrcdir, self.local_path, ignore=self.ignore)
        # if not rc:
        #     raise ConfigurationError("Local file structure not compatible with saved hash")

    def restore(self, hashval):
        pass # rclone-d files: do nothing to restore

    def __str__(self):
        return "Rclone-d repo %s, locally copied in %s in role '%s'" % (self.remote_origin, self.local_path, self.role)

class RcloneFactory(ResourceFactory):
    def from_command_line(self, role, name, workspace_dir, batch, verbose,
                          remote_path, local_path, config, compute_hash):
        """Instantiate a resource object from the add command's arguments"""
        return RcloneResource(name, role, workspace_dir, remote_path, local_path, config, compute_hash)

    def from_json(self, json_data, local_params, workspace_dir, batch, verbose):
        """Instantiate a resource object from the parsed resources.json file"""
        assert json_data['resource_type']==LOCAL_FILE
        return RcloneResource(json_data['name'],
                                 json_data['role'],  
                                 workspace_dir, json_data['remote_origin'], json_data['local_path'],
                                 json_data['config'], json_data['compute_hash'])

    def from_json_remote(self, json_data, workspace_dir, batch, verbose):
        """Instantiate a resource object from the parsed resources.json file"""
        assert json_data['resource_type']==LOCAL_FILE
        # XXX need to convert local path to be stored in local params
        return RcloneResource(json_data['name'],
                                 json_data['role'], 
                                 workspace_dir, json_data['remote_origin'], json_data['local_path'],
                                 json_data['config'], json_data['compute_hash'])

    def suggest_name(self, local_path, *args):
        return os.path.basename(local_path)

