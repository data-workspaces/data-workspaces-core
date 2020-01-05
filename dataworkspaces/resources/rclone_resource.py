# Copyright 2018 by MPI-SWS and Data-ken Research. Licensed under Apache 2.0. See LICENSE.txt.
"""
Resource for files copied in by rclone 
"""
import os
import os.path 
import stat
from typing import Tuple, List, Set, Pattern, Optional, Union
import json
import shutil

from dataworkspaces.errors import ConfigurationError
from dataworkspaces.workspace import Workspace, Resource, LocalStateResourceMixin,\
    FileResourceMixin, SnapshotResourceMixin, JSONDict, JSONList,\
    ResourceRoles, ResourceFactory
from dataworkspaces.utils.snapshot_utils import move_current_files_local_fs
from dataworkspaces.utils.file_utils import does_subpath_exist
from dataworkspaces.third_party.rclone import RClone

RCLONE_RESOURCE_TYPE = 'rclone'

"""
dws add rclone [options] remote local

See 
"""


class RcloneResource(Resource, LocalStateResourceMixin, FileResourceMixin, SnapshotResourceMixin):
    def __init__(self, name:str, role:str, workspace:Workspace,
                 remote_origin:str, local_path:str, config:Optional[str]=None,
                 compute_hash:bool=False, ignore:List[str]=[], verbose:bool=False):
        super().__init__(RCLONE_RESOURCE_TYPE, name, role, workspace)
        (self.remote_name, rpath) = remote_origin.split(':')
        self.remote_path = os.path.abspath(rpath)
        self.remote_origin = self.remote_name + ':' + self.remote_path
        self.local_path = os.path.abspath(local_path)
        self.compute_hash = compute_hash
        self.ignore = ignore
        self.verbose = verbose
        self.config = config
        if config:
            self.rclone = RClone(cfgfile=self.config)
        else:
            self.rclone = RClone()

    def get_params(self) -> JSONDict:
        return {
            'resource_type':self.resource_type,
            'name':self.name,
            'role':self.role,
            'remote_origin':self.remote_origin,
            'local_path':self.local_path,
            'config':self.config,
            'compute_hash':self.compute_hash
        }

    def get_local_path_if_any(self) -> str:
        return self.local_path

    def results_move_current_files(self, rel_dest_root:str, exclude_files:Set[str],
                                   exclude_dirs_re:Pattern) -> None:
        move_current_files_local_fs(self.name, self.local_path, rel_dest_root,
                                    exclude_files, exclude_dirs_re,
                                    verbose=self.workspace.verbose)

    def add_results_file(self, data:Union[JSONDict,JSONList], rel_dest_path:str) -> None:
        """save JSON results data to the specified path in the resource.
        """
        assert self.role==ResourceRoles.RESULTS
        abs_dest_path = os.path.join(self.local_path, rel_dest_path)
        parent_dir = os.path.dirname(abs_dest_path)
        if not os.path.isdir(parent_dir):
            os.makedirs(parent_dir)
        with open(abs_dest_path, 'w') as f:
            json.dump(data, f, indent=2)

    def does_subpath_exist(self, subpath:str, must_be_file:bool=False,
                           must_be_directory:bool=False) -> bool:
        return does_subpath_exist(self.local_path, subpath, must_be_file,
                                  must_be_directory)

    def read_results_file(self, subpath:str) -> Union[JSONDict,JSONList]:
        """Read and parse json results data from the specified path
        in the resource. If the path does not exist or is not a file
        throw a ConfigurationError.
        """
        path = os.path.join(self.local_path, subpath)
        if not os.path.isfile(path):
            raise ConfigurationError("subpath %s does not exist or is not a file in resource %s"%
                                     (subpath, self.name))
        with open(path, 'r') as f:
            try:
                return json.load(f)
            except Exception as e:
                raise ConfigurationError("Parse error when reading %s in resource %s"
                                         %(subpath, self.name)) from e

    def upload_file(self, src_local_path:str,
                    rel_dest_path:str) -> None:
        """Copy a local file to the specified path in the
        resource. This may be a local copy or an upload, depending
        on the resource implmentation
        """
        abs_dest_path = os.path.join(self.local_path, rel_dest_path)
        parent_dir = os.path.dirname(abs_dest_path)
        if not os.path.exists(src_local_path):
            raise ConfigurationError("Source file %s does not exist." % src_local_path)
        if not os.path.isdir(parent_dir):
            os.makedirs(parent_dir)
        shutil.copyfile(src_local_path, rel_dest_path)

    def get_local_params(self) -> JSONDict:
        return {} # TODO: local filepath can override global path

    def pull_precheck(self) -> None:
        """Nothing to do, since we donot support sync.
        TODO: Support pulling from remote
        """
        pass

    def pull(self) -> None:
        """Nothing to do, since we donot support sync.
        TODO: Support pulling from remote
        """
        pass

    def push_precheck(self) -> None:
        """Nothing to do, since we donot support sync.
        """
        pass

    def push(self) -> None:
        """Nothing to do, since we donot support sync.
        """
        pass


        
    def add(self):
        print("rclone: Add is called")
        self.add_from_remote()



    def snapshot_precheck(self) -> None:
        pass

    def snapshot(self) -> Tuple[Optional[str], Optional[str]]:
        if self.workspace.verbose:
            print("In snapshot: ", self.remote_name,  self.remote_path, self.local_path)
        if self.compute_hash:
            (ret, out) = self.rclone.check(self.remote_origin, self.local_path, flags=['--one-way']) 
        else:
            (ret, out) = self.rclone.check(self.remote_origin, self.local_path, flags=['--one-way', '--size-only']) 
        print('Snapshot returns ', ret, out)
        return (ret, None) # None for the restore hash since we cannot restore

    def restore_precheck(self, hashval):
        pass
        # rc = hashtree.check_hashes(hashval, self.rsrcdir, self.local_path, ignore=self.ignore)
        # if not rc:
        #     raise ConfigurationError("Local file structure not compatible with saved hash")

    def restore(self, hashval):
        pass # rclone-d files: do nothing to restore

    def delete_snapshot(self, workspace_snapshot_hash:str, resource_restore_hash:str,
                        relative_path:str) -> None:
        snapshot_dir_path = os.path.join(self.local_path, relative_path)
        if os.path.isdir(snapshot_dir_path):
            if self.workspace.verbose:
                print("Deleting snapshot directory %s from resource %s" %
                      (relative_path, self.name))
            shutil.rmtree(snapshot_dir_path)

    def validate_subpath_exists(self, subpath:str) -> None:
        super().validate_subpath_exists(subpath)

    def __str__(self):
        return "Rclone-d repo %s, locally copied in %s in role '%s'" %\
                (self.remote_origin, self.local_path, self.role)

class RcloneFactory(ResourceFactory):
    def _add_prechecks(self, local_path, remote_path, config) -> RClone:
        if os.path.exists(local_path) and not(os.access(local_path, os.W_OK)): 
            raise ConfigurationError(local_path + ' does not have write permission')
        if config:
            rclone = RClone(cfgfile=config)
        else:
            rclone = RClone()
        known_remotes = rclone.listremotes()
        (remote_name, _) = remote_path.split(":")
        if remote_name not in known_remotes:
            raise ConfigurationError("Remote '" + remote_name + "' not found by rclone")
        return rclone

    def _copy_from_remote(self, local_path, remote_origin, rclone):
        if not (os.path.exists(local_path)):
            os.makedirs(local_path)
        ret = rclone.copy(remote_origin, local_path)
        if ret['code'] != 0:
            raise ConfigurationError("rclone copy raised error %d: %s" % (ret['code'], ret['err']))
        # mark the files as readonly
        print('Marking files as readonly')
        for (dirpath, dirnames, filenames) in os.walk(local_path):
            for f_name in filenames:
                abspath = os.path.abspath(os.path.join(dirpath, f_name))
                mode = os.stat(abspath)[stat.ST_MODE]
                os.chmod(abspath, mode & ~stat.S_IWUSR & ~stat.S_IWGRP & ~stat.S_IWOTH)

    def from_command_line(self, role, name, workspace,
                          remote_path, local_path, config, compute_hash):
        rclone = self._add_prechecks(local_path, remote_path, config)
        self._copy_from_remote(local_path, remote_path, rclone)
        return RcloneResource(name, role, workspace, remote_path, local_path, config,
                              compute_hash)

    def from_json(self, params:JSONDict, local_params:JSONDict,
                  workspace:Workspace) -> RcloneResource:
        """Instantiate a resource object from the parsed resources.json file"""
        assert params['resource_type']==RCLONE_RESOURCE_TYPE
        return RcloneResource(params['name'],
                                 params['role'],  
                                 workspace,  params['remote_origin'], params['local_path'],
                                 params['config'], params['compute_hash'])

    def has_local_state(self) -> bool:
        return True

    def clone(self, params:JSONDict, workspace:Workspace) -> LocalStateResourceMixin:
        """Instantiate a resource that was created remotely. In this case, we will
        copy from the remote origin.
        """
        local_path = params['local_path']
        remote_origin = params['remote_origin']
        config = params['config']
        rclone = self._add_prechecks(local_path, remote_origin, config)
        self._copy_from_remote(local_path, remote_origin, rclone)
        return RcloneResource(params['name'], params['role'], workspace, remote_origin, local_path, config,
                              params['compute_hash'])



    def suggest_name(self, role, local_path, compute_hash):
        return os.path.basename(local_path)

