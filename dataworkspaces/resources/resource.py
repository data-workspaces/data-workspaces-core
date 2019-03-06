# Copyright 2018,2019 by MPI-SWS and Data-ken Research. Licensed under Apache 2.0. See LICENSE.txt.
"""
Base classes for resoures
"""
import json
import copy
import os
from os.path import join, exists, abspath, expanduser, dirname, isdir, realpath
from tempfile import NamedTemporaryFile

import click

from dataworkspaces.errors import InternalError, ConfigurationError

class ResourceRoles:
    SOURCE_DATA_SET='source-data'
    INTERMEDIATE_DATA='intermediate-data'
    CODE='code'
    RESULTS='results'

RESOURCE_ROLE_CHOICES = [
    ResourceRoles.SOURCE_DATA_SET,
    ResourceRoles.INTERMEDIATE_DATA,
    ResourceRoles.CODE,
    ResourceRoles.RESULTS
]

# short explanation of each role
RESOURCE_ROLE_PURPOSES = {
    ResourceRoles.SOURCE_DATA_SET:"source data",
    ResourceRoles.INTERMEDIATE_DATA:"intermediate data",
    ResourceRoles.CODE:"code",
    ResourceRoles.RESULTS:"experimental results"
}


class Resource:
    """Base class for all resources"""
    def __init__(self, scheme, name, role, workspace_dir):
        self.scheme = scheme
        self.name = name
        self.role = role
        self.workspace_dir = workspace_dir

    def has_results_role(self):
        return self.role==ResourceRoles.RESULTS

    def to_json(self):
        """Return a json (unserialized) representation of this
        resource for the resources file.
        """
        # subclasses can call this and then add to the dict
        return {
            'resource_type': self.scheme,
            'name': self.name,
            'role': self.role
        }

    def local_params_to_json(self):
        """Return a dict of local parameters in a format that can be serialized directly
        to json. This will be saved in the file resource_local_params.json.
        """
        return {}

    def get_local_path_if_any(self):
        """If the resource has an associated local path on the system,
        return it. Othewise, return None. This is used for determining
        whether the local path is under the same directory
        as the data workspace. In that case, we add it to .gitignore
        """
        return None

    def add_prechecks(self):
        pass

    def add(self):
        pass

    def add_from_remote(self):
        pass

    def snapshot_prechecks(self):
        pass

    def validate_subpath_exists(self, subpath):
        """Validate that the subpath is valid within this
        resource. Default implementation checks the local
        filesystem if any. If the resource is remote-only,
        then the subclass should override this method to
        check on the remote side.
        """
        lp = self.get_local_path_if_any()
        if lp is not None:
            path = join(lp, subpath)
            if not isdir(path):
                raise ConfigurationError("Subpath %s does not exist for resource %s"%
                                         (subpath, self.name))

    def results_move_current_files(self, rel_dest_root, exclude_files,
                                   exclude_dirs_re):
        """If the resource has a result role, we want
        to move the current files into a subdirectory ahead
        of taking a snapshot.

        rel_dest_root is the relative path within the resource for
        a directory to be created and the files moved.

        exclude_files is a (possibly empty) set of relative
        file paths to exclude from the move (e.g. an additive
        results file).

        exclude_dirs_re is a regular expression for relative file paths
        used to exclude the directories of prior checkpoints.
        """
        pass

    def add_results_file(self, temp_path, rel_dest_path):
        """Move a results file from the temporary location to
        the specified path in the resource.
        """
        raise NotImplementedError(self.__class__.__name__)

    def add_results_file_from_buffer(self, str_buffer, rel_dest_path):
        """Convenience function which saves the buffer as a temporary
        file, calls add_results_file() and handles cleaning up
        the temp file in case of an error.
        """
        tfname = None
        try:
            with NamedTemporaryFile(mode='w', delete=False) as tf:
                tfname = tf.name
                tf.write(str_buffer)
            self.add_results_file(tfname, rel_dest_path)
        finally:
            if tfname is not None and exists(tfname):
                os.remove(tfname)

    def snapshot(self):
        pass

    def restore_prechecks(self, hashval):
        pass

    def restore(self, hashval):
        pass

    def push_prechecks(self):
        pass

    def push(self):
        """Push to remote origin, if any"""
        pass

    def pull_prechecks(self):
        pass

    def pull(self):
        """Pull from remote origin, if any"""
        pass

    def __str__(self):
        return 'Resource %s in role %s' % (self.name, self.role)



class ResourceFactory:
    def from_command_line(self, role, name, workspace_dir, batch, verbose,
                          *args):
        """Instantiate a resource object from the add command's
        arguments"""
        pass

    def from_json(self, json_data, local_params, workspace_dir, batch, verbose):
        """Instantiate a resource object from the parsed resources.json file"""
        pass

    def from_json_remote(self, json_data, workspace_dir, batch, verbose):
        """Instantiate a resource object from the parsed resources.json file
        that came from the remote origin. We don't yet have local params,
        since this resource is not yet on the local machine. If not in batch
        mode, this method can ask the user for any additional information needed
        (e.g. a local path). In batch mode, should either come up with a reasonable
        default or error out if not enough information is available."""
        raise InternalError("from_json_remote() not implemented for %s" % self.__class__.__name__)

    def suggest_name(self, *args):
        """Given the arguments passed in to create a resource,
        suggest a name for the case where the user did not provide one
        via --name. This will be used by suggest_resource_name() to
        find a short, but unique name for the resource.
        """
        pass

# Mapping from resource type name (e.g. file, git, s3) to ResourceFactory
# Registered via dataworkspaces.resources.register_resource_types
RESOURCE_TYPES = {
    
}

def get_resource_file_path(workspace_dir):
    return join(workspace_dir, '.dataworkspace/resources.json')

def get_resource_local_params_file_path(workspace_dir):
    return join(workspace_dir, '.dataworkspace/resource_local_params.json')

def get_snapshot_hash_file_path(workspace_dir, snapshot_hash):
    return join(workspace_dir,
                '.dataworkspace/snapshots/snapshot-%s.json'% snapshot_hash)
class ResourceCollection:
    """A collection of resources, with a json representation.
    Base class for resources.json and snapshot manifests.
    """
    def __init__(self, resources_json_file, local_params_file, workspace_dir, batch, verbose):
        with open(resources_json_file, 'r') as f:
            self.json_data = json.load(f)
        if local_params_file is not None:
            with open(local_params_file, 'r') as f:
                self.local_params = json.load(f)
        else:
            self.local_params = {}
        def get_local_params(rname):
            return self.local_params[rname] if rname in self.local_params else {}
        self.resources = [get_resource_from_json(rdata,
                                                 get_local_params(rdata['name']),
                                                 workspace_dir, batch, verbose)
                          for rdata in self.json_data]
        self.by_name = {r.name:r for r in self.resources}

    def is_a_current_name(self, name):
        return name in self.by_name

    def get_names(self):
        return set(self.by_name.keys())

    def add_resource(self, r):
        assert r.name not in self.by_name.keys()
        self.resources.append(r)
        self.json_data.append(r.to_json())
        self.by_name[r.name] = r

class CurrentResources(ResourceCollection):
    """In-memory representation of resources.json - the list of resources in the
    workspace.
    """
    def __init__(self, resources_json_file, local_params_file, workspace_dir, batch, verbose):
        super().__init__(resources_json_file, local_params_file,
                         workspace_dir, batch, verbose)
        self.json_file = resources_json_file

    def write_current_resources(self):
        with open(self.json_file, 'w') as f:
            json.dump(self.json_data, f, indent=2)

    def write_snapshot_manifest(self, snapshot_filepath, name_to_hashval):
        sn_json = copy.deepcopy(self.json_data)
        for rdata in sn_json:
            rdata['hash'] = name_to_hashval[rdata['name']]
        with open(snapshot_filepath, 'w') as f:
            json.dump(sn_json, f, indent=2)

    def map_local_path_to_resource(self, path):
        """Given a path on the local filesystem, map it to
        a resource and the path within the resource.
        Raises ConfigurationError if no match is found.
        """
        if path.endswith('/') and len(path)>0:
            path = path[0:-1] # remove trailing slash
        rp = realpath(abspath(expanduser(path)))
        for r in self.resources:
            lp = r.get_local_path_if_any()
            if lp is None:
                continue
            if lp.endswith('/') and len(lp)>0:
                lp = lp[0:-1] # remove trailing slash
            rlp = realpath(lp)
            if rp==rlp or path==lp:
                return (r.name, None)
            elif path.startswith(lp):
                return (r.name, path[len(lp)+1:])
            elif rp.startswith(rlp):
                return (r.name, rp[len(rlp)+1:])
        raise ConfigurationError("Did not find a resource corresponding to local path %s"%
                                 path)

    def validate_resource_name(self, resource_name, subpath=None):
        if resource_name not in self.by_name:
            raise ConfigurationError("No resource named '%s'" % resource_name)
        if subpath is None:
            return
        else:
            self.by_name[resource_name].validate_subpath_exists(subpath)

    def __str__(self):
        resources = sorted(self.by_name.keys())
        return 'CurrentResources(%s)' % ', '.join(resources)

    @staticmethod
    def read_current_resources(workspace_dir, batch, verbose):
        resource_file = get_resource_file_path(workspace_dir)
        if not exists(resource_file):
            raise InternalError("Missing current resources file %s" % resource_file)
        local_params_file = get_resource_local_params_file_path(workspace_dir)
        if not exists(local_params_file):
            raise InternalError("Missing resource local params file %s" %
                                local_params_file)
        return CurrentResources(resource_file, local_params_file, workspace_dir, batch, verbose)


class SnapshotResources(ResourceCollection):
    """In-memory represtation of a snapshot file. The JSON representation is
    the same as resources.json, but we add the hash for each resource.
    """
    def __init__(self, snapshot_json_file, local_params_file,
                 workspace_dir, batch, verbose):
        super().__init__(snapshot_json_file, local_params_file,
                         workspace_dir, batch, verbose)
        self.name_to_hashval = {rdata['name']:rdata['hash'] for rdata in self.json_data}
        self.json_file = snapshot_json_file
        self.resources_file = get_resource_file_path(workspace_dir)

    def write_revised_snapshot_manifest(self, snapshot_filepath, name_to_hashval):
        sn_json = copy.deepcopy(self.json_data)
        for rdata in sn_json:
            # only new resources will need hash entries added
            if 'hash' not in rdata:
                hashval = name_to_hashval[rdata['name']]
                rdata['hash'] = hashval
                self.name_to_hashval[rdata['name']] = hashval
        with open(snapshot_filepath, 'w') as f:
            json.dump(sn_json, f, indent=2)

    def write_current_resources(self):
        """Write out a resources.json file corresponding to
           the resources in the snapshot."""
        r_json = copy.deepcopy(self.json_data)
        for rdata in r_json:
            if 'hash' in rdata:
                del rdata['hash']
        with open(self.resource_file, 'w') as f:
            json.dump(r_json, f, indent=2)

    @staticmethod
    def read_shapshot_manifest(snapshot_hash, workspace_dir, batch, verbose):
        filepath = get_snapshot_hash_file_path(workspace_dir, snapshot_hash)
        if not exists(filepath):
            raise InternalError("Missing snapshot manifest file %s" % filepath)
        local_params_file = get_resource_local_params_file_path(workspace_dir)
        if not exists(local_params_file):
            raise InternalError("Missing resource local params file %s"%
                                local_params_file)
        return SnapshotResources(filepath, local_params_file,
                                 workspace_dir, batch, verbose)


def get_factory_by_scheme(scheme):
    if scheme not in RESOURCE_TYPES:
        raise InternalError("'%s' not a valid resource type. Valid types are: %s."
                            % (scheme, ', '.join(sorted(RESOURCE_TYPES.keys()))))
    return RESOURCE_TYPES[scheme]


def suggest_resource_name(scheme, role, existing_resource_names, *args):
    name = get_factory_by_scheme(scheme).suggest_name(*args)
    if name not in existing_resource_names:
        return name
    longer_name = name + '-' + role
    if longer_name not in existing_resource_names:
        return longer_name
    i = 2
    while True:
        numbered_name = longer_name + '-' + str(i)
        if numbered_name not in existing_resource_names:
            return numbered_name
        i += 1



def get_resource_from_command_line(scheme, role, name,
                                   workspace_dir, batch, verbose, *args):
    factory = get_factory_by_scheme(scheme)
    return factory.from_command_line(role, name, workspace_dir, batch, verbose,
                                     *args)

def get_resource_from_json(json_data, local_params, workspace_dir, batch, verbose):
    factory = get_factory_by_scheme(json_data['resource_type'])
    return factory.from_json(json_data, local_params, workspace_dir, batch, verbose)

def get_resource_from_json_remote(json_data, workspace_dir, batch, verbose):
    factory = get_factory_by_scheme(json_data['resource_type'])
    return factory.from_json_remote(json_data, workspace_dir, batch, verbose)


class LocalPathType(click.Path):
    """A subclass of click's Path input parameter type used to validate a local path
    where we are going to put a resource. The path does not necessarily exist yet, but
    we need to validate that the parent directory exists and is writable.
    """
    def __init__(self):
        super().__init__(exists=False, file_okay=False, dir_okay=True, writable=True)

    def convert(self, value, param, ctx):
        rv = super().convert(value, param, ctx)
        parent = dirname(rv)
        if not exists(parent):
            self.fail('%s "%s" does not exist.' % (self.path_type, parent), param, ctx)
        if not isdir(parent):
            self.fail('%s "%s" is a file.' % (self.path_type, parent), param, ctx)
        if not os.access(parent, os.W_OK):
            self.fail('%s "%s" is not writable.' % (self.path_type, parent), param, ctx)
        return abspath(expanduser(rv))

