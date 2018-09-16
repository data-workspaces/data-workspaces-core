"""
Base classes for resoures
"""
import re
import json
import copy
from urllib.parse import urlparse
from os.path import expanduser, isdir, abspath, join, exists

from dataworkspaces.errors import InternalError

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


class Resource:
    """Base class for all resources"""
    def __init__(self, scheme, name, url, role, workspace_dir):
        self.scheme = scheme
        self.name = name
        self.url = url
        self.role = role
        self.workspace_dir = workspace_dir

    def to_json(self):
        """Return a json (unserialized) representation of this
        resource for the resources file.
        """
        # subclasses can call this and then add to the dict
        return {
            'resource_type': self.scheme,
            'name': self.name,
            'url': self.url,
            'role': self.role
        }

    def add_prechecks(self):
        pass

    def add(self):
        pass

    def snapshot_prechecks(self):
        pass

    def snapshot(self):
        pass

    def restore_prechecks(self, hashval):
        pass

    def restore(self, hashval):
        pass

    def __str__(self):
        return 'Resource %s in role %s' % (self.name, self.role)


class ResourceFactory:
    def from_command_line(self, role, name, workspace_dir, batch, verbose,
                          *args):
        """Instantiate a resource object from the add command's
        arguments"""
        pass

    def from_json(self, json_data, workspace_dir, batch, verbose):
        """Instantiate a resource object from the parsed resources.json file"""
        pass

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

class ResourceCollection:
    """A collection of resources, with a json representation.
    Base class for resources.json and snapshot manifests.
    """
    def __init__(self, resources_json_file, workspace_dir, batch, verbose):
        with open(resources_json_file, 'r') as f:
            self.json_data = json.load(f)
        self.resources = [get_resource_from_json(rdata, workspace_dir, batch, verbose)
                          for rdata in self.json_data]
        self.by_name = {r.name:r for r in self.resources}
        self.urls = set([r.url for r in self.resources])

    def is_a_current_name(self, name):
        return name in self.by_name

    def get_names(self):
        return set(self.by_name.keys())

    def add_resource(self, r):
        assert r.name not in self.by_name.keys()
        self.resources.append(r)
        self.json_data.append(r.to_json())
        self.by_name[r.name] = r
        self.urls.add(r.url)

class CurrentResources(ResourceCollection):
    """In-memory representation of resources.json - the list of resources in the
    workspace.
    """
    def __init__(self, resources_json_file, workspace_dir, batch, verbose):
        super().__init__(resources_json_file, workspace_dir, batch, verbose)
        self.json_file = resources_json_file

    def write_current_resources(self):
        with open(self.json_file, 'w') as f:
            json.dump(self.json_data, f, indent=2)

    def write_snapshot_manifest(self, snapshot_filepath, url_to_hashval):
        sn_json = copy.deepcopy(self.json_data)
        for rdata in sn_json:
            rdata['hash'] = url_to_hashval[rdata['url']]
        with open(snapshot_filepath, 'w') as f:
            json.dump(sn_json, f, indent=2)

    def __str__(self):
        resources = ['%s[%s]' % (rname, self.by_name[rname].url)
                     for rname in sorted(self.by_name.keys())]
        return 'CurrentResources(%s)' % ', '.join(resources)
        
    @staticmethod
    def read_current_resources(workspace_dir, batch, verbose):
        resource_file = join(workspace_dir, '.dataworkspace/resources.json')
        if not exists(resource_file):
            raise InternalError("Missing current resources file %s" % resource_file)
        return CurrentResources(resource_file, workspace_dir, batch, verbose)


class SnapshotResources(ResourceCollection):
    """In-memory represtation of a snapshot file. The JSON representation is
    the same as resources.json, but we add the hash for each resource.
    """
    def __init__(self, snapshot_json_file, workspace_dir, batch, verbose):
        super().__init__(snapshot_json_file, workspace_dir, batch, verbose)
        self.url_to_hashval = {rdata['url']:rdata['hash'] for rdata in self.json_data}
        self.json_file = snapshot_json_file
        self.resources_file = join(workspace_dir, '.dataworkspace/resources.json')

    def write_revised_snapshot_manifest(self, snapshot_filepath, url_to_hashval):
        sn_json = copy.deepcopy(self.json_data)
        for rdata in sn_json:
            # only new resources will need hash entries added
            if 'hash' not in rdata:
                hashval = url_to_hashval[rdata['url']]
                rdata['hash'] = hashval
                self.url_to_hashval[rdata['url']] = hashval
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
        filepath = join(workspace_dir, '.dataworkspace/snapshots/snapshot-%s.json'% snapshot_hash)
        if not exists(filepath):
            raise InternalError("Missing snapshot manifest file %s" % filepath)
        return SnapshotResources(filepath, workspace_dir, batch, verbose)


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

def get_resource_from_json(json_data, workspace_dir, batch, verbose):
    factory = get_factory_by_scheme(json_data['resource_type'])
    return factory.from_json(json_data, workspace_dir, batch, verbose)

