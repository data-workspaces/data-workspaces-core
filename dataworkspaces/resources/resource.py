"""
Base classes for resoures
"""
import re
import json
from urllib.parse import urlparse
from os.path import expanduser, isdir, abspath, join, exists

from dataworkspaces.errors import ConfigurationError

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

def read_current_resources(workspace_dir):
    rfile = join(workspace_dir, '.dataworkspace/resources.json')
    if not exists(rfile):
        raise ConfigurationError("Missing resources file at %s" % rfile)
    with open(rfile, 'r') as f:
        data = json.load(f)
    return data

def get_resource_names(resource_json_list):
    names = set()
    for r in resource_json_list:
        names.add(r['name'])
    return names


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

