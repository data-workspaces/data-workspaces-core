"""
Base classes for resoures
"""
import re
from urllib.parse import urlparse
from os.path import expanduser, isdir, abspath

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

class Resource:
    """Base class for all resources"""
    def __init__(self, scheme, url, role, workspace_dir):
        self.scheme = scheme
        self.url = url
        self.role = role
        self.workspace_dir = workspace_dir

    def to_json(self):
        """Return a json (unserialized) representation of this
        resource for the resources file.
        """
        pass

    def add_prechecks(self):
        pass

    def add(self):
        pass

    def snapshot_prechecks(self):
        pass

    def snapshot(self):
        pass

    def __str__(self):
        return 'Resource %s in role %s' % (self.url, self.role)


class ResourceFactory:
    def from_command_line(self, role, workspace_dir, batch, verbose,
                          *args):
        """Instantiate a resource object from the add command's
        arguments"""
        pass

    def from_json(self, json_data, workspace_dir, batch, verbose):
        """Instantiate a resource object from the parsed resources.json file"""
        pass

# Mapping from resource type name (e.g. file, git, s3) to ResourceFactory
# Registered via dataworkspaces.resources.register_resource_types
RESOURCE_TYPES = {
    
}


def get_resource_from_command_line(scheme, role, workspace_dir, batch, verbose, *args):
    if scheme not in RESOURCE_TYPES:
        # should have been caught in command line processing
        raise InternalError("'%s' not a valid resource type. Valid types are: %s."
                            % (scheme, ', '.join(sorted(RESOURCE_TYPES.keys()))))
    factory = RESOURCE_TYPES[scheme]
    return factory.from_command_line(role, workspace_dir, batch, verbose, *args)



