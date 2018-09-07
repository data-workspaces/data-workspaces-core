
def register_resource_type(scheme, factory):
    """
    Register a ResourceFactory object for the specified scheme
    """
    from ..resources.resource import RESOURCE_TYPES, ResourceFactory
    if type(factory)==type:
        factory = factory()
    assert isinstance(factory, ResourceFactory)
    RESOURCE_TYPES[scheme] = factory

from ..resources.git_resource import GitRepoFactory
register_resource_type('git', GitRepoFactory)

from ..resources.local_file_resource import LocalFileFactory
register_resource_type('file', LocalFileFactory)
