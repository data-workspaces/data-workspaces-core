from dataworkspaces.errors import ConfigurationError
from typing import Optional, Type
import os
from os.path import abspath, expanduser, isdir, join, curdir, dirname

def get_workspace(caller_workspace_arg:Optional[str]=None,
                  exc_type:Type[Exception]=ConfigurationError,
                  current_dir:Optional[str]=None) ->str:
    """For commands that execute in the context of a containing
    workspace, find the nearest containging workspace and return
    its absolute path. If the caller provides one, we validate it
    and return it. Otherwise, we search outward from the current directory.
    Throws an exception if the workspace was invalid
    or could not be found.
    """
    if caller_workspace_arg is not None:
        workspace_dir = abspath(expanduser(caller_workspace_arg))
        if not isdir(workspace_dir):
            raise exc_type("Workspace directory %s does not exist" %
                           workspace_dir)
        dws_dir = join(workspace_dir, '.dataworkspace')
        if not isdir(dws_dir) or not os.access(dws_dir, os.W_OK):
            raise exc_type("Provided directory for workspace %s has not been initialized as a data workspace" % workspace_dir)
        else:
            return workspace_dir
    else:
        if current_dir is not None:
            curr_dir_abs = abspath(expanduser(current_dir))
        else:
            curr_dir_abs = abspath(expanduser(curdir))
        curr_base = curr_dir_abs
        while curr_base != '/':
            if isdir(join(curr_base, '.dataworkspace')) and os.access(curr_base, os.W_OK):
                return curr_base
            else:
                curr_base = dirname(curr_base)
        raise exc_type("Cound not find an enclosing data workspace starting from %s"%
                       curr_dir_abs)
