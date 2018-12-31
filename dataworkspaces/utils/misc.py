"""
Miscellaneous utilities.
"""

import os
from os.path import dirname

def remove_dir_if_empty(path, base_dir, verbose=False):
    """Remove an empty directory and any parents that are empty,
    up to base_dir.
    """
    if path==base_dir:
        return
    elif len(os.listdir(path))==0:
        os.rmdir(path)
        if verbose:
            print("Removing (now) empty directory %s" % path)
        remove_dir_if_empty(dirname(path), base_dir, verbose)
