# Copyright 2018,2019 by MPI-SWS and Data-ken Research. Licensed under Apache 2.0. See LICENSE.txt.
"""
File-related utilities
"""

import os
from os.path import dirname
import shutil

from dataworkspaces.errors import ConfigurationError

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


def safe_rename(src, dest):
    """Safe replacement for os.rename(). The problem is that os.rename()
    does not work across file systems. In that case, you need to actually
    copy the file
    """
    try:
        os.rename(src, dest)
    except OSError:
        try:
            shutil.copyfile(src, dest)
            os.remove(src)
        except Exception as e:
            raise ConfigurationError("Unable to copy %s to %s: %s"%
                                     (src, dest, e)) from e

