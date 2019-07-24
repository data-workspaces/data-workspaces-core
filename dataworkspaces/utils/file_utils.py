# Copyright 2018,2019 by MPI-SWS and Data-ken Research. Licensed under Apache 2.0. See LICENSE.txt.
"""
File-related utilities
"""

import os
from os.path import dirname, isdir
import shutil

from dataworkspaces.errors import ConfigurationError

def remove_dir_if_empty(path:str, base_dir:str, verbose:bool=False) -> None:
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


def safe_rename(src:str, dest:str) -> None:
    """Safe replacement for os.rename(). The problem is that os.rename()
    does not work across file systems. In that case, you need to actually
    copy the file
    """
    try:
        os.rename(src, dest)
    except OSError:
        try:
            if isdir(src):
                shutil.copytree(src, dest)
                shutil.rmtree(src)
            else:
                shutil.copyfile(src, dest)
                os.remove(src)
        except Exception as e:
            raise ConfigurationError("Unable to copy %s to %s: %s"%
                                     (src, dest, e)) from e

