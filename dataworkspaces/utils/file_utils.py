# Copyright 2018,2019 by MPI-SWS and Data-ken Research. Licensed under Apache 2.0. See LICENSE.txt.
"""
File-related utilities
"""

import os
from os.path import dirname, isdir, abspath, expanduser, exists, isabs, commonpath, isfile, join
import shutil
import click
from typing import Optional

from dataworkspaces.errors import ConfigurationError


def remove_dir_if_empty(path: str, base_dir: str, verbose: bool = False) -> None:
    """Remove an empty directory and any parents that are empty,
    up to base_dir.
    """
    if path == base_dir:
        return
    elif len(os.listdir(path)) == 0:
        os.rmdir(path)
        if verbose:
            print("Removing (now) empty directory %s" % path)
        remove_dir_if_empty(dirname(path), base_dir, verbose)


def safe_rename(src: str, dest: str) -> None:
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
            raise ConfigurationError("Unable to copy %s to %s: %s" % (src, dest, e)) from e


def get_subpath_from_absolute(absolute_parent_path: str, absolute_child_path: str) -> Optional[str]:
    """Given two absolute paths where one is the parent of the other, return the
    child path as a relative path from the parent. Returns None if the paths are
    equal and raises a ValueError if absolute_child_path is not actually a
    parent of absolute_parent_path.
    """
    assert isabs(absolute_parent_path)
    assert isabs(absolute_child_path)
    if absolute_parent_path.endswith("/") and len(absolute_parent_path) > 1:
        absolute_parent_path = absolute_parent_path[:-1]
    if absolute_child_path.endswith("/") and len(absolute_child_path) > 1:
        absolute_child_path = absolute_child_path[:-1]
    if absolute_child_path == absolute_parent_path:
        return None
    if commonpath([absolute_parent_path, absolute_child_path]) != absolute_parent_path:
        raise ValueError(
            "'%s is not a subpath of '%s'" % (absolute_child_path, absolute_parent_path)
        )
    else:
        return absolute_child_path[len(absolute_parent_path) + 1 :]


def does_subpath_exist(
    base_dir: str, subpath: str, must_be_file: bool = False, must_be_directory: bool = False
) -> bool:
    path = join(base_dir, subpath)
    if isfile(path) and (not must_be_directory):
        return True  # includes links
    elif isdir(path) and not (must_be_file):
        return True
    else:
        return False  # does not exist, but a special file


def parent_path(path):
    """Return the path to the parent directory of path"""
    return abspath(join(path, os.pardir))


class LocalPathType(click.Path):
    """A subclass of click's Path input parameter type used to validate a local path
    where we are going to put a resource. The path does not necessarily exist yet, but
    we need to validate that the parent directory exists and is writable.

    If must_be_outside_of_workspace is set to the workspace directory, we validate
    that the path is outside of the workspace.

    If allow_multiple_levels_of_missing_dirs is True, we go up the tree to find the
    first parent in the hierarchy that exists and check its permissions. This works
    in cases where we're going to use os.makedirs() to create multiple levels of
    the hierarchy.
    """

    def __init__(
        self,
        exists: bool = False,
        must_be_outside_of_workspace: Optional[str] = None,
        allow_multiple_levels_of_missing_dirs: bool = False,
    ):
        super().__init__(
            exists=exists,
            file_okay=False,
            dir_okay=True,
            writable=True,
            # always use unicode, not bytes
            path_type=str,
        )
        self.must_be_outside_of_workspace = must_be_outside_of_workspace
        self.allow_multiple_levels_of_missing_dirs = allow_multiple_levels_of_missing_dirs

    def convert(self, value, param, ctx):
        rv = abspath(expanduser(super().convert(value, param, ctx)))
        if isdir(rv):
            parent_dir = rv
        else:
            parent_dir = parent_path(rv)
        if self.allow_multiple_levels_of_missing_dirs:
            while not isdir(parent_dir) and parent_dir != "/":
                parent_dir = parent_path(parent_dir)
        if not exists(parent_dir):
            self.fail('%s "%s" does not exist.' % (self.path_type, parent_dir), param, ctx)  # type: ignore
        if not isdir(parent_dir):
            self.fail('%s "%s" is a file.' % (self.path_type, parent_dir), param, ctx)  # type: ignore
        if not os.access(parent_dir, os.W_OK):
            self.fail('%s "%s" is not writable.' % (self.path_type, parent_dir), param, ctx)  # type: ignore
        if (self.must_be_outside_of_workspace is not None) and commonpath(
            [self.must_be_outside_of_workspace, rv]
        ) in (self.must_be_outside_of_workspace, rv):
            self.fail(
                '%s must be outside of workspace "%s"'
                % (self.path_type, self.must_be_outside_of_workspace),  # type: ignore
                param,
                ctx,
            )
        return rv
