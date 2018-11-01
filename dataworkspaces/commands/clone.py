
import os
from os.path import curdir, dirname, abspath, expanduser, join, isdir, exists
import uuid
import shutil
import json

import click

from dataworkspaces.errors import ConfigurationError, InternalError
import dataworkspaces.commands.actions as actions
from .init import get_config_file_path
from .pull import AddRemoteResource


def clone_command(repository, directory=None, batch=False, verbose=False):
    plan = []
    ns = actions.Namespace()

    # initial checks on the directory
    if directory:
        directory = abspath(expanduser(directory))
        parent_dir = dirname(directory)
        if isdir(directory):
            raise ConfigurationError("Clone target directory '%s' already exists"% directory)
        initial_path = directory
    else:
        parent_dir = abspath(expanduser(curdir))
        initial_path = join(parent_dir, uuid.uuid4().hex) # get a unique name within this directory
    if not isdir(parent_dir):
        raise ConfigurationError("Parent directory '%s' does not exist" % parent_dir)
    if not os.access(parent_dir, os.W_OK):
        raise ConfigurationError("Unable to write into directory '%s'" % parent_dir)

    # ping the remote repo
    cmd = [actions.GIT_EXE_PATH, 'ls-remote', '--quiet', repository]
    try:
        actions.call_subprocess(cmd, parent_dir, verbose)
    except Exception as e:
        raise ConfigurationError("Unable to access remote repository '%s'" % repository) from e

    # we have to clone the repo first to find out its name!
    try:
        cmd = [actions.GIT_EXE_PATH, 'clone', repository, initial_path]
        actions.call_subprocess(cmd, parent_dir, verbose)
        config_file = get_config_file_path(initial_path)
        if not exists(config_file):
            raise ConfigurationError("Did not find configuration file in remote repo")
        with open(config_file, 'r') as f:
            config_json = json.load(f)
        if 'name' not in config_json:
            raise InternalError("Missing 'name' property in configuration file")
        workspace_name = config_json['name']
        if directory is None:
            new_name = join(parent_dir, workspace_name)
            if isdir(new_name):
                raise ConfigurationError("Clone target directory %s already exists" % new_name)
            os.rename(initial_path, new_name)
            directory = new_name
    except:
        if isdir(initial_path):
            shutil.rmtree(initial_path)
        if (directory is not None) and isdir(directory):
            shutil.rmtree(directory)
        raise
    





