# Copyright 2018,2019 by MPI-SWS and Data-ken Research. Licensed under Apache 2.0. See LICENSE.txt.

import os
from os.path import curdir, dirname, abspath, expanduser, join, isdir, exists
import uuid
import shutil
import json

from dataworkspaces.errors import ConfigurationError, InternalError, UserAbort
from dataworkspaces.utils.git_utils import is_a_git_fat_repo, validate_git_fat_in_path
from dataworkspaces.resources.resource import \
    get_resource_file_path, get_resource_local_params_file_path
import dataworkspaces.commands.actions as actions
from .init import get_config_file_path
from .add import UpdateLocalParams, add_local_dir_to_gitignore_if_needed
from .pull import AddRemoteResource
from .params import get_local_defaults, get_local_params_file_path


def clone_command(repository, hostname, directory=None, batch=False, verbose=False):
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
        if is_a_git_fat_repo(directory):
            validate_git_fat_in_path()
            import dataworkspaces.third_party.git_fat as git_fat
            python2_exe = git_fat.find_python2_exe()
            git_fat.run_git_fat(python2_exe, ['init'], cwd=directory,
                                verbose=verbose)
            # pull the objects referenced by the current head
            git_fat.run_git_fat(python2_exe, ['pull'], cwd=directory,
                                verbose=verbose)
    except:
        if isdir(initial_path):
            shutil.rmtree(initial_path)
        if (directory is not None) and isdir(directory):
            shutil.rmtree(directory)
        raise

    # ok, now we have the main repo, so we can clone all the resources
    with open(get_local_params_file_path(directory), 'w') as f:
        json.dump(get_local_defaults(hostname), f, indent=2)
    with open(get_resource_local_params_file_path(directory), 'w') as f:
        json.dump({}, f, indent=2)
    resources_file = get_resource_file_path(directory)
    with open(resources_file, 'r') as f:
        resources_json = json.load(f)

    add_to_gi = None
    gitignore_path = None
    try:
        for resource_json in resources_json:
            add_remote_action = AddRemoteResource(ns, verbose, batch, directory, resource_json)
            plan.append(add_remote_action)
            plan.append(UpdateLocalParams(ns, verbose, add_remote_action.r, directory))
            add_to_gi = add_local_dir_to_gitignore_if_needed(ns, verbose, add_remote_action.r,
                                                             directory)
            if add_to_gi:
                plan.append(add_to_gi)
                gitignore_path = add_to_gi.gitignore_path
        if gitignore_path:
            plan.append(actions.GitAdd(ns, verbose, directory, [gitignore_path]))
            plan.append(actions.GitCommit(ns, verbose, directory, "Added new resources to gitignore"))
    except:
        # since we had to create the main repo before doing the sanity chacks
        # on the resources, we delete the main repo if there's an error.
        shutil.rmtree(directory)
        raise

    try:
        actions.run_plan(plan, "clone data workspace",
                         "cloned data workspace", batch=batch, verbose=verbose)
    except UserAbort:
        shutil.rmtree(directory)
        raise



