# Copyright 2018,2019 by MPI-SWS and Data-ken Research. Licensed under Apache 2.0. See LICENSE.txt.

from os.path import isabs, join, exists
import json

import click

from dataworkspaces.errors import ConfigurationError, InternalError
import dataworkspaces.commands.actions as actions
from dataworkspaces.resources.resource import get_resource_from_command_line,\
    suggest_resource_name, CurrentResources, get_resource_local_params_file_path
from dataworkspaces.utils.git_utils import validate_git_fat_in_path_if_needed



class AddResource(actions.Action):
    def __init__(self, ns, verbose, resource):
        super().__init__(ns, verbose)
        self.resource = resource
        self.resource.add_prechecks()

    def run(self):
        self.resource.add()

    def __str__(self):
        return "Run add actions for %s" % str(self.resource)


class AddResourceToFile(actions.Action):
    def __init__(self, ns, verbose, resource, current_resources):
        super().__init__(ns, verbose)
        self.resource = resource
        self.current_resources = current_resources
        # A given resource should resolve to a unique name, so this is the best way
        # to check for duplication.
        if current_resources.is_a_current_name(resource.name):
            raise ConfigurationError("Resource '%s' already in workspace" %
                                     resource.name)

    def run(self):
        self.current_resources.add_resource(self.resource)
        self.current_resources.write_current_resources()

    def __str__(self):
        return "Add '%s' to resources.json file" % str(self.resource)

class UpdateLocalParams(actions.Action):
    def __init__(self, ns, verbose, resource, workspace_dir):
        self.rname = resource.name
        self.local_params_fpath = get_resource_local_params_file_path(workspace_dir)
        if not exists(self.local_params_fpath):
            raise InternalError("Missing file %s" % self.local_params_fpath)
        self.local_params_for_resource = resource.local_params_to_json()

    def run(self):
        """We have to load and update the file at run time rather than at init type.
        Otherwise, if we are updating multiple resources (e.g. for the clone command),
        we will write over earlier resources with the later resources. By reading and
        updating for each resource, we ensure we are always updating based on the latest
        version.

        TODO: A more efficient implementation might gather up all the changes in a
        namespace variable and apply them in a separate action at the end.
        """
        with open(self.local_params_fpath, 'r') as f:
            local_params_data = json.load(f)
        local_params_data[self.rname] = self.local_params_for_resource
        with open(self.local_params_fpath, 'w') as f:
            json.dump(local_params_data, f, indent=2)

    def __str__(self):
        return "Add local parameters for %s to %s" % \
            (self.rname, self.local_params_fpath)
class AddResourceToGitIgnore(actions.Action):
    """The resource is under the workspace's git repo, so we add it to
    .gitignore
    """
    def __init__(self, ns, verbose, local_relpath, gitignore_path):
        super().__init__(ns, verbose)
        self.local_repath = '/'+local_relpath if not local_relpath.startswith('/') \
                            else local_relpath
        self.gitignore_path = gitignore_path

    def run(self):
        with open(self.gitignore_path, 'a') as f:
            f.write(self.local_repath + '\n')

    def __str__(self):
        return "Add '%s' to .gitignore" % self.local_repath


def add_local_dir_to_gitignore_if_needed(ns, verbose, resource, workspace_dir):
    """Figure out whether resource has a local path under the workspace's
    git repo, which needs to be added to .gitignore. If so, return an
    action instance which will do it.
    """
    if resource.scheme=='git-subdirectory':
        return None # this is always a part of the dataworkspace's repo
    local_path = resource.get_local_path_if_any()
    if local_path is None:
        return None
    assert isabs(local_path), "Resource local path should be absolute"
    if not local_path.startswith(workspace_dir):
        return None
    local_relpath = resource.get_local_path_if_any()[len(workspace_dir)+1:]
    if not local_relpath.endswith('/'):
        local_relpath_noslash = local_relpath
        local_relpath = local_relpath + '/'
    else:
        local_relpath_noslash = local_relpath[:-1]
    # Add a / as the start to indicate that the path starts at the root of the repo.
    # Otherwise, we'll hit cases where the path could match other directories (e.g. issue #11)
    local_relpath = '/'+local_relpath if not local_relpath.startswith('/') else local_relpath
    local_relpath_noslash = '/'+local_relpath_noslash \
                            if not local_relpath_noslash.startswith('/') \
                            else local_relpath_noslash
    gitignore_path = join(workspace_dir, '.gitignore')
    # read the gitignore file to see if relpath is already there
    if exists(gitignore_path):
        with open(gitignore_path, 'r') as f:
            for line in f:
                line = line.rstrip()
                if line==local_relpath or line==local_relpath_noslash:
                    return None # no need to add
    return AddResourceToGitIgnore(ns, verbose, local_relpath, gitignore_path)


def add_command(scheme, role, name, workspace_dir, batch, verbose, *args):
    current_resources = CurrentResources.read_current_resources(workspace_dir, batch, verbose)
    current_names = current_resources.get_names()
    if batch:
        if name==None:
            name = suggest_resource_name(scheme, role, current_names,
                                         *args)
        else:
            if name in current_names:
                raise ConfigurationError("Resource name '%s' already in use"%
                                         name)
    else:
        suggested_name = None
        while (name is None) or (name in current_names):
            if suggested_name==None:
                suggested_name = suggest_resource_name(scheme, role,
                                                       current_names,
                                                       *args)
            name = click.prompt("Please enter a short, unique name for this resource",
                                default=suggested_name)
            if name in current_names:
                click.echo("Resource name '%s' already in use." %
                           name, err=True)

    validate_git_fat_in_path_if_needed(workspace_dir)
    ns = actions.Namespace()
    r = get_resource_from_command_line(scheme, role, name, workspace_dir,
                                       batch, verbose, *args)
    plan = []
    plan.append(AddResource(ns, verbose, r))
    plan.append(AddResourceToFile(ns, verbose, r, current_resources))
    git_add_files = [current_resources.json_file]
    add_to_git_ignore_step = add_local_dir_to_gitignore_if_needed(ns, verbose,
                                                                  r, workspace_dir)
    if add_to_git_ignore_step:
        plan.append(add_to_git_ignore_step)
        git_add_files.append(add_to_git_ignore_step.gitignore_path)
    plan.append(actions.GitAdd(ns, verbose,
                               workspace_dir, git_add_files))
    plan.append(actions.GitCommit(ns, verbose,
                                  workspace_dir, 'Added resource %s'%str(r)))
    plan.append(UpdateLocalParams(ns, verbose, r, workspace_dir))
    actions.run_plan(plan, 'Add %s to workspace'%str(r), 'Added %s to workspace'%str(r), batch, verbose)
