# Copyright 2018,2019 by MPI-SWS and Data-ken Research. Licensed under Apache 2.0. See LICENSE.txt.
import os
from os.path import isdir, join, dirname, basename
import json

import click

from dataworkspaces.utils.regexp_utils import RSYNC_RE, USERNAME_RE, FPATH_RE
from dataworkspaces.utils.git_utils import get_dot_gitfat_file_path, validate_git_fat_in_path
from dataworkspaces.resources.resource import \
    get_resource_file_path, get_resource_local_params_file_path,\
    RESOURCE_ROLE_CHOICES
from dataworkspaces.errors import ConfigurationError
import dataworkspaces.commands.actions as actions
from .params import get_all_defaults, get_local_defaults,\
                    get_local_params_file_path
from dataworkspaces import __version__
from .add import add_command

def get_config_file_path(workspace_dir):
    return join(workspace_dir, '.dataworkspace/config.json')

def get_snapshot_dir_path(workspace_dir):
    return join(workspace_dir, '.dataworkspace/snapshots')

def get_snapshot_metadata_dir_path(workspace_dir):
    return join(workspace_dir, '.dataworkspace/snapshot_metadata')


class MakeWorkSpaceConfig(actions.Action):
    def __init__(self, ns, verbose, workspace_dir, workspace_name, hostname):
        super().__init__(ns, verbose)
        self.dataworkspace_dir =  join(workspace_dir, '.dataworkspace')
        self.config_fpath = get_config_file_path(workspace_dir)
        self.resources_fpath = get_resource_file_path(workspace_dir)
        self.resource_local_params_fpath = \
            get_resource_local_params_file_path(workspace_dir)
        self.local_params_fpath = get_local_params_file_path(workspace_dir)
        self.gitignore_fpath = join(workspace_dir, '.dataworkspace/.gitignore')
        self.snapshots_dir  = get_snapshot_dir_path(workspace_dir)
        self.snapshot_metadata_dir = get_snapshot_metadata_dir_path(workspace_dir)
        self.workspace_name = workspace_name
        self.local_params = get_local_defaults(hostname)
        if isdir(self.dataworkspace_dir):
            raise actions.ConfigurationError(".dataworkspace already exists in %s" %
                                             dirname(self.dataworkspace_dir))

    def run(self):
        os.mkdir(self.dataworkspace_dir)
        os.mkdir(self.snapshots_dir)
        os.mkdir(self.snapshot_metadata_dir)
        with open(self.config_fpath, 'w') as f:
            json.dump({'name':self.workspace_name, 'dws-version':__version__,
                       'global_params':get_all_defaults()},
                      f, indent=2)
        with open(self.resources_fpath, 'w') as f:
            json.dump([], f, indent=2)
        with open(self.local_params_fpath, 'w') as f:
            json.dump(self.local_params, f, indent=2)
        with open(self.resource_local_params_fpath, 'w') as f:
            json.dump({}, f, indent=2)
        with open(self.gitignore_fpath, 'w') as f:
            f.write("%s\n" % basename(self.local_params_fpath))
            f.write("%s\n" % basename(self.resource_local_params_fpath))
            f.write("current_lineage/\n")

    def __str__(self):
        return "Initialize .dataworkspace directory for workspace '%s'" %\
            self.workspace_name

class InitializeGitFat(actions.Action):
    def __init__(self, ns, verbose, workspace_dir, git_fat_remote, git_fat_user,
                 git_fat_port, git_fat_attributes, dot_git_fat_fpath,
                 dot_git_attributes_fpath):
        super().__init__(ns, verbose)
        self.workspace_dir = workspace_dir
        self.dataworkspace_dir =  join(workspace_dir, '.dataworkspace')
        if (RSYNC_RE.match(git_fat_remote) is None) and \
           (FPATH_RE.match(git_fat_remote) is None):
            raise ConfigurationError("'%s' is not a valid remote address for rsync (used by git-fat)" % git_fat_remote)
        if git_fat_user is not None and USERNAME_RE.match(git_fat_user) is None:
            raise ConfigurationError("'%s' is not a valid remote username for git-fat"%
                                     git_fat_user)
        import dataworkspaces.third_party.git_fat as git_fat
        self.python2_exe = git_fat.find_python2_exe()
        self.git_fat_remote = git_fat_remote
        self.git_fat_user = git_fat_user
        self.git_fat_port = git_fat_port
        self.git_fat_attributes = git_fat_attributes
        self.dot_git_fat_fpath = dot_git_fat_fpath
        self.dot_git_attributes_fpath = dot_git_attributes_fpath

    def run(self):
        import dataworkspaces.third_party.git_fat as git_fat
        #click.echo("Initializing git-fat with remote %s" % self.git_fat_remote)
        with open(self.dot_git_fat_fpath, 'w') as f:
            f.write("[rsync]\nremote = %s\n" % self.git_fat_remote)
            if self.git_fat_user:
                f.write("sshuser = %s\n" % self.git_fat_user)
            if self.git_fat_port:
                f.write("sshport = %s\n" % self.git_fat_port)
        if self.git_fat_attributes is not None:
            with open(self.dot_git_attributes_fpath, 'w') as f:
                for extn in self.git_fat_attributes.split(','):
                    f.write('%s filter=fat -crlf\n' % extn)
        git_fat.run_git_fat(self.python2_exe, ['init'], cwd=self.workspace_dir,
                            verbose=self.verbose)

    def __str__(self):
        return "Initialize the git-fat Git extension on the dataworkspace's repository, using remote %s." %\
            self.git_fat_remote

def init_command(name, hostname, create_resources,
                 git_fat_remote=None, git_fat_user=None, git_fat_port=None,
                 git_fat_attributes=None, batch=False, verbose=False):
    plan = []
    ns = actions.Namespace()
    workspace_dir = actions.CURR_DIR
    if actions.is_git_repo(workspace_dir):
        click.echo("Found a git repo, we will add to it")
    else:
        plan.append(actions.GitInit(ns, verbose, workspace_dir))
    step = MakeWorkSpaceConfig(ns, verbose, workspace_dir, name, hostname)
    config_fpath = step.config_fpath
    resources_fpath = step.resources_fpath
    gitignore_fpath = step.gitignore_fpath
    plan.append(step)
    files_to_add = [config_fpath, resources_fpath, gitignore_fpath]
    if git_fat_remote is not None:
        validate_git_fat_in_path()
        dot_git_fat_fpath = get_dot_gitfat_file_path(workspace_dir)
        files_to_add.append(dot_git_fat_fpath)
        if git_fat_attributes:
            dot_git_attributes_fpath = join(workspace_dir, '.gitattributes')
            files_to_add.append(dot_git_attributes_fpath)
        else:
            dot_git_attributes_fpath = None
        plan.append(InitializeGitFat(ns, verbose, workspace_dir, git_fat_remote,
                                     git_fat_user, git_fat_port, git_fat_attributes,
                                     dot_git_fat_fpath, dot_git_attributes_fpath))
    plan.append(actions.GitAdd(ns, verbose, workspace_dir, files_to_add))
    plan.append(actions.GitCommit(ns, verbose, workspace_dir,
                                  'Initial version of data workspace'))
    actions.run_plan(plan, 'initialize a workspace at %s' % workspace_dir,
                     'initialized a workspace at %s' % workspace_dir,
                     batch=batch, verbose=verbose)
    if len(create_resources)>0:
        click.echo("Will now create sub-directory resources for "+
                   ", ".join(create_resources))
        for role in create_resources:
            assert role in RESOURCE_ROLE_CHOICES, "bad role name %s" % role
            add_command('git-subdirectory', role, role,
                        workspace_dir, batch, verbose,
                        join(workspace_dir, role),
                        # no prompt for subdirectory
                        False)
        click.echo("Finished initializing resources:")
        for role in create_resources:
            click.echo("  %s: ./%s" %(role, role))



