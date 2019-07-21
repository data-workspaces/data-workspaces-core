# Copyright 2018,2019 by MPI-SWS and Data-ken Research. Licensed under Apache 2.0. See LICENSE.txt.
import os
from os.path import abspath, expanduser, join
from typing import List, Optional

import click

from dataworkspaces.workspace import init_workspace, RESOURCE_ROLE_CHOICES


# XXX Need to add back in
# class InitializeGitFat(actions.Action):
#     def __init__(self, ns, verbose, workspace_dir, git_fat_remote, git_fat_user,
#                  git_fat_port, git_fat_attributes, dot_git_fat_fpath,
#                  dot_git_attributes_fpath):
#         super().__init__(ns, verbose)
#         self.workspace_dir = workspace_dir
#         self.dataworkspace_dir =  join(workspace_dir, '.dataworkspace')
#         if (RSYNC_RE.match(git_fat_remote) is None) and \
#            (FPATH_RE.match(git_fat_remote) is None):
#             raise ConfigurationError(("'%s' is not a valid remote address for rsync (used by git-fat). "+
#                                       "Please use the format HOSTNAME:/PATH")
#                                      % git_fat_remote)
#         if git_fat_user is not None and USERNAME_RE.match(git_fat_user) is None:
#             raise ConfigurationError("'%s' is not a valid remote username for git-fat"%
#                                      git_fat_user)
#         import dataworkspaces.third_party.git_fat as git_fat
#         self.python2_exe = git_fat.find_python2_exe()
#         self.git_fat_remote = git_fat_remote
#         self.git_fat_user = git_fat_user
#         self.git_fat_port = git_fat_port
#         self.git_fat_attributes = git_fat_attributes
#         self.dot_git_fat_fpath = dot_git_fat_fpath
#         self.dot_git_attributes_fpath = dot_git_attributes_fpath

#     def run(self):
#         import dataworkspaces.third_party.git_fat as git_fat
#         #click.echo("Initializing git-fat with remote %s" % self.git_fat_remote)
#         with open(self.dot_git_fat_fpath, 'w') as f:
#             f.write("[rsync]\nremote = %s\n" % self.git_fat_remote)
#             if self.git_fat_user:
#                 f.write("sshuser = %s\n" % self.git_fat_user)
#             if self.git_fat_port:
#                 f.write("sshport = %s\n" % self.git_fat_port)
#         if self.git_fat_attributes is not None:
#             with open(self.dot_git_attributes_fpath, 'w') as f:
#                 for extn in self.git_fat_attributes.split(','):
#                     f.write('%s filter=fat -crlf\n' % extn)
#         git_fat.run_git_fat(self.python2_exe, ['init'], cwd=self.workspace_dir,
#                             verbose=self.verbose)

#     def __str__(self):
#         return "Initialize the git-fat Git extension on the dataworkspace's repository, using remote %s." %\
#             self.git_fat_remote

def init_command(name:str, hostname:str, create_resources:List[str],
                 git_fat_remote:Optional[str]=None, git_fat_user:Optional[str]=None,
                 git_fat_port:Optional[int]=None,
                 git_fat_attributes:Optional[str]=None,
                 batch:bool=False, verbose:bool=False):
    workspace_dir=abspath(expanduser(os.curdir))
    workspace = init_workspace('dataworkspaces.backends.git', #TODO: remove hardcoding
                               name, hostname, batch, verbose, workspace_dir)
    # XXX Add back in git fat
    # if git_fat_remote is not None:
    #     validate_git_fat_in_path()
    #     dot_git_fat_fpath = get_dot_gitfat_file_path(workspace_dir)
    #     files_to_add.append(dot_git_fat_fpath)
    #     if git_fat_attributes:
    #         dot_git_attributes_fpath = join(workspace_dir, '.gitattributes')
    #         files_to_add.append(dot_git_attributes_fpath)
    #     else:
    #         dot_git_attributes_fpath = None
    #     plan.append(InitializeGitFat(ns, verbose, workspace_dir, git_fat_remote,
    #                                  git_fat_user, git_fat_port, git_fat_attributes,
    #                                  dot_git_fat_fpath, dot_git_attributes_fpath))
    # plan.append(actions.GitAdd(ns, verbose, workspace_dir, files_to_add))
    # plan.append(actions.GitCommit(ns, verbose, workspace_dir,
    #                               'Initial version of data workspace'))

    if len(create_resources)>0:
        click.echo("Will now create sub-directory resources for "+
                   ", ".join(create_resources))
        for role in create_resources:
            assert role in RESOURCE_ROLE_CHOICES, "bad role name %s" % role
            workspace.add_resource(role, 'git-subdirectory', role,
                                   join(workspace.workspace_dir, role),
                                   confirm_subdir_create=False)
        click.echo("Finished initializing resources:")
        for role in create_resources:
            click.echo("  %s: ./%s" %(role, role))

    workspace.save("workspace initialization")
    click.echo("Workspace %s initialized successfully."%name)

