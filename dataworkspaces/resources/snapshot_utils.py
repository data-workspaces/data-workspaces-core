# Copyright 2018,2019 by MPI-SWS and Data-ken Research. Licensed under Apache 2.0. See LICENSE.txt.
"""
Utilities for dealing with snapshots, particularly for resources where
we move files into a unique snapshot subdirectory each time a
snapshot is taken.
"""
import os
from os.path import join, exists
import re
import stat

import click

from dataworkspaces.errors import ConfigurationError
from dataworkspaces.utils.misc import remove_dir_if_empty

# Timestamps have the form '2018-09-30T14:09:05'
TEMPLATE_VAR_PATS = {
    'USERNAME':r'\w([\w\-\.])*',
    'HOSTNAME':r'\w([\w\-\.])*',
    'SNAPSHOT_NO':r'\d\d+',
    'ISO_TIMESTAMP':r'\d\d\d\d\-\d\d\-\d\dT\d\d:\d\d:\d\d',
    'YEAR':r'\d\d\d\d',
    'MONTH':r'\d\d',
    'SHORT_MONTH':r'\w\w\w',
    'DAY':r'\d\d',
    'HOUR':r'\d\d',
    'MIN':r'\d\d',
    'SEC':r'\d\d',
    'DAY_OF_WEEK':r'\w+',
    'TAG':r'\w([\w\-\.])*',
    #'TAGBOTH':r'\-(\w+\-)?',
    #'TAGLEFT':r'(\-\w+)?',
    #'TAGRIGHT':r'(\w+\-)?'
}
TEMPLATE_VAR_RE = re.compile('\\{\w+\\}')

VALID_TEMPLATE_VARS=set(TEMPLATE_VAR_PATS.keys())
# remove the 'internal' template variables
#VALID_TEMPLATE_VARS.remove('TAGBOTH')
#VALID_TEMPLATE_VARS.remove('TAGLEFT')
#VALID_TEMPLATE_VARS.remove('TAGRIGHT')

def validate_template(template):
    if not template.startswith('snapshots/'):
        raise ConfigurationError("Templates must start with 'snapshots/'")
    for mo in TEMPLATE_VAR_RE.finditer(template):
        tvar = mo.group(0)[1:-1]
        if tvar not in VALID_TEMPLATE_VARS:
            raise ConfigurationError("Unknown variable '%s' in results directory template '%s'"%
                                     (tvar, template))


# def _translate_tag_vars(template):
#     """The {TAG} template variable is optional, so we treat it as a
#     special case to avoid generated paths like foo/bar- or fooo/bar--baz
#     when the tag is not present. We replace any occurrances of a {TAG}
#     that has dashes on either side with one a template variable that
#     indicates where the dashes are.
#     """
#     return template.replace('-{TAG}-', '{TAGBOTH}')\
#                    .replace('-{TAG}', '{TAGLEFT}')\
#                    .replace('{TAG}-', '{TAGRIGHT}')

def make_re_pattern_for_dir_template(template):
    #template = _translate_tag_vars(template)
    # we escape the template and then "unescape" the braces
    escaped = re.escape(template).replace('\\{', '{').replace('\\}', '}')
    def repl(tvar_mo):
        tvar = tvar_mo.group(0)[1:-1]
        assert tvar in TEMPLATE_VAR_PATS, \
            "Uknown result directory template variable '%s'" %tvar
        return TEMPLATE_VAR_PATS[tvar]
    return '^' + TEMPLATE_VAR_RE.sub(repl, escaped) + '$'

WEEKDAYS=['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
SHORT_MONTHS=['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep',
              'Oct', 'Nov', 'Dec']

def expand_dir_template(template, username, hostname, timestamp, snapshot_no,
                        snapshot_tag=None):
    #template = _translate_tag_vars(template)
    values = {
        'USERNAME':username,
        'HOSTNAME':hostname,
        'ISO_TIMESTAMP':timestamp.isoformat()[0:19], # truncate fractional seconds
        'YEAR':'%04d'%timestamp.year,
        'MONTH':'%02d'%timestamp.month,
        'SHORT_MONTH':SHORT_MONTHS[timestamp.month-1],
        'DAY':'%02d'%timestamp.day,
        'HOUR':'%02d'%timestamp.hour,
        'MIN':'%02d'%timestamp.minute,
        'SEC':'%02d'%timestamp.second,
        'DAY_OF_WEEK':WEEKDAYS[timestamp.weekday()],
        'SNAPSHOT_NO':'%03d'%snapshot_no,
    }
    if snapshot_tag is not None:
        values['TAG'] = snapshot_tag
        #values['TAGBOTH'] = '-' + snapshot_tag + '-'
        #values['TAGLEFT'] = '-' + snapshot_tag
        #values['TAGRIGHT'] = snapshot_tag + '-'
    else:
        #values['TAG'] = values['TAGLEFT'] = values['TAGRIGHT'] = ''
        #values['TAGBOTH'] = '-'
        values['TAG'] = '%03d'%snapshot_no # if the tag isn't present, use the snapshot number
    def repl(tvar_mo):
        tvar = tvar_mo.group(0)[1:-1]
        assert tvar in values, \
            "Uknown result directory template variable '%s'" %tvar
        return values[tvar]
    return TEMPLATE_VAR_RE.sub(repl, template)


def move_file_and_set_readonly(src, dest):
    os.rename(src, dest)
    mode = os.stat(dest)[stat.ST_MODE]
    os.chmod(dest, mode & ~stat.S_IWUSR & ~stat.S_IWGRP & ~stat.S_IWOTH)

DOT_GIT_RE = re.compile('^'+re.escape('.git')+'$')
def move_current_files_local_fs(resource_name,
                                base_dir, rel_dest_root, exclude_files,
                                exclude_dirs_res, move_fn=move_file_and_set_readonly,
                                verbose=False):
    """Utility for impelementing Resource.results_move_current_file()
    for when the files are stored on the local filesystem (e.g. local or git
    resources).
    exclude_dirs_res is either a single regular expression object or a list
    of regular expression objects. It should not include .git, as that will always
    be appended to the list.

    The move function should also set file to read-only. For git, this should
    be done before adding the file to the staging area.

    This returns the list of (before, after) relative path pairs.
    """
    abs_dest_root = join(base_dir, rel_dest_root)
    created_dir = False # only create when we actually have a file to move
    moved_files = []
    dirs_to_remove_if_empty = []
    if not isinstance(exclude_dirs_res, list):
        exclude_dirs_res = [exclude_dirs_res,]
    exclude_dirs_res.append(DOT_GIT_RE)
    for (dirpath, dirnames, filenames) in os.walk(base_dir):
        assert dirpath.startswith(base_dir)
        rel_dirpath = dirpath[len(base_dir)+1:]
        def join_to_rel_dirpath(f):
            return join(rel_dirpath, f)
        join_rel_path = join_to_rel_dirpath if len(rel_dirpath)>0 \
                        else lambda f:f
        # find directories we should skip, as they represent results from
        # prior runs
        skip = []
        for (i, dir_name) in enumerate(dirnames):
            abs_dirpath = join(dirpath, dir_name)
            rel_dirpath = abs_dirpath[len(base_dir)+1:]
            for exclude_dirs_re in exclude_dirs_res:
                if exclude_dirs_re.match(rel_dirpath):
                    skip.append(i)
                    if verbose:
                        print("Skipping directory %s" % rel_dirpath)
        skip.reverse()
        for i in skip:
            del dirnames[i]
        # move files to our new directory
        moved_files_out_of_this_dir = False
        for f in filenames:
            rel_src_file = join_rel_path(f)
            if rel_src_file in exclude_files:
                if verbose:
                    click.echo("[%s] Leaving %s" % (resource_name, rel_src_file))
                continue
            assert not rel_src_file.startswith('snapshot/'),\
                "Internal error: file %s should have been excluded from move"%\
                rel_src_file
            rel_dest_file = join(rel_dest_root, join_rel_path(f))
            abs_src_file = join(dirpath, f)
            abs_dest_file = join(abs_dest_root, join_rel_path(f))
            if verbose:
                click.echo("[%s] Moving %s to %s" % (resource_name,
                                                     rel_src_file, rel_dest_file))
                click.echo("     Absolute %s => %s" % (abs_src_file, abs_dest_file))
            dest_parent = os.path.dirname(abs_dest_file)
            if not created_dir:
                # lazily create the root directory
                os.makedirs(abs_dest_root)
                created_dir = True
            if not exists(dest_parent):
                os.makedirs(dest_parent)
            move_fn(abs_src_file, abs_dest_file)
            moved_files.append((rel_src_file, rel_dest_file))
            moved_files_out_of_this_dir = True
        if moved_files_out_of_this_dir:
            dirs_to_remove_if_empty.append(dirpath)
    for dirpath in dirs_to_remove_if_empty:
        remove_dir_if_empty(dirpath, base_dir, verbose=verbose)
    return moved_files

