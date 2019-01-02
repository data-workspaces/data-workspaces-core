# Copyright 2018 by MPI-SWS and Data-ken Research. Licensed under Apache 2.0. See LICENSE.txt.

import os
from os.path import join, exists, isdir
import re
import json
import datetime
import getpass
import shutil

import click

from dataworkspaces.utils.git_utils import is_a_git_hash
from dataworkspaces.resources.resource import CurrentResources
from dataworkspaces.resources.snapshot_utils import \
    expand_dir_template, validate_template, make_re_pattern_for_dir_template
import dataworkspaces.commands.actions as actions
from .params import RESULTS_DIR_TEMPLATE, RESULTS_MOVE_EXCLUDE_FILES,\
                    get_config_param_value, get_local_param_from_file,\
                    HOSTNAME
from .init import get_config_file_path
from .run import get_current_lineage_dir
from dataworkspaces.errors import InternalError, ConfigurationError


class TakeResourceSnapshot(actions.Action):
    """Will store the hash in the namespace property map_of_hashes,
    using the resource url as a key"""
    def __init__(self, ns, verbose, resource):
        super().__init__(ns, verbose)
        self.resource = resource
        self.resource.snapshot_prechecks()

    def run(self):
        self.ns.map_of_hashes[self.resource.name] = self.resource.snapshot()

    def __str__(self):
        return "Run snapshot actions for %s" % str(self.resource)

class WriteSnapshotFile(actions.Action):
    """Creates a snapshot file and populates the
    snapshot_hash property in the namespace"""
    @actions.provides_to_ns('snapshot_hash', str)
    @actions.provides_to_ns('snapshot_filename', str)
    @actions.requires_from_ns('map_of_hashes', dict)
    def __init__(self, ns, verbose, workspace_dir, current_resources):
        super().__init__(ns, verbose)
        self.workspace_dir = workspace_dir
        self.current_resources = current_resources
        self.new_snapshot = None

    def run(self):
        def write_fn(tempfile):
            self.current_resources.write_snapshot_manifest(tempfile,
                                                           self.ns.map_of_hashes)
        (self.ns.snapshot_hash, self.ns.snapshot_filename, self.new_snapshot) = \
            actions.write_and_hash_file(
                write_fn,
                join(self.workspace_dir,
                     ".dataworkspace/snapshots/snapshot-<HASHVAL>.json"),
                self.verbose)

    def __str__(self):
        return 'Create and hash snapshot file'


class MoveCurrentFilesForResults(actions.Action):
    def __init__(self, ns, verbose, workspace_dir, resource, exclude_files,
                 rel_dest_root, exclude_dirs_re):
        super().__init__(ns, verbose)
        assert resource.has_results_role()
        self.resource = resource
        self.exclude_files = exclude_files
        self.rel_dest_root = rel_dest_root
        self.exclude_dirs_re = exclude_dirs_re

    def run(self):
        self.resource.results_move_current_files(self.rel_dest_root,
                                                 self.exclude_files,
                                                 self.exclude_dirs_re)

    def __str__(self):
        return 'Move results files for resource %s to subdirectory %s' %\
            (self.resource.name, self.rel_dest_root)


# TODO: If not a new snapshot, merge history entries!
class AppendSnapshotHistory(actions.Action):
    @actions.requires_from_ns('snapshot_hash', str)
    def __init__(self, ns, verbose, snapshot_history_file,
                 snapshot_history_data, tag, message, timestamp, rel_dest_root,
                 hostname):
        super().__init__(ns, verbose)
        self.snapshot_data = {
            'tag':tag, 'message':message,
            'relative_destination_path':rel_dest_root,
            'hostname':hostname,
            'timestamp':timestamp.isoformat()
        }
        self.snapshot_history_file = snapshot_history_file
        self.snapshot_history_data = snapshot_history_data

    def run(self):
        self.snapshot_data['hash'] = self.ns.snapshot_hash
        self.snapshot_history_data.append(self.snapshot_data)
        with open(self.snapshot_history_file, 'w') as f:
            json.dump(self.snapshot_history_data, f, indent=2)

    def __str__(self):
        return "Append snapshot metadata to .dataworkspace/snapshots/snapshot_history.json"

def get_snapshot_lineage_dir(workspace_dir, snapshot_hash):
    return join(workspace_dir, '.dataworkspace/snapshot_lineage/%s' % snapshot_hash)

class SaveLineageData(actions.Action):
    @actions.requires_from_ns('snapshot_hash', str)
    @actions.provides_to_ns('lineage_files', list)
    def __init__(self, ns, verbose, workspace_dir):
        super().__init__(ns, verbose)
        self.workspace_dir = workspace_dir
        self.current_lineage_dir = get_current_lineage_dir(workspace_dir)
        if not isdir(self.current_lineage_dir):
            self.basenames = None # indicates that there are no lineage files
        else:
            self.basenames = [f for f in os.listdir(self.current_lineage_dir) if f.endswith('.json')]
            if len(self.basenames)==0:
                self.basenames = None

    def has_lineage_files(self):
        return self.basenames is not None

    def run(self):
        snapshot_hash = self.ns.snapshot_hash
        lineage_dir = get_snapshot_lineage_dir(self.workspace_dir, snapshot_hash)
        lineage_files = []
        os.makedirs(lineage_dir)
        for basename in self.basenames:
            src = join(self.current_lineage_dir, basename)
            dest = join(lineage_dir, basename)
            if self.verbose:
                click.echo(" Copy %s => %s"% (src, dest))
            shutil.copy(src, dest)
            lineage_files.append(dest)
        self.ns.lineage_files = lineage_files

    def __str__(self):
        return "Copy lineage %d files from current workspace to snapshot lineage" % \
            len(self.basenames)


def get_snapshot_history_file_path(workspace_dir):
    return join(workspace_dir,
                '.dataworkspace/snapshots/snapshot_history.json')

def snapshot_command(workspace_dir, batch, verbose, tag=None, message=''):
    print("snapshot of %s, tag=%s, message=%s" % (workspace_dir, tag, message))
    snapshot_timestamp = datetime.datetime.now()
    if (tag is not None) and is_a_git_hash(tag):
        raise ConfigurationError("Tag '%s' looks like a git hash. Please pick something else." % tag)
    current_resources = CurrentResources.read_current_resources(workspace_dir, batch, verbose)
    plan = []
    ns = actions.Namespace()
    ns.map_of_hashes = actions.Promise(dict, "TakeResourceSnapshot")

    snapshot_history_file = get_snapshot_history_file_path(workspace_dir)
    if not exists(snapshot_history_file):
        raise InternalError("Missing snapshot history file at %s" %
                            snapshot_history_file)
    with open(snapshot_history_file, 'r') as f:
        snapshot_history_data = json.load(f)
    # Snapshot numbers are just assigned based on where they are in the
    # history file. Counting starts at 1.
    snapshot_number = len(snapshot_history_data)+1
    with open(get_config_file_path(workspace_dir), 'r') as f:
        config_data = json.load(f)
    exclude_files = set(get_config_param_value(config_data, RESULTS_MOVE_EXCLUDE_FILES))
    results_dir_template = get_config_param_value(config_data, RESULTS_DIR_TEMPLATE)
    username = getpass.getuser()
    hostname = get_local_param_from_file(workspace_dir, HOSTNAME)
    validate_template(results_dir_template)
    # relative path to which we will mvoe results files
    rel_dest_root = expand_dir_template(results_dir_template, username, hostname,
                                        snapshot_timestamp, snapshot_number,
                                        tag)
    exclude_dirs_re = re.compile(make_re_pattern_for_dir_template(results_dir_template))


    for r in current_resources.resources:
        if r.has_results_role():
            plan.append(MoveCurrentFilesForResults(ns, verbose, workspace_dir, r,
                                                   exclude_files,
                                                   rel_dest_root,
                                                   exclude_dirs_re))
        plan.append(
            TakeResourceSnapshot(ns, verbose, r))
    plan.append(WriteSnapshotFile(ns, verbose, workspace_dir, current_resources))
    plan.append(AppendSnapshotHistory(ns, verbose, snapshot_history_file,
                                      snapshot_history_data, tag, message,
                                      snapshot_timestamp, rel_dest_root,
                                      hostname))
    plan.append(actions.GitAdd(ns, verbose, workspace_dir,
                               lambda:[ns.snapshot_filename,
                                       snapshot_history_file]))
    # see if we need to add lineage files
    save_lineage = SaveLineageData(ns, verbose, workspace_dir)
    if save_lineage.has_lineage_files():
        plan.append(save_lineage)
        plan.append(actions.GitAdd(ns, verbose, workspace_dir,
                                   actions.NamespaceRef('lineage_files', list, ns)))
    plan.append(actions.GitCommit(ns, verbose, workspace_dir,
                                  commit_message=lambda:"Snapshot "+ns.snapshot_hash))
    ns.map_of_hashes = {}
    actions.run_plan(plan, "take snapshot of workspace",
                     "taken snapshot of workspace", batch=batch, verbose=verbose)
    return ns.snapshot_hash


