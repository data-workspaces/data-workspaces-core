# Copyright 2018,2019 by MPI-SWS and Data-ken Research. Licensed under Apache 2.0. See LICENSE.txt.

import os
from os.path import join, exists, isdir, basename, dirname
import re
import json
import datetime
import getpass
from copy import copy

import click

from dataworkspaces.utils.subprocess_utils import call_subprocess
from dataworkspaces.utils.git_utils import \
    is_a_git_hash, is_a_shortened_git_hash, \
    validate_git_fat_in_path_if_needed, GIT_EXE_PATH
from dataworkspaces.resources.resource import CurrentResources
from dataworkspaces.resources.snapshot_utils import \
    expand_dir_template, validate_template, make_re_pattern_for_dir_template
import dataworkspaces.commands.actions as actions
from .params import RESULTS_DIR_TEMPLATE, RESULTS_MOVE_EXCLUDE_FILES,\
                    get_config_param_value, get_local_param_from_file,\
                    HOSTNAME
from .init import get_config_file_path, get_snapshot_metadata_dir_path
from dataworkspaces.utils.lineage_utils import \
    get_current_lineage_dir, get_snapshot_lineage_dir,\
    LineageStoreCurrent
from dataworkspaces.errors import ConfigurationError, UserAbort

def find_metadata_for_tag(tag, workspace_dir):
    """Return the snapshot metadata for the specified
    tag. Returns None if no such tag exists.
    """
    md_dir=get_snapshot_metadata_dir_path(workspace_dir)
    def process_dir(dirpath):
        for f in os.listdir(dirpath):
            p = join(dirpath, f)
            if isdir(p):
                result = process_dir(p)
                if result is not None:
                    return result
            elif f.endswith('_md.json'):
                with open(p, 'r') as fobj:
                    data = json.load(fobj)
                if tag in data['tags']:
                    return data
    return process_dir(md_dir)


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

def get_next_snapshot_number(workspace_dir):
    """Snapshot numbers are assigned based on how many snapshots have
    already been taken. Counting starts at 1. Note that snaphsot
    numbers are not necessarily unique, as people could simultaneously
    take snapshots in different copies of the workspace. Thus, we
    usually combine the snapshot with the hostname.
    """
    md_dirpath = get_snapshot_metadata_dir_path(workspace_dir)
    if not isdir(md_dirpath):
        return 1
    # we recursively walk the tree to be future-proof in case we
    # find that we need to start putting metadata into subdirectories.
    def process_dir(dirpath):
        cnt=0
        for f in os.listdir(dirpath):
            p = join(dirpath, f)
            if isdir(p):
                cnt += process_dir(p)
            elif f.endswith('_md.json'):
                cnt += 1
        return cnt
    return 1 + process_dir(md_dirpath)


_CONF_MESSAGE=\
"A snapshot with this hash already exists. Do you want to update "+\
"the message from '%s' to '%s'?"

def merge_snapshot_metadata(old, new, batch):
    """Merge two snapshot metadata dicts for when someone creates
    a snapshot without making changes. They might have
    added more tags or changed the message.
    """
    assert old['hash'] == new['hash']
    tags = old['tags'] + [tag for tag in new['tags']
                          if tag not in old['tags']]
    if old['message']!=new['message'] and (new['message'] is not None) \
       and (not batch) and \
       click.confirm(_CONF_MESSAGE%(old['message'], new['message'])):
        message = new['message']
    else:
        message = old['message']
    return {
        'hash':old['hash'],
        'tags':tags,
        'message':message,
        'relative_destination_path':old['relative_destination_path'],
        'hostname':old['hostname'],
        'timestamp':old['timestamp'],
        # Save a new timestamp to indicate that a snapshot was taken.
        # This also servces to force there to be a change in the dws
        # git repo.
        'updated_timestamp':new['timestamp']
    }


class WriteSnapshotMetadata(actions.Action):
    @actions.requires_from_ns('snapshot_hash', str)
    @actions.provides_to_ns('snapshot_metadata_file', str)
    def __init__(self, ns, verbose, batch, workspace_dir,
                 tag, message, timestamp, rel_dest_root,
                 hostname):
        super().__init__(ns, verbose)
        self.batch = batch
        self.snapshot_metadata_dir = get_snapshot_metadata_dir_path(workspace_dir)
        self.tag = tag
        self.message = message
        self.timestamp = timestamp
        self.rel_dest_root = rel_dest_root
        self.hostname = hostname

    def run(self):
        snapshot_data = {
            'hash':self.ns.snapshot_hash,
            'tags':[self.tag] if self.tag is not None else [],
            'message':self.message,
            'relative_destination_path':self.rel_dest_root,
            'hostname':self.hostname,
            'timestamp':self.timestamp.isoformat()
        }
        if not isdir(self.snapshot_metadata_dir):
            os.mkdir(self.shapshot_metadata_dir) # just in case
        filename = join(self.snapshot_metadata_dir,
                        self.ns.snapshot_hash+'_md.json')
        if exists(filename):
            with open(filename, 'r') as f:
                old_snapshot_data = json.load(f)
            snapshot_data = merge_snapshot_metadata(old_snapshot_data,
                                                    snapshot_data,
                                                    self.batch)
        with open(filename, 'w') as f:
            json.dump(snapshot_data, f, indent=2)
        self.ns.snapshot_metadata_file = filename

    def __str__(self):
        return "Write snapshot metadata file to .dataworkspace/snapshot_metadata/HASH_md.json"


class RemoveTagFromOldSnapshot(actions.Action):
    @actions.requires_from_ns('snapshot_hash', str)
    def __init__(self, ns, verbose, workspace_dir, existing_tag_md, remove_tag):
        super().__init__(ns, verbose)
        self.remove_tag = remove_tag
        self.old_hash_file = join(get_snapshot_metadata_dir_path(workspace_dir),
                                  existing_tag_md['hash'] + '_md.json')
        self.snapshot_data = copy(existing_tag_md)
        old_len = len(self.snapshot_data['tags'])
        self.snapshot_data['tags'] = [tag for tag in self.snapshot_data['tags']
                                      if tag!=remove_tag]
        assert old_len!=len(self.snapshot_data['tags']), \
            "did not remove tag %s from %s"%(remove_tag, self.snapshot_data['hash'])
        self.snapshot_data['updated_timestamp'] = datetime.datetime.now().isoformat()

    def run(self):
        if self.ns.snapshot_hash==self.snapshot_data['hash']:
            print("No need to remove tag %s from snapshot %s, as that is also the new snapshot we are taking"%
                  (self.remove_tag, self.snapshot_data['hash']))
            return
        with open(self.old_hash_file, 'w') as f:
            json.dump(self.snapshot_data, f, indent=2)
        call_subprocess([GIT_EXE_PATH, 'add', basename(self.old_hash_file)],
                        cwd=dirname(self.old_hash_file), verbose=self.verbose)

    def __str__(self):
        return "Remove tag %s from old commit %s" % (self.remove_tag, self.old_hash)


class SaveLineageData(actions.Action):
    @actions.requires_from_ns('snapshot_hash', str)
    @actions.requires_from_ns('map_of_hashes', dict)
    @actions.provides_to_ns('lineage_files', list)
    def __init__(self, ns, verbose, workspace_dir, resource_names,
                 results_resources, rel_dest_root):
        super().__init__(ns, verbose)
        self.workspace_dir = workspace_dir
        self.current_lineage_dir = get_current_lineage_dir(workspace_dir)
        self.resource_names = resource_names
        self.results_resources = results_resources
        self.rel_dest_root = rel_dest_root
        if not isdir(self.current_lineage_dir):
            self.num_files = 0
        else:
            currfiles= set(LineageStoreCurrent.get_resource_names_in_fsstore(
                           self.current_lineage_dir))
            self.num_files = len(currfiles.intersection(set(resource_names)))

    def has_lineage_files(self):
        return self.num_files > 0

    def run(self):
        assert self.has_lineage_files()
        snapshot_hash = self.ns.snapshot_hash
        map_of_hashes = self.ns.map_of_hashes
        store = LineageStoreCurrent.load(self.current_lineage_dir)
        store.replace_placeholders_with_real_certs(map_of_hashes)
        store.save(self.current_lineage_dir)
        for rr in self.results_resources:
            (lineages, complete) = store.get_lineage_for_resource(rr.name)
            if len(lineages)>0:
                data = {'resource_name':rr.name,
                        'complete':complete,
                        'lineages':[l.to_json() for l in lineages]}
                rr.add_results_file_from_buffer(json.dumps(data, indent=2),
                                                join(self.rel_dest_root,
                                                     'lineage.json'))
        lineage_dir = get_snapshot_lineage_dir(self.workspace_dir, snapshot_hash)
        os.makedirs(lineage_dir)
        (dest_files, warnings) =\
            LineageStoreCurrent.copy_fsstore_to_snapshot(self.current_lineage_dir,
                                                         lineage_dir,
                                                         self.resource_names)
        self.ns.lineage_files = dest_files
        # We need to invalidate the resource lineage for any results,
        # as we've moved the data to a subdirectory
        if len(self.results_resources)>0:
            LineageStoreCurrent.invalidate_fsstore_entries(self.current_lineage_dir,
                                                           [rr.name for rr in self.results_resources])

    def __str__(self):
        return "Copy lineage %d files from current workspace to snapshot lineage" % \
            self.num_files



def snapshot_command(workspace_dir, batch, verbose, tag=None, message=''):
    print("snapshot of %s, tag=%s, message=%s" % (workspace_dir, tag, message))
    snapshot_timestamp = datetime.datetime.now()
    if (tag is not None) and (is_a_git_hash(tag) or is_a_shortened_git_hash(tag)):
        raise ConfigurationError("Tag '%s' looks like a git hash. Please pick something else." % tag)
    if tag is not None:
        existing_tag_md = find_metadata_for_tag(tag, workspace_dir)
        if existing_tag_md:
            msg = "Tag '%s' already exists for snapshot %s taken %s"%\
                                         (tag, existing_tag_md['hash'],
                                          existing_tag_md['timestamp'])
            if batch:
                raise ConfigurationError(msg)
            elif not click.confirm(msg + ". Remove this tag so we an add it to the new snapshot?"):
                raise UserAbort()
        else:
            existing_tag_md = None
    else:
        existing_tag_md = None
    current_resources = CurrentResources.read_current_resources(workspace_dir, batch, verbose)
    resource_names = current_resources.by_name.keys()
    plan = []
    ns = actions.Namespace()
    ns.map_of_hashes = actions.Promise(dict, "TakeResourceSnapshot")

    snapshot_number = get_next_snapshot_number(workspace_dir)
    with open(get_config_file_path(workspace_dir), 'r') as f:
        config_data = json.load(f)
    exclude_files = set(get_config_param_value(config_data, RESULTS_MOVE_EXCLUDE_FILES))
    results_dir_template = get_config_param_value(config_data, RESULTS_DIR_TEMPLATE)
    username = getpass.getuser()
    hostname = get_local_param_from_file(workspace_dir, HOSTNAME)
    validate_template(results_dir_template)
    # relative path to which we will move results files
    rel_dest_root = expand_dir_template(results_dir_template, username, hostname,
                                        snapshot_timestamp, snapshot_number,
                                        tag)
    exclude_dirs_re = re.compile(make_re_pattern_for_dir_template(results_dir_template))

    validate_git_fat_in_path_if_needed(workspace_dir)
    results_resources = []
    for r in current_resources.resources:
        if r.has_results_role():
            plan.append(MoveCurrentFilesForResults(ns, verbose, workspace_dir, r,
                                                   exclude_files,
                                                   rel_dest_root,
                                                   exclude_dirs_re))
            results_resources.append(r)
        plan.append(
            TakeResourceSnapshot(ns, verbose, r))
    plan.append(WriteSnapshotFile(ns, verbose, workspace_dir, current_resources))
    plan.append(WriteSnapshotMetadata(ns, verbose, batch, workspace_dir,
                                      tag, message, snapshot_timestamp,
                                      rel_dest_root, hostname))
    plan.append(actions.GitAdd(ns, verbose, workspace_dir,
                               lambda:[ns.snapshot_filename,
                                       ns.snapshot_metadata_file]))
    if existing_tag_md is not None:
        plan.append(RemoveTagFromOldSnapshot(ns, verbose, workspace_dir,
                                             existing_tag_md, tag))
    # see if we need to add lineage files
    save_lineage = SaveLineageData(ns, verbose, workspace_dir, resource_names,
                                   results_resources, rel_dest_root)
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


