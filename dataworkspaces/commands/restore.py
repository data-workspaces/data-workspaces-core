# Copyright 2018,2019 by MPI-SWS and Data-ken Research. Licensed under Apache 2.0. See LICENSE.txt.
import json
import os
from os.path import join, isdir
import click

from dataworkspaces.utils.git_utils import \
    is_a_git_hash, is_a_shortened_git_hash, is_a_git_fat_repo, \
    validate_git_fat_in_path_if_needed
import dataworkspaces.commands.actions as actions
from dataworkspaces.errors import ConfigurationError, UserAbort, \
    InternalError
from dataworkspaces.resources.resource import \
    CurrentResources, SnapshotResources
from .init import get_snapshot_metadata_dir_path
from .snapshot import get_snapshot_lineage_dir
from dataworkspaces.utils.lineage_utils import \
    get_current_lineage_dir, LineageStoreCurrent

class RestoreResource(actions.Action):
    def __init__(self, ns, verbose, resource, restore_hashes):
        super().__init__(ns, verbose)
        self.resource = resource
        #self.hashval = snapshot_resources.name_to_hashval[resource.name]
        if (resource.name in restore_hashes):
            self.hashval = restore_hashes[resource.name]
            if self.hashval is None:
                raise ConfigurationError("Resource '%s' cannot be restored. To run the restore of the workspace, skip this resource using the --leave option"%
                                         resource.name)
        else:
            raise InternalError("Unable to restore resource '%s': no restore hash"
                                % resource.name)
        self.resource.restore_prechecks(self.hashval)

    def run(self):
        self.resource.restore(self.hashval)

    def __str__(self):
        return "Run restore actions for %s" % str(self.resource)

class SkipResource(actions.Action):
    def __init__(self, ns, verbose, resource, reason):
        super().__init__(ns, verbose)
        self.resource = resource
        self.reason = reason

    def run(self):
        pass

    def __str__(self):
        return 'Skipping resource %s, %s' % (str(self.resource), self.reason)

class AddResourceToSnapshotResourceList(actions.Action):
    def __init__(self, ns, verbose, resource, snapshot_resources):
        super().__init__(ns, verbose)
        self.resource = resource
        self.snapshot_resources = snapshot_resources
        # A given resource should resolve to a unique name, so this is the best way
        # to check for duplication.
        if snapshot_resources.is_a_current_name(resource.name):
            raise ConfigurationError("A resource with name '%s' already in snapshot" % resource.url)

    def run(self):
        self.snapshot_resources.add_resource(self.resource)
        #self.snapshot_resources.write_snapshot_resources()

    def __str__(self):
        return "Add '%s' to resources.json file" % str(self.resource)

class WriteRevisedSnapshotFile(actions.Action):
    @actions.requires_from_ns('map_of_hashes', dict)
    @actions.provides_to_ns('snapshot_hash', str)
    @actions.provides_to_ns('snapshot_filename', str)
    def __init__(self, ns, verbose, workspace_dir, snapshot_resources):
        super().__init__(ns, verbose)
        self.workspace_dir = workspace_dir
        self.snapshot_resources = snapshot_resources

    def run(self):
        def write_fn(tempfile):
            self.snapshot_resources.write_revised_snapshot_manifest(tempfile,
                                                                    self.ns.map_of_hashes)
        (self.ns.snapshot_hash, self.ns.snapshot_filename, _) = \
            actions.write_and_hash_file(
                write_fn,
                join(self.workspace_dir,
                     ".dataworkspace/snapshots/snapshot-<HASHVAL>.json"),
                self.verbose)

    def __str__(self):
        return 'Create and hash snapshot file'

class WriteRevisedResourceFile(actions.Action):
    def __init__(self, verbose, snapshot_resources):
        self.verbose = verbose
        self.snapshot_resources = snapshot_resources

    def run(self):
        self.snapshot_resources.write_current_resources()

    def __str__(self):
        return "Write revised resources.json file"


class CopyLineageFilesToCurrent(actions.Action):
    def __init__(self, ns, verbose, current_lineage_dir, snapshot_lineage_dir,
                 resource_names):
        super().__init__(ns, verbose)
        self.current_lineage_dir = current_lineage_dir
        self.snapshot_lineage_dir = snapshot_lineage_dir
        self.resource_names = resource_names

    def run(self):
        if not isdir(self.current_lineage_dir):
            os.makedirs(self.current_lineage_dir)
        LineageStoreCurrent.restore_store_from_snapshot(self.snapshot_lineage_dir,
                                                        self.current_lineage_dir,
                                                        self.resource_names)

    def __str__(self):
        return "Copy lineage files from snapshot to current lineage"

class GitFatPull(actions.Action):
    def __init__(self, ns, verbose, workspace_dir):
        super().__init__(ns, verbose)
        self.workspace_dir = workspace_dir
        import dataworkspaces.third_party.git_fat as git_fat
        self.python2_exe = git_fat.find_python2_exe()

    def run(self):
        import dataworkspaces.third_party.git_fat as git_fat
        git_fat.run_git_fat(self.python2_exe, ['pull'],
                            cwd=self.workspace_dir, verbose=self.verbose)

    def __str__(self):
        return 'Run a git-fat pull to update any files managed by git-fat'


def process_names(current_names, snapshot_names, only=None, leave=None):
    """Based on what we have currently, what's in the snapshot, and the
    --only, --leave, and --ignore-dropped command line options, figure out what we should
    do for each resource.
    """
    all_names = snapshot_names.union(current_names)
    names_to_restore = snapshot_names.intersection(current_names)
    names_to_add = snapshot_names.difference(current_names)
    names_to_leave = current_names.difference(snapshot_names)

    if only is not None:
        only_names = only.split(',')
        for name in only_names:
            if name not in all_names:
                raise click.UsageError("No resource in '%s' exists in current or restored workspaces"
                                       % name)
        for name in all_names.difference(only_names):
            # Names not in only 
            if name in names_to_restore:
                names_to_restore.remove(name)
                names_to_leave.add(name)

    if leave is not None:
        leave_names = leave.split(',')
        for name in leave_names:
            if name not in all_names:
                raise click.UsageError("No resource in '%s' exists in current or restored workspaces"
                                       % name)
            elif name in names_to_restore:
                names_to_restore.remove(name)
                names_to_leave.add(name)
    return (sorted(names_to_restore), sorted(names_to_add), sorted(names_to_leave))


def find_snapshot(tag_or_hash, workspace_dir):
    """Return a (hash, tags, result_hashes) tuple for the tag or hash. Throws
    a configuration error if not found.
    """
    if is_a_git_hash(tag_or_hash):
        is_hash = True
        is_short_hash = False
        # we'll be case-insensitive for full hashes
        tag_or_hash = tag_or_hash.lower()
    elif is_a_shortened_git_hash(tag_or_hash):
        is_short_hash = True
        is_hash = False
    else:
        is_hash = is_short_hash = False
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
                if (is_hash and data['hash']==tag_or_hash) or \
                   (is_short_hash and data['hash'].startswith(tag_or_hash)) or\
                   ((not (is_hash or is_short_hash)) and
                    tag_or_hash in data['tags']):
                    return (data['hash'], data['tags'], data['restore_hashes'])
    result = process_dir(md_dir)
    if result is not None:
        return result
    elif is_hash or is_short_hash:
        raise ConfigurationError("Did not find a snapshot corresponding to '%s' in history" % tag_or_hash)
    else:
        raise ConfigurationError("Did not find a snapshot corresponding to tag '%s' in history" % tag_or_hash)


def restore_command(workspace_dir, batch, verbose, tag_or_hash, only=None, leave=None):
    validate_git_fat_in_path_if_needed(workspace_dir)
    # First, find the history entry
    (snapshot_hash, snapshot_tags, restore_hashes) = find_snapshot(tag_or_hash, workspace_dir)
    snapshot_resources = SnapshotResources.read_shapshot_manifest(snapshot_hash, workspace_dir, batch, verbose)
    current_resources = CurrentResources.read_current_resources(workspace_dir, batch, verbose)
    original_current_resource_names = current_resources.get_names()
    (names_to_restore, names_to_add, names_to_leave) = \
        process_names(original_current_resource_names, snapshot_resources.get_names(), only, leave)
    plan = []
    ns = actions.Namespace()
    ns.map_of_hashes = {}
    names_to_restore_lineage = []
    results_resources = set()
    for name in names_to_restore:
        # resources in both current and restored
        r = current_resources.by_name[name]
        if not r.has_results_role():
            # just need to call restore
            plan.append(RestoreResource(ns, verbose, r, restore_hashes))
            names_to_restore_lineage.append(r.name) # only non-results restored
        else:
            # This is a results resource, we'll add it to the leave set
            if only and (name in only):
                raise click.BadOptionUsage(message="Resource '%s' has a Results role and should not be included in the --only list" %
                                           name)
            names_to_leave.append(name)
            results_resources.add(name)
    # remove any names that were results (they were moved to names_to_leave)
    names_to_restore = [name for name in names_to_restore if name not in results_resources]
    for name in names_to_add:
        # These are resources which are in the restored snapshot, but not the
        # current resources. We'll grab the resource objects from snapshot_resources
        # XXX Do we need to handle Results resources differently?
        r = snapshot_resources.by_name[name]
        plan.append(RestoreResource(ns, verbose, r, snapshot_resources))
        if not r.has_results_role():
            # only non-results lineage restored
            names_to_restore_lineage.append(r.name)
    for name in names_to_leave:
        # These resources are only in the current resource list or explicitly left out.
        r = current_resources.by_name[name]
        # if we are adding a current resource to the restored snapshot, we actually
        # have to snapshot the resource itself.
        r = current_resources.by_name[name]
        if not snapshot_resources.is_a_current_name(name):
            # we are just leaving a resource added since snapshot was taken
            plan.append(AddResourceToSnapshotResourceList(ns, verbose, r, snapshot_resources))
    need_to_write_resources_file = \
        original_current_resource_names!=snapshot_resources.get_names()
    if need_to_write_resources_file:
        plan.append(actions.GitAdd(ns, verbose, workspace_dir,
                                   [snapshot_resources.resource_file]))

    # handling of lineage
    current_lineage_dir = get_current_lineage_dir(workspace_dir)
    snapshot_lineage_dir = get_snapshot_lineage_dir(workspace_dir, snapshot_hash)
    if isdir(snapshot_lineage_dir):
        plan.append(CopyLineageFilesToCurrent(ns, verbose, current_lineage_dir,
                                              snapshot_lineage_dir,
                                              names_to_restore_lineage))

    if is_a_git_fat_repo(workspace_dir):
        plan.append(GitFatPull(ns, verbose, workspace_dir))

    if need_to_write_resources_file:
        plan.append(actions.GitCommit(ns, verbose, workspace_dir,
                                      commit_message="Updating resources after restore to %s"%
                                      tag_or_hash))
    tagstr = ' (%s)'%','.join(snapshot_tags) if len(snapshot_tags)>0 else ''
    click.echo("Restore snapshot %s%s" % (snapshot_hash, tagstr))
    def fmt_rlist(rnames):
        if len(rnames)>0:
            return ', '.join(rnames)
        else:
            return 'None'
    click.echo("  Resources to restore: %s" % fmt_rlist(names_to_restore))
    click.echo("  Resources to add: %s" % fmt_rlist(names_to_add))
    click.echo("  Resources to leave: %s" % fmt_rlist(names_to_leave))
    if (not verbose) and (not batch):
        # Unless in batch mode, we always want to ask for confirmation
        # If not in verbose, do it here. In verbose, we'll ask after
        # we print the plan.
        resp = input("Should I perform this restore? [Y/n]")
        if resp.lower()!='y' and resp!='':
            raise UserAbort()
    actions.run_plan(plan, 'run this restore', 'run restore', batch, verbose)

