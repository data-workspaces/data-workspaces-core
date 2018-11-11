import json
import os
from os.path import join, isdir
import datetime
import shutil

import click

import dataworkspaces.commands.actions as actions
from dataworkspaces.errors import ConfigurationError, UserAbort
from dataworkspaces.resources.resource import \
    CurrentResources, SnapshotResources
from .snapshot import TakeResourceSnapshot, AppendSnapshotHistory,\
                      get_snapshot_history_file_path,\
                      get_snapshot_lineage_dir
from .run import get_current_lineage_dir

class RestoreResource(actions.Action):
    def __init__(self, ns, verbose, resource, snapshot_resources):
        super().__init__(ns, verbose)
        self.resource = resource
        self.hashval = snapshot_resources.name_to_hashval[resource.name]
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

class ClearLineageDir(actions.Action):
    def __init__(self, ns, verbose, current_lineage_dir):
        super().__init__(ns, verbose)
        self.current_lineage_dir = current_lineage_dir

    def run(self):
        shutil.rmtree(self.current_lineage_dir)

    def __str__(self):
        return "Delete the (invalid) current lineage directory %s" %\
            self.current_lineage_dir

class CopyLineageFilesToCurrent(actions.Action):
    def __init__(self, ns, verbose, current_lineage_dir, snapshot_lineage_dir):
        super().__init__(ns, verbose)
        self.current_lineage_dir = current_lineage_dir
        self.snapshot_lineage_dir = snapshot_lineage_dir

    def run(self):
        if isdir(self.current_lineage_dir):
            shutil.rmtree(self.current_lineage_dir)
        os.makedirs(self.current_lineage_dir)
        basenames = os.listdir(self.snapshot_lineage_dir)
        for basename in basenames:
            src = join(self.snapshot_lineage_dir, basename)
            dest = join(self.current_lineage_dir, basename)
            if self.verbose:
                click.echo(" Copy %s => %s" % (src, dest))
            shutil.copy(src, dest)

    def __str__(self):
        return "Copy lineage files from snapshot to current lineage"


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

def restore_command(workspace_dir, batch, verbose, tag_or_hash,
                    only=None, leave=None, no_new_snapshot=False):
    # First, find the history entry
    sh_file = get_snapshot_history_file_path(workspace_dir)
    with open(sh_file, 'r') as f:
        sh_data = json.load(f)
    is_hash = actions.is_a_git_hash(tag_or_hash)
    found = False
    for snapshot in sh_data:
        if is_hash and snapshot['hash']==tag_or_hash:
            found = True
            break
        elif (not is_hash) and snapshot['tag']==tag_or_hash:
            found = True
            break
    if not found:
        if is_hash:
            raise ConfigurationError("Did not find a snapshot corresponding to '%s' in history" % tag_or_hash)
        else:
            raise ConfigurationError("Did not find a snapshot corresponding to tag '%s' in history" % tag_or_hash)

    snapshot_resources = SnapshotResources.read_shapshot_manifest(snapshot['hash'], workspace_dir, batch, verbose)
    current_resources = CurrentResources.read_current_resources(workspace_dir, batch, verbose)
    original_current_resource_names = current_resources.get_names()
    (names_to_restore, names_to_add, names_to_leave) = \
        process_names(original_current_resource_names, snapshot_resources.get_names(), only, leave)
    plan = []
    creating_new_snapshot = False # True if we are creating a new snapshot and hash
    need_to_clear_lineage = False # True if we did a partial restore and we need to clear out lineage data
    ns = actions.Namespace()
    ns.map_of_hashes = {}
    for name in names_to_restore:
        # resources in both current and restored
        r = current_resources.by_name[name]
        if not r.has_results_role():
            # just need to call restore
            plan.append(RestoreResource(ns, verbose, r, snapshot_resources))
        else:
            # This is a results resource, we'll add it to the leave set
            if only and (name in only):
                raise click.BadOptionUsage(message="Resource '%s' has a Results role and should not be included in the --only list" %
                                           name)
            names_to_leave.append(name)
    for name in names_to_add:
        # These are resources which are in the restored snapshot, but not the
        # current resources. We'll grab the resource objects from snapshot_resources
        # XXX Do we need to handle Results resources differently?
        r = snapshot_resources.by_name[name]
        plan.append(RestoreResource(ns, verbose, r, snapshot_resources))
    for name in names_to_leave:
        # These resources are only in the current resource list or explicitly left out.
        r = current_resources.by_name[name]
        # if we are adding a current resource to the restored snapshot, we actually
        # have to snapshot the resource itself.
        r = current_resources.by_name[name]
        if snapshot_resources.is_a_current_name(name):
            # we are leaving a resource in common between current and snapshot
            need_to_clear_lineage = True
        else:
            # we are just leaving a resource added since snapshot was taken
            plan.append(AddResourceToSnapshotResourceList(ns, verbose, r, snapshot_resources))
        if not no_new_snapshot:
            plan.append(TakeResourceSnapshot(ns, verbose, r))
            creating_new_snapshot = True
    need_to_write_resources_file = \
        original_current_resource_names!=snapshot_resources.get_names()
    if creating_new_snapshot:
        write_revised = WriteRevisedSnapshotFile(ns, verbose, workspace_dir,
                                                 snapshot_resources)
        plan.append(write_revised)
        history_action = AppendSnapshotHistory(ns, verbose, sh_file, sh_data, None,
                                               "Revert creating a new hash",
                                               datetime.datetime.now())
        plan.append(history_action)
        if need_to_write_resources_file:
            plan.append(WriteRevisedResourceFile(ns, verbose, snapshot_resources))
            plan.append(actions.GitAdd(ns, verbose, workspace_dir,
                                       lambda:[ns.snapshot_filename,
                                               sh_file,
                                               snapshot_resources.resource_file]))
        else:
            plan.append(actions.GitAdd(ns, verbose, workspace_dir,
                                       lambda:[ns.snapshot_filename, sh_file]))
    elif need_to_write_resources_file:
        plan.append(actions.GitAdd(ns, verbose, workspace_dir,
                                   [snapshot_resources.resource_file]))

    # handling of lineage
    current_lineage_dir = get_current_lineage_dir(workspace_dir)
    snapshot_lineage_dir = get_snapshot_lineage_dir(workspace_dir, snapshot['hash'])
    if need_to_clear_lineage:
        if isdir(current_lineage_dir):
            plan.append(ClearLineageDir(ns, verbose, current_lineage_dir))
    elif isdir(snapshot_lineage_dir):
        plan.append(CopyLineageFilesToCurrent(ns, verbose, current_lineage_dir,
                                              snapshot_lineage_dir))
    

    tagstr = ', tag=%s' % snapshot['tag'] if snapshot['tag'] else ''
    if creating_new_snapshot:
        desc = "Partial restore of snapshot %s%s, resulting in a new snapshot"% \
                (snapshot['hash'], tagstr)
        commit_msg_fn = lambda: desc + " " + ns.snapshot_hash
    else:
        desc = "Restore snapshot %s%s" % (snapshot['hash'], tagstr)
        commit_msg_fn = lambda: desc

    if need_to_write_resources_file or creating_new_snapshot:
        plan.append(actions.GitCommit(ns, verbose, workspace_dir,
                                      commit_message=commit_msg_fn))
    click.echo(desc)
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
    if creating_new_snapshot:
        click.echo("New snapshot is %s." % ns.snapshot_hash)
    
