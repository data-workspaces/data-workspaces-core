import json
from os.path import join, exists

import dataworkspaces.commands.actions as actions
from dataworkspaces.errors import ConfigurationError, InternalError
from dataworkspaces.resources.resource import CurrentResources, SnapshotResources
from .snapshot import TakeResourceSnapshot, AppendSnapshotHistory

class RestoreResource(actions.Action):
    def __init__(self, verbose, resource, snapshot_resources):
        super().__init__(verbose)
        self.resource = resource
        self.hashval = snapshot_resources.url_to_hashval[resource.url]
        self.resource.restore_prechecks(self.hashval)

    def run(self):
        self.resource.restore(self.hashval)

    def __str__(self):
        return "Run restore actions for %s" % str(self.resource)

class SkipResource(actions.Action):
    def __init__(self, verbose, resource, reason):
        super().__init__(verbose)
        self.resource = resource
        self.reason = reason

    def run(self):
        pass

    def __str__(self):
        return 'Skipping resource %s, %s' % (str(self.resource), self.reason)

class AddResourceToSnapshot(actions.Action):
    def __init__(self, verbose, resource, snapshot_resources):
        super().__init__(verbose)
        self.resource = resource
        self.snapshot_resources = snapshot_resources
        # A given resource should resolve to a unique URL, so this is the best way
        # to check for duplication.
        if resource.url in snapshot_resources.urls:
            raise ConfigurationError("A resource with url '%s' already in snapshot" % resource.url)

    def run(self):
        self.snapshot_resources.add_resource(self.resource)
        self.snapshot_resources.write_snapshot_resources()

    def __str__(self):
        return "Add '%s' to resources.json file" % str(self.resource)

class WriteRevisedSnapshotFile(actions.Action):
    def __init__(self, verbose, workspace_dir, map_of_hashes, snapshot_resources):
        self.verbose = verbose
        self.workspace_dir = workspace_dir
        self.map_of_hashes = map_of_hashes
        self.snapshot_resources = snapshot_resources
        self.snapshot_hash = None
        self.snapshot_filename = None

    def run(self):
        def write_fn(tempfile):
            self.snapshot_resources.write_revised_snapshot_manifest(tempfile,
                                                                   self.map_of_hashes)
        (self.snapshot_hash, self.snapshot_filename) = \
            actions.write_and_hash_file(
                write_fn,
                join(self.workspace_dir,
                     ".dataworkspace/snapshots/snapshot-<HASHVAL>.json"),
                self.verbose)

    def __str__(self):
        return 'Create and hash snapshot file'

def restore_command(workspace_dir, batch, verbose, tag_or_hash,
                    ignore_dropped=False):
    # First, find the history entry
    sh_file = join(workspace_dir, '.dataworkspace/snapshots/snapshot_history.json')
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
    snapshot_names = snapshot_resources.get_names()
    current_names = current_resources.get_names()
    common_names = sorted(snapshot_names.intersection(current_names))
    names_to_add = sorted(snapshot_names.difference(current_names))
    names_to_ignore = sorted(current_names.difference(snapshot_names))
    plan = []
    create_new_hash = False
    map_of_hashes = {}
    for name in common_names:
        plan.append(RestoreResource(verbose, current_resources.by_name[name],
                                    snapshot_resources))
    for name in names_to_add:
        # These are resources which are in the restored snapshot, but not the
        # current resources. We'll grab the resource objects from snapshot_resources
        plan.append(RestoreResource(verbose, snapshot_resources.by_name[name],
                                    snapshot_resources))
    for name in names_to_ignore:
        # These resources are only in the current resource list.
        r = current_resources.by_name[name]
        if ignore_dropped:
            plan.append(SkipResource(verbose, r, "it is not in restored snapshot"))
        else:
            # if we are adding a current resource to the restored snapshot, we actually
            # have to snapshot the resource itself.
            r = current_resources.by_name[name]
            plan.append(AddResourceToSnapshot(verbose, r, snapshot_resources))
            plan.append(TakeResourceSnapshot(verbose, r, map_of_hashes))
            create_new_hash = True
    if create_new_hash:
        write_revised = WriteRevisedSnapshotFile(verbose, workspace_dir, map_of_hashes,
                                                 snapshot_resources)
        plan.append(write_revised)
        history_action = AppendSnapshotHistory(verbose, workspace_dir, None,
                                               "Revert creating a new hash",
                                               lambda:write_revised.snapshot_hash)
        plan.append(history_action)
        plan.append(actions.GitAddDeferred(workspace_dir,
                                           lambda:[write_revised.snapshot_filename,
                                                   history_action.snapshot_history_file],
                                           verbose))
        plan.append(actions.GitCommit(workspace_dir,
                                      message=lambda:"Snapshot "+
                                                     (lambda h:h.hash_value)(hash_action),
                                      verbose=verbose))

    tagstr = ', tag=%s' % snapshot['tag'] if snapshot['tag'] else ''
    if not create_new_hash:
        print("Revert to snapshot %s%s" % (snapshot['hash'], tagstr))
    else:
        print("Modified revert to snapshot %s%s, resulting in a new snapshot"%
              (snapshot['hash'], tagstr))
