# Copyright 2018,2019 by MPI-SWS and Data-ken Research. Licensed under Apache 2.0. See LICENSE.txt.

from typing import Optional

import click

from dataworkspaces.utils.hash_utils import \
    is_a_git_hash, is_a_shortened_git_hash
from dataworkspaces.errors import ConfigurationError, UserAbort
from dataworkspaces.workspace import Workspace, SnapshotMetadata



_CONF_MESSAGE=\
"A snapshot with this hash already exists. Do you want to update "+\
"the message from '%s' to '%s'?"

def merge_snapshot_metadata(old, new, batch):
    """Merge two snapshot metadatas for when someone creates
    a snapshot without making changes. They might have
    added more tags or changed the message.
    """
    assert old.hashval == new.hashval
    tags = old.tags + [tag for tag in new.tags
                       if tag not in old.tags]
    if old.message!=new.message and (new.message is not None) \
       (new.message!='') and (not batch) and \
       click.confirm(_CONF_MESSAGE%(old.message, new.message)):
        message = new.message
    else:
        message = old.message
    return SnapshotMetadata(
        hashval=old.hashval,
        tags=tags,
        message=message,
        hostname=old.hostname,
        timestamp=old.timestamp,
        relative_detination_path=old.relative_destination_path,
        metric_name=old.metric_name,
        metric_value=old.metric_value,
        updated_timestamp=new.timestamp
    )

# XXX Need to add back in lineage!
# class SaveLineageData(actions.Action):
#     @actions.requires_from_ns('snapshot_hash', str)
#     @actions.requires_from_ns('map_of_hashes', dict)
#     @actions.provides_to_ns('lineage_files', list)
#     def __init__(self, ns, verbose, workspace_dir, resource_names,
#                  results_resources, rel_dest_root):
#         super().__init__(ns, verbose)
#         self.workspace_dir = workspace_dir
#         self.current_lineage_dir = get_current_lineage_dir(workspace_dir)
#         self.resource_names = resource_names
#         self.results_resources = results_resources
#         self.rel_dest_root = rel_dest_root
#         if not isdir(self.current_lineage_dir):
#             self.num_files = 0
#         else:
#             currfiles= set(LineageStoreCurrent.get_resource_names_in_fsstore(
#                            self.current_lineage_dir))
#             self.num_files = len(currfiles.intersection(set(resource_names)))

#     def has_lineage_files(self):
#         return self.num_files > 0

#     def run(self):
#         assert self.has_lineage_files()
#         snapshot_hash = self.ns.snapshot_hash
#         map_of_hashes = self.ns.map_of_hashes
#         store = LineageStoreCurrent.load(self.current_lineage_dir)
#         store.replace_placeholders_with_real_certs(map_of_hashes)
#         store.save(self.current_lineage_dir)
#         for rr in self.results_resources:
#             (lineages, complete) = store.get_lineage_for_resource(rr.name)
#             if len(lineages)>0:
#                 data = {'resource_name':rr.name,
#                         'complete':complete,
#                         'lineages':[l.to_json() for l in lineages]}
#                 rr.add_results_file_from_buffer(json.dumps(data, indent=2),
#                                                 join(self.rel_dest_root,
#                                                      'lineage.json'))
#         lineage_dir = get_snapshot_lineage_dir(self.workspace_dir, snapshot_hash)
#         os.makedirs(lineage_dir)
#         (dest_files, warnings) =\
#             LineageStoreCurrent.copy_fsstore_to_snapshot(self.current_lineage_dir,
#                                                          lineage_dir,
#                                                          self.resource_names)
#         self.ns.lineage_files = dest_files
#         # We need to invalidate the resource lineage for any results,
#         # as we've moved the data to a subdirectory
#         if len(self.results_resources)>0:
#             LineageStoreCurrent.invalidate_fsstore_entries(self.current_lineage_dir,
#                                                            [rr.name for rr in self.results_resources])

#     def __str__(self):
#         return "Copy lineage %d files from current workspace to snapshot lineage" % \
#             self.num_files



def snapshot_command(workspace:Workspace, tag:Optional[str]=None, message:str=''):
    if (tag is not None) and (is_a_git_hash(tag) or is_a_shortened_git_hash(tag)):
        raise ConfigurationError("Tag '%s' looks like a git hash. Please pick something else." % tag)
    # Remove existing tag if present
    if tag is not None:
        try:
            existing_tag_md = workspace.get_snapshot_by_tag(tag)
        except ConfigurationError:
            existing_tag_md = None
        if existing_tag_md is not None:
            msg = "Tag '%s' already exists for snapshot %s taken %s"%\
                                         (tag, existing_tag_md['hash'],
                                          existing_tag_md['timestamp'])
            if workspace.batch:
                raise ConfigurationError(msg)
            elif not click.confirm(msg + ". Remove this tag so we can add it to the new snapshot?"):
                raise UserAbort()
            else:
                workspace.remove_tag_from_snapshot(existing_tag_md.hashval, tag)

    (md, manifest) = workspace.snapshot(tag, message)

    try:
        old_md = workspace.get_snapshot_metadata(md.hashval)
    except:
        old_md = None
    if old_md is not None:
        md = merge_snapshot_metadata(old_md, md, workspace.batch)

    workspace.save_snapshot_metadata_and_manifest(md, manifest)

    # XXX add back in lineage
    # # see if we need to add lineage files
    # save_lineage = SaveLineageData(ns, verbose, workspace_dir, resource_names,
    #                                results_resources, rel_dest_root)
    # if save_lineage.has_lineage_files():
    #     plan.append(save_lineage)
    #     plan.append(actions.GitAdd(ns, verbose, workspace_dir,
    #                                actions.NamespaceRef('lineage_files', list, ns)))
    # plan.append(actions.GitCommit(ns, verbose, workspace_dir,
    #                               commit_message=lambda:"Snapshot "+ns.snapshot_hash))
    if tag:
        click.echo("Have successfully taken snapshot of workspace, tagged with '%s', hash is %s." %
                   (tag, md.hashval))
    else:
        click.echo("Have successfully taken snapshot of workspace, hash is %s." % md.hashval)
    return md.hashval


