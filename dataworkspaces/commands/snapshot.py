# Copyright 2018,2019 by MPI-SWS and Data-ken Research. Licensed under Apache 2.0. See LICENSE.txt.

from typing import Optional, cast

import click

from dataworkspaces.utils.hash_utils import \
    is_a_git_hash, is_a_shortened_git_hash
from dataworkspaces.errors import ConfigurationError, UserAbort
from dataworkspaces.workspace import Workspace, SnapshotMetadata, SnapshotWorkspaceMixin



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



def snapshot_command(workspace:Workspace, tag:Optional[str]=None, message:str='') -> str:
    if (tag is not None) and (is_a_git_hash(tag) or is_a_shortened_git_hash(tag)):
        raise ConfigurationError("Tag '%s' looks like a git hash. Please pick something else." % tag)

    if not isinstance(workspace, SnapshotWorkspaceMixin):
        raise ConfigurationError("Workspace %s does not support snapshots." % workspace.name)
    mixin = cast(SnapshotWorkspaceMixin, workspace)
    # Remove existing tag if present
    if tag is not None:
        try:
            existing_tag_md = mixin.get_snapshot_by_tag(tag)
        except ConfigurationError:
            existing_tag_md = None
        if existing_tag_md is not None:
            msg = "Tag '%s' already exists for snapshot %s taken %s"%\
                                         (tag, existing_tag_md.hashval,
                                          existing_tag_md.timestamp)
            if workspace.batch:
                raise ConfigurationError(msg)
            elif not click.confirm(msg + ". Remove this tag so we can add it to the new snapshot?"):
                raise UserAbort()
            else:
                mixin.remove_tag_from_snapshot(existing_tag_md.hashval, tag)

    (md, manifest) = mixin.snapshot(tag, message)

    try:
        old_md = mixin.get_snapshot_metadata(md.hashval)
    except:
        old_md = None
    if old_md is not None:
        md = merge_snapshot_metadata(old_md, md, workspace.batch)

    mixin.save_snapshot_metadata_and_manifest(md, manifest)
    workspace.save("Completed snapshot %s" % md.hashval)

    if tag:
        click.echo("Have successfully taken snapshot of workspace, tagged with '%s', hash is %s." %
                   (tag, md.hashval))
    else:
        click.echo("Have successfully taken snapshot of workspace, hash is %s." % md.hashval)
    return md.hashval


