# Copyright 2018,2019 by MPI-SWS and Data-ken Research. Licensed under Apache 2.0. See LICENSE.txt.

import click

from dataworkspaces.workspace import Workspace, SnapshotWorkspaceMixin
from dataworkspaces.errors import ConfigurationError


# XXX port ineage
# def lineage_files_for_snapshot(workspace_dir, snapshot):
#     lineage_dir = get_snapshot_lineage_dir(workspace_dir, snapshot)
#     if isdir(lineage_dir):
#         return {n:join(lineage_dir, n)
#                 for n in os.listdir(lineage_dir) if n.endswith('.json')}
#     else:
#         return {}

# def compare_lineage_files(f1, f2):
#     """Compare two lineage files. Returns a mesasge indicating
#     whether there were differences, and if so, where.
#     """
#     with open(f1, 'r') as f:
#         data1 = json.load(f)
#     with open(f2, 'r') as f:
#         data2 = json.load(f)
#     differences = ''
#     assert data1['step_name']==data2['step_name']
#     for key in ['command_path', 'args', 'cwd', 'timestamp']:
#         v1 = data1[key]
#         v2 = data2[key]
#         if v1==v2:
#             continue
#         differences += "    Values are different for %s:\n" % key
#         differences += "      %s\n" % repr(v1)
#         differences += "      %s\n" % repr(v2)
#     if len(differences)>0:
#         return '  Step %s has lineage differences:\n' % data1['step_name'] + differences
#     else:
#         return '  Step %s has no lineage differences.\n' % data1['step_name']


def diff_command(workspace: Workspace, snapshot_or_tag1: str, snapshot_or_tag2: str) -> None:
    if not isinstance(workspace, SnapshotWorkspaceMixin):
        raise ConfigurationError(
            "Diff command not supported for workspace %s as backend does not support snapshots"
            % workspace.name
        )
    md1 = workspace.get_snapshot_by_tag_or_hash(snapshot_or_tag1)
    manifest1 = {r["name"]: r for r in workspace.get_snapshot_manifest(md1.hashval)}
    snstr1 = "%s, tags %s" % (md1.hashval, ",".join(md1.tags)) if len(md1.tags) > 0 else md1.hashval
    sn1_names = frozenset([n for n in manifest1.keys()])
    md2 = workspace.get_snapshot_by_tag_or_hash(snapshot_or_tag2)
    manifest2 = {r["name"]: r for r in workspace.get_snapshot_manifest(md2.hashval)}
    snstr2 = "%s, tags %s" % (md2.hashval, ",".join(md2.tags)) if len(md2.tags) > 0 else md2.hashval
    sn2_names = frozenset([n for n in manifest2.keys()])

    click.echo("Comparing:\n    Snapshot %s to\n    Snapshot %s" % (snstr1, snstr2))
    common_names = sn1_names.intersection(sn2_names)
    same_resources = []
    different_resources = []
    for name in sorted(common_names):
        h1 = manifest1[name]["hash"]
        h2 = manifest2[name]["hash"]
        if h1 == h2:
            same_resources.append(name)
        else:
            different_resources.append(name)
    if len(same_resources) > 0:
        click.echo("  Resources with the same value:")
        for name in same_resources:
            click.echo("    " + name)
    else:
        click.echo("  Resources with the same value: None")
    if len(different_resources) > 0:
        click.echo("  Resources with different values:")
        for name in different_resources:
            click.echo("    " + name)
    else:
        click.echo("  Resources with different values: None")
    added_resources = sorted(sn2_names.difference(sn1_names))
    if len(added_resources) > 0:
        click.echo("  Added resources:")
        for name in added_resources:
            click.echo("    " + name)
    else:
        click.echo("  Added resources: None")
    removed_resources = sorted(sn1_names.difference(sn2_names))
    if len(removed_resources) > 0:
        click.echo("  Removed resources:")
        for name in removed_resources:
            click.echo("    " + name)
    else:
        click.echo("  Removed resources: None")

    # XXX: port lineage
    # sn1_lineage_files = lineage_files_for_snapshot(workspace_dir, snapshot1)
    # sn1_basenames = set(sn1_lineage_files.keys())
    # sn2_lineage_files = lineage_files_for_snapshot(workspace_dir, snapshot2)
    # sn2_basenames = set(sn2_lineage_files.keys())
    # common_basenames = sn1_basenames.intersection(sn2_basenames)
    # for basename in common_basenames:
    #     print(compare_lineage_files(sn1_lineage_files[basename], sn2_lineage_files[basename]),end='')
    # added_basenames = sn2_basenames.difference(sn1_basenames)
    # if len(added_basenames)>0:
    #     click.echo("  Added lineage steps: %s" % ', '.join([basename.replace('.json', '')
    #                                                         for basename in added_basenames]))
    # else:
    #     click.echo("  No added lineage steps.")
    # removed_basenames = sn1_basenames.difference(sn2_basenames)
    # if len(removed_basenames)>0:
    #     click.echo("  Removed lineage steps: %s" % ', '.join([basename.replace('.json', '')
    #                                                         for basename in removed_basenames]))
    # else:
    #     click.echo("  No removed lineage steps.")
