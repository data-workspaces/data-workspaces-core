# Copyright 2018,2019 by MPI-SWS and Data-ken Research. Licensed under Apache 2.0. See LICENSE.txt.
import click
from typing import Optional, cast, Dict, List, Any

assert Dict and List and Any  # for pyflakes
from collections import Counter

from dataworkspaces.workspace import RESOURCE_ROLE_CHOICES, Workspace, SnapshotWorkspaceMixin
from dataworkspaces.utils.print_utils import print_columns, ColSpec


METRIC_NAME_WIDTH = 18
METRIC_VAL_WIDTH = 12

NUM_METRICS = 2


def print_snapshot_history(
    workspace: SnapshotWorkspaceMixin, reverse: bool = True, max_count: Optional[int] = None
):
    history = workspace.list_snapshots(reverse, max_count)
    # find the most common metrics
    mcounter = Counter()  # type: Counter
    for md in history:
        if md.metrics is not None:
            mcounter.update(md.metrics.keys())
    metric_names = [m for (m, cnt) in mcounter.most_common(NUM_METRICS)]
    spec = {
        "Hash": ColSpec(width=8),
        "Tags": ColSpec(width=20),
        "Created": ColSpec(width=19),
        "Message": ColSpec(width=30),
    }
    hashes = []  # type: List[str]
    tags = []  # type: List[str]
    created = []  # type: List[str]
    metrics = {n: [] for n in metric_names}  # type: Dict[str,List[Any]]
    messages = []  # type: List[str]

    returned = 0
    for md in history:
        hashes.append(md.hashval[0:7])
        tags.append(", ".join(md.tags))
        created.append(md.timestamp[0:-7])
        messages.append(md.message)
        for m in metric_names:
            metrics[m].append(md.metrics[m] if md.metrics is not None and m in md.metrics else None)
        returned += 1
    columns = {"Hash": hashes, "Tags": tags, "Created": created}
    for m in metric_names:
        columns[m] = metrics[m]
        spec[m] = ColSpec(width=25, truncate=True)
    columns["Message"] = messages
    click.echo("\n")
    print_columns(columns, null_value="", spec=spec, paginate=False, title="History of snapshots")
    if max_count is not None and returned == max_count:
        click.echo("Showing first %d snapshots" % max_count)
    else:
        click.echo("%d snapshots total" % returned)


def print_resource_status(workspace: Workspace):
    names_by_role = {role: [] for role in RESOURCE_ROLE_CHOICES}  # type:Dict[str,List[str]]
    resource_names = []
    roles = []
    types = []
    params = []
    missing_roles = []
    # we are going to order resources by role
    for rname in workspace.get_resource_names():
        role = workspace.get_resource_role(rname)
        names_by_role[role].append(rname)
    for role in RESOURCE_ROLE_CHOICES:
        if len(names_by_role[role]) > 0:
            for rname in names_by_role[role]:
                resource_names.append(rname)
                roles.append(role)
                types.append(workspace.get_resource_type(rname))
                params.append(
                    ",\n".join(
                        [
                            "%s=%s" % (pname, pval)
                            for (pname, pval) in workspace._get_resource_params(rname).items()
                            if pname not in ("resource_type", "name", "role")
                        ]
                    )
                )
        else:
            missing_roles.append(role)
    print_columns(
        {"Resource": resource_names, "Role": roles, "Type": types, "Parameters": params},
        # spec={'Parameters':ColSpec(width=40)},
        null_value="",
        title="Resources for workspace: %s" % workspace.name,
        paginate=False,
    )
    if len(missing_roles) > 0:
        click.echo("No resources for the following roles: %s." % ", ".join(missing_roles))


def status_command(workspace: Workspace, history: bool, limit: Optional[int] = None):
    print("Status for workspace: %s" % workspace.name)
    print_resource_status(workspace)
    if history:
        if not isinstance(workspace, SnapshotWorkspaceMixin):
            click.echo(
                "Workspace %s cannot perform snapshots, ignoring --history option" % workspace.name,
                err=True,
            )
        else:
            print_snapshot_history(
                cast(SnapshotWorkspaceMixin, workspace), reverse=True, max_count=limit
            )
