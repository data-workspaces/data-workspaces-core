# Copyright 2018,2019 by MPI-SWS and Data-ken Research. Licensed under Apache 2.0. See LICENSE.txt.
import click
from typing import Optional, cast, Dict, List, Any, Tuple

assert Dict and List and Any  # for pyflakes
from collections import Counter
from os.path import join
import sys

from dataworkspaces.workspace import (
    RESOURCE_ROLE_CHOICES,
    Workspace,
    SnapshotWorkspaceMixin,
    JSONDict,
    ResourceRoles,
    FileResourceMixin,
)
from dataworkspaces.errors import ConfigurationError
from dataworkspaces.utils.print_utils import print_columns, ColSpec
from dataworkspaces.utils.lineage_utils import make_lineage_table


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


def report_history_command(workspace: Workspace, limit: Optional[int] = None):
    if not isinstance(workspace, SnapshotWorkspaceMixin):
        raise ConfigurationError(
            "Workspace %s cannot perform snapshots, history not available" % workspace.name
        )
    else:
        print_snapshot_history(
            cast(SnapshotWorkspaceMixin, workspace), reverse=True, max_count=limit
        )


def report_status_command(workspace: Workspace):
    print("Status for workspace: %s" % workspace.name)
    print_resource_status(workspace)


def report_lineage_command(workspace: Workspace, tag_or_hash: Optional[str] = None):
    snapshot_hash = None  # type: Optional[str]
    if tag_or_hash is not None:
        md = workspace.as_snapshot_ws().get_snapshot_by_tag_or_hash(tag_or_hash)
        snapshot_hash = md.hashval
        title = "Lineage for %s" % tag_or_hash
    else:
        title = "Lineage for current state"
    refs = []
    ltypes = []
    details = []
    inputs = []  # type:List[Optional[str]]
    for (ref, ltype, detail, input_list) in make_lineage_table(
        workspace.get_instance(), workspace.as_lineage_ws().get_lineage_store(), snapshot_hash
    ):
        refs.append(ref)
        ltypes.append(ltype)
        details.append(detail)
        if input_list is not None:
            inputs.append("\n".join(input_list))
        else:
            inputs.append(None)
    print_columns(
        {"Resource": refs, "Type": ltypes, "Details": details, "Inputs": inputs},
        title=title,
        paginate=False,
    )


def _find_results_file_if_present(
    workspace: Workspace, subpath: str, resource_name: Optional[str] = None
) -> Optional[Tuple[JSONDict, str]]:
    if resource_name is not None:
        check_resources = [resource_name]
    else:
        check_resources = [
            rn
            for rn in workspace.get_resource_names()
            if workspace.get_resource_role(rn) == ResourceRoles.RESULTS
        ]
    for rn in check_resources:
        resource = workspace.get_resource(rn)
        if not isinstance(resource, FileResourceMixin):
            continue
        if not resource.does_subpath_exist(subpath, must_be_file=True):
            continue
        return (resource.read_results_file(subpath), "%s:/%s" % (rn, subpath))
    return None  # not found


def _get_results(
    workspace: Workspace, tag_or_hash: Optional[str] = None, resource_name: Optional[str] = None
) -> Optional[Tuple[JSONDict, str]]:
    """Get a results file as a parsed json dict. If no resource or snapshot
    is specified, searches all the results resources for a file. If a snapshot
    is specified, we look in the subdirectory where the resuls have been moved.
    If no snapshot is specified, and we don't find a file, we look in the most
    recent snapshot.

    Returns a tuple with the results and the logical path (resource:/subpath) to
    the results. If nothing is found, returns None.
    """
    if tag_or_hash is not None:
        if not isinstance(workspace, SnapshotWorkspaceMixin):
            raise ConfigurationError("Workspace %s does not support snapshots" % workspace.name)
        md = workspace.as_snapshot_ws().get_snapshot_by_tag_or_hash(tag_or_hash)
        subpath = join(md.relative_destination_path, "results.json")
        return _find_results_file_if_present(workspace, subpath, resource_name)
    else:
        result = _find_results_file_if_present(workspace, "results.json", resource_name)
        if result is not None:
            return result
        # not found - ok, try the snapshot
        if not isinstance(workspace, SnapshotWorkspaceMixin):
            return None
        print(
            "Did not find a results.json file in current workspace, checking most recent snapshot...",
            file=sys.stderr,
        )
        rmd = workspace.get_most_recent_snapshot()
        if rmd is not None:
            subpath = join(rmd.relative_destination_path, "results.json")
            return _find_results_file_if_present(workspace, subpath, resource_name)
        else:
            return None


def report_results_command(
    workspace: Workspace, tag_or_hash: Optional[str] = None, resource_name: Optional[str] = None
):
    results = _get_results(workspace, tag_or_hash, resource_name)
    if results is None:
        msg = "Could not find results file"
        if tag_or_hash:
            msg += " for snapshot %s" % tag_or_hash
        if resource_name:
            msg += " for resource %s" % resource_name
        raise ConfigurationError(msg)
    else:
        (data, path) = results
        click.echo("Results file at %s" % path)

        def print_dict_as_table(d, name, serialize_dicts=False):
            keys = []
            values = []
            for (k, v) in d.items():
                if not isinstance(v, dict):
                    keys.append(k)
                    values.append(v)
                elif serialize_dicts:
                    keys.append(k)
                    values.append(", ".join(["%s: %s" % (mk, mv) for (mk, mv) in v.items()]))
            click.echo()
            print_columns({"Key": keys, "Value": values}, paginate=False, title=name)

        print_dict_as_table(data, "General Properties")
        if "parameters" in data:
            print_dict_as_table(data["parameters"], "Parameters")
        if "metrics" in data:
            print_dict_as_table(data["metrics"], "Metrics")
            for (k, v) in data["metrics"].items():
                if isinstance(v, dict):
                    print_dict_as_table(v, "Metrics: " + k, serialize_dicts=True)
