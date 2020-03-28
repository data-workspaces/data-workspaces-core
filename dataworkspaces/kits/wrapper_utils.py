"""
Common utils for wrapping objects with the Lineage API.
"""
import datetime
from typing import Optional, Union, cast, Dict
from os.path import exists

from dataworkspaces.workspace import Workspace, ResourceRoles, ResourceRef
from dataworkspaces.utils.lineage_utils import LineageError, infer_step_name
from dataworkspaces.kits.jupyter import get_step_name_for_notebook
from dataworkspaces.lineage import ResultsLineage
from dataworkspaces.resources.api_resource import API_RESOURCE_TYPE, ApiResource
from dataworkspaces.errors import ConfigurationError

import numpy as np

try:
    import pandas
except ImportError:
    pandas = None

try:
    import tensorflow
except ImportError:
    tensorflow = None  # type: ignore


class NotSupportedError(ConfigurationError):
    """Thrown when a wrapper encounters an unsupported configuration.
    """

    pass


def _infer_step_name() -> str:
    """Come up with a step name by looking at whether this is a notebook
    and then the command line arguments.
    """
    try:
        notebook_name = get_step_name_for_notebook()
        if notebook_name is not None:
            return notebook_name
    except:
        pass  # not a notebook
    return infer_step_name()


def _metric_scalar_to_json(v):
    if isinstance(v, int) or isinstance(v, str):
        return v
    elif isinstance(v, np.int64) or isinstance(v, np.int32):
        return int(v)
    elif isinstance(v, np.float64) or isinstance(v, np.float32):
        return float(v)
    elif isinstance(v, datetime.datetime):
        return v.isoformat()
    else:
        return v


def _metric_obj_to_json(v):
    if isinstance(v, dict):
        return {k: _metric_obj_to_json(vi) for (k, vi) in v.items()}
    elif isinstance(v, list) or isinstance(v, tuple):
        return [_metric_obj_to_json(vi) for vi in v]
    else:
        return _metric_scalar_to_json(v)


def _add_to_hash(array_data, hash_state):
    if isinstance(array_data, np.ndarray):
        hash_state.update(array_data.data)
    elif (pandas is not None) and isinstance(array_data, pandas.DataFrame):
        for c in array_data.columns:
            hash_state.update(array_data[c].to_numpy(copy=False).data)
    elif (pandas is not None) and isinstance(array_data, pandas.Series):
        hash_state.update(array_data.to_numpy(copy=False).data)
    elif isinstance(array_data, tuple) or isinstance(array_data, list):
        # Tensorflow frequently puts the parts of a dataset in a tuple.
        # For example: (features, labels)
        for element in array_data:
            _add_to_hash(element, hash_state)
    elif isinstance(array_data, dict):
        # Tensorflow uses a dict (specifically OrderedDict) to store
        # the columns of a CSV.
        for column in array_data.values():
            _add_to_hash(column, hash_state)
    elif (tensorflow is not None) and isinstance(array_data, tensorflow.data.Dataset):  # type: ignore
        # We need to iterate through the dataset, to force an eager evaluation
        for t in array_data:
            _add_to_hash(t, hash_state)
    elif (tensorflow is not None) and isinstance(array_data, tensorflow.Tensor):  # type: ignore
        if hasattr(array_data, "numpy"):
            _add_to_hash(array_data.numpy(), hash_state)
        else:
            raise Exception(
                "Tensor type %s is not in eager mode, cannot convert to numpy, value was: %s"
                % (type(array_data), repr(array_data))
            )
    elif (
        isinstance(array_data, np.uint8)
        or isinstance(array_data, np.int8)
        or isinstance(array_data, np.int32)
        or isinstance(array_data, np.int64)
    ):
        hash_state.update(bytes(int(array_data)))
    else:
        raise Exception(
            "Unable to hash data type %s, data was: %s" % (type(array_data), array_data)
        )


def _find_resource(
    workspace: Workspace, role: str, name_or_ref: Optional[Union[str, ResourceRef]] = None
) -> ResourceRef:
    resource_names = [n for n in workspace.get_resource_names()]
    if isinstance(name_or_ref, str):
        if (
            (not name_or_ref.startswith("./"))
            and (not name_or_ref.startswith("/"))
            and (name_or_ref in resource_names)
        ):
            return ResourceRef(name_or_ref)
        elif exists(name_or_ref):
            return workspace.map_local_path_to_resource(
                name_or_ref, expecting_a_code_resource=False
            )
        else:
            raise LineageError(
                "Could not find a resource for '"
                + name_or_ref
                + "' with role '"
                + role
                + "' in your workspace. Please create a resource"
                + " using the 'dws add' command or correct the name. "
                + "Currently defined resources are: "
                + ", ".join(
                    ["%s (role %s)" % (n, workspace.get_resource_role(n)) for n in resource_names]
                )
                + "."
            )
    elif isinstance(name_or_ref, ResourceRef):
        workspace.validate_resource_name(name_or_ref.name, name_or_ref.subpath)
        return name_or_ref
    else:
        # no resource specified. If we have exactly one for that role,
        # we will use it
        resource_for_role = None
        for rname in workspace.get_resource_names():
            if workspace.get_resource_role(rname) == role:
                if resource_for_role is None:
                    resource_for_role = ResourceRef(rname, subpath=None)
                else:
                    raise LineageError(
                        "There is more than one resource for role "
                        + role
                        + " in your workspace. Please specify the resource you want"
                        + " in model wrapping function or use a wrapped data set"
                    )
        if resource_for_role is not None:
            return resource_for_role
        else:
            raise LineageError(
                "Could not find a "
                + role
                + " resource in your workspace. Please create a resource"
                + " using the dws add command."
            )


class _DwsModelState:
    def __init__(
        self,
        workspace: Workspace,
        input_resource: Optional[Union[str, ResourceRef]] = None,
        results_resource: Optional[Union[str, ResourceRef]] = None,
    ):
        self.workspace = workspace
        self.results_ref = _find_resource(workspace, ResourceRoles.RESULTS, results_resource)
        self.default_input_resource = input_resource
        self.api_resource_cache = {}  # type: Dict[str,ApiResource]
        self.lineage = ResultsLineage(
            _infer_step_name(), datetime.datetime.now(), {}, [], [], self.results_ref, workspace
        )

    def find_input_resources_and_return_if_api(
        self, data, target_data=None
    ) -> Optional[ApiResource]:
        if hasattr(data, "resource"):
            ref = data.resource
        else:
            ref = _find_resource(
                self.workspace, ResourceRoles.SOURCE_DATA_SET, self.default_input_resource
            )
        self.lineage.add_input_ref(ref)
        data_resource_type = self.workspace.get_resource_type(ref.name)
        if target_data is not None and hasattr(target_data, "resource"):
            target_ref = data.resource
            if target_ref != ref:  # only can happen if resource is specified on data
                if (
                    data_resource_type == API_RESOURCE_TYPE
                    or self.workspace.get_resource_type(target_ref.name) == API_RESOURCE_TYPE
                ):
                    raise NotSupportedError(
                        "Currently, we do not support API Resources where the feature and target data are from different resources (%s and %s)."
                        % (ref, target_ref)
                    )
                self.lineage.add_input_ref(target_ref)
        if data_resource_type == API_RESOURCE_TYPE:
            if ref.name not in self.api_resource_cache:
                self.api_resource_cache[ref.name] = cast(
                    ApiResource, self.workspace.get_resource(ref.name)
                )
            return self.api_resource_cache[ref.name]
        else:
            return None

    def write_metrics_and_complete(self, metrics):
        metrics = _metric_obj_to_json(metrics)
        if self.workspace.verbose:
            print("dws>> Metrics: %s" % repr(metrics))
        self.lineage.write_results(metrics)
        self.lineage.complete()

    def reset_lineage(self):
        """If you are rerunning a step, call this to reset the start and execution
        times as well as the in_progress marker in the lineage.
        """
        self.lineage.step.execution_time_seconds = None
        self.lineage.step.start_time = datetime.datetime.now()
        self.lineage.in_progress = True
