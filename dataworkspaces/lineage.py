"""
This module  provides an API for tracking
*data lineage* -- the history of how a given result was created, including the
versions of original source data and the various steps run in the *data pipeline*
to produce the final result.

The basic idea is that your workflow is a sequence of *pipeline steps*::

      ----------     ----------     ----------     ----------
      |        |     |        |     |        |     |        |
      | Step 1 |---->| Step 2 |---->| Step 3 |---->| Step 4 |
      |        |     |        |     |        |     |        |
      ----------     ----------     ----------     ----------

A step could be a command line script, a Jupyter notebook or perhaps
a step in an automated workflow tool (e.g. Apache Airflow).
Each step takes a number of *inputs* and *parameters* and generates *outputs*.
The inputs are resources in your workspace (or subpaths within a resource) from
which the step will read to perform its task. The parameters are configuration
values passed to the step (e.g. the command line arguments of a script). The outputs
are the resources (or subpaths within a resource), which are written to by
the step. The outputs may represent results or intermediate data to be consumed
by downstream steps.

The lineage API captures this data for each step. Here is a view of the data captured::

                                Parameters
                                ||  ||  ||
                                \\/  \\/  \\/
                               ------------
                             =>|          |=>
            Input resources  =>|  Step i  |=> Output resources
                             =>|          |=>
                               ------------
                                    /\\
                                    ||
                                   Code
                               Dependencies


To do this, we need use the following classes:

* :class:`~ResourceRef` - A reference to a resource for use as a step input or output.
  A ResourceRef contains a resource name and an optional path within that resource.
  This lets you manage lineage down to the directory or even file level. The APIs also
  support specifying a path on the local filesystem instead of a ResourceRef. This path
  is automatically resolved to a ResourceRef (it must map to the a location under the
  local path of a resource). By storing :class:`~ResourceRef`s instead of hard-coded
  filesystem paths, we can include non-local resources (like an S3 bucket) and ensure
  that the workspace is easily deployed on a new machine.
* :class:`~Lineage` - The main lineage object, instantiated at the start of your step.
  At the beginning of your step, you specify the inputs, parameters, and outputs. At the
  end of the step, the data is saved, along with any results you might have from that step.
  Lineage instances are
  `context managers <https://docs.python.org/3/reference/datamodel.html#context-managers>`_,
  which means you can use a ``with`` statement to manage their lifecycle.
* :class:`~LineageBuilder` - This is a helper class to guide the creation of your lineage
  object.

**Example**

Here is an example usage of the lineage API in a command line script::

  import argparse 
  from dataworkspaces.lineage import LineageBuilder

  def main():
      ...
      parser = argparse.ArgumentParser()
      parser.add_argument('--gamma', type=float, default=0.01,
                          help="Regularization parameter")
      parser.add_argument('input_data_dir', metavar='INPUT_DATA_DIR', type=str,
                          help='Path to input data')
      parser.add_argument('results_dir', metavar='RESULTS_DIR', type=str,
                          help='Path to where results should be stored')
      args = parser.parse_args()
      ...
      # Create a LineageBuilder instance to specify the details of the step
      # to the lineage API.
      builder = LineageBuilder()\\
                  .as_script_step()\\
                  .with_parameters({'gamma':args.gamma})\\
                  .with_input_path(args.input_data_dir)\\
                  .as_results_step(args.results_dir)
    
      # builder.eval() will construct the lineage object. We call it within a
      # with statement to get automatic save/cleanup when we leave the
      # with block.
      with builder.eval() as lineage:
  
          ... do your work here ...
  
          # all done, write the results
          lineage.write_results({'accuracy':accuracy,
                                 'precision':precision,
                                 'recall':recall,
                                 'roc_auc':roc_auc})
  
      # When leaving the with block, the lineage is automatically saved to the
      # workspace. If an exception is thrown, the lineage is not saved, but the
      # outputs are marked as being in an unknown state.

      return 0

  # boilerplate to call our main function if this is called as a script.
  if __name__ == '__main__:
      sys.exit(main())

"""
import sys
from abc import ABC, abstractmethod
import contextlib
from collections import OrderedDict
import datetime
from typing import List, Union, Any, Type, Iterable, Dict, Optional, cast
from os.path import curdir, join, isabs, abspath, expanduser, exists
from argparse import ArgumentParser, Namespace
from copy import copy

from dataworkspaces.errors import ConfigurationError
from dataworkspaces.workspace import (
    Workspace,
    load_workspace,
    FileResourceMixin,
    PathNotAResourceError,
    SnapshotWorkspaceMixin,
    ResourceRoles,
    _find_containing_workspace,
)
from dataworkspaces.utils.lineage_utils import (
    ResourceRef,
    StepLineage,
    infer_step_name,
    infer_script_path,
    LineageError,
)


##########################################################################
#                   Main lineage API
##########################################################################


class Lineage(contextlib.AbstractContextManager):
    """This is the main object for tracking the execution of a step.
    Rather than instantiating it directly, use the :class:`~LineageBuilder`
    class to construct your :class:`~Lineage` instance.
    """

    def __init__(
        self,
        step_name: str,
        start_time: datetime.datetime,
        parameters: Dict[str, Any],
        inputs: List[Union[str, ResourceRef]],
        code: List[Union[str, ResourceRef]],
        workspace: Workspace,
        command_line: Optional[List[str]] = None,
        current_directory: Optional[str] = None,
    ):
        self.workspace = workspace  # type: Workspace
        self.instance = workspace.get_instance()
        # if not isinstance(workspace, SnapshotWorkspaceMixin) or not workspace.supports_lineage():
        #     raise ConfigurationError("Backend for workspace %s does not support lineage" % workspace.name)
        self.store = cast(SnapshotWorkspaceMixin, workspace).get_lineage_store()
        input_resource_refs = []  # type: List[ResourceRef]
        for r_or_p in inputs:
            if isinstance(r_or_p, ResourceRef):
                workspace.validate_resource_name(r_or_p.name, r_or_p.subpath)
                input_resource_refs.append(r_or_p)
            else:
                ref = workspace.map_local_path_to_resource(r_or_p)
                input_resource_refs.append(ref)
        code_resource_refs = []  # type: List[ResourceRef]
        for r_or_p in code:
            if isinstance(r_or_p, ResourceRef):
                self.workspace.validate_resource_name(
                    r_or_p.name, r_or_p.subpath, expected_role=ResourceRoles.CODE
                )
                code_resource_refs.append(r_or_p)
            else:
                ref = workspace.map_local_path_to_resource(r_or_p, expecting_a_code_resource=True)
                # For now, we will resolve code paths at the resource level.
                # We drop the subpath, unless the user provided it explicitly
                # through a ResourceRef.
                crr = ResourceRef(ref.name, None)
                if crr not in code_resource_refs:
                    code_resource_refs.append(crr)

        # The run_from_directory can be either a resource reference (best),
        # a path on the local filesystem, or None
        try:
            if current_directory is not None:
                if not isabs(current_directory):
                    current_directory = abspath(expanduser((current_directory)))
                run_from_directory = workspace.map_local_path_to_resource(
                    current_directory
                )  # type: Optional[ResourceRef]
            else:
                run_from_directory = None
        except PathNotAResourceError:
            run_from_directory = None

        self.step = StepLineage.make_step_lineage(
            workspace.get_instance(),
            step_name,
            start_time,
            parameters,
            input_resource_refs,
            code_resource_refs,
            self.store,
            command_line=command_line,
            run_from_directory=run_from_directory,
        )
        self.in_progress = True

    def add_input_path(self, path: str) -> None:
        if not exists(path):
            raise LineageError("Path %s does not exist" % path)
        ref = self.workspace.map_local_path_to_resource(path)  # mypy: ignore
        self.step.add_input(self.workspace.get_instance(), self.store, ref)  # mypy: ignore

    def add_input_ref(self, ref: ResourceRef) -> None:
        self.step.add_input(self.workspace.get_instance(), self.store, ref)

    def add_output_path(self, path: str) -> None:
        """Resolve the path to a resource name and subpath. Add
        that to the lineage as an output of the step. From this point on,
        if the step fails (:func:`~abort` is called), the associated resource
        and subpath will be marked as being in an "unknown" state.
        """
        ref = self.workspace.map_local_path_to_resource(path)  # mypy: ignore
        self.step.add_output(self.workspace.get_instance(), self.store, ref)  # mypy: ignore

    def add_output_ref(self, ref: ResourceRef):
        """Add the resource reference to the lineage as an output of the step.
        From this point on, if the step fails (:func:`~abort` is called), the
        associated resource and subpath will be marked as being in an
        "unknown" state.
        """
        self.step.add_output(self.workspace.get_instance(), self.store, ref)

    def add_param(self, name: str, value) -> None:
        """Add or update one of the step's parameters.
        """
        assert self.in_progress  # should only do while step running
        self.step.parameters[name] = value

    def abort(self):
        """The step has failed, so we mark its outputs in an unknown state.
        If you create the lineage via a "with" statement, then this will be
        called for you automatically.
        """
        if not self.in_progress:
            print(
                "WARNING: Lineage.abort() called after complete() or abort() call for %s"
                % self.step.step_name,
                file=sys.stderr,
            )
        else:
            self.in_progress = False
        for output_cert in self.step.output_resources:
            self.store.clear_entry(self.instance, output_cert.ref)

    def _set_execution_time(self):
        """If the execution time has not already been set, and the start timestamp
        was captured, compute and set the exeuction time. This may be called from
        two places: :func:`ResultsResource.write_results` and from :func:`complete`,
        which is called when exiting the lineage's context manager ("with") block.
        Since the user could potentially call both, we only set it the first call.
        Both calls should happen after the real work for the step, so that should
        be ok.
        """
        if self.step.execution_time_seconds is None and self.step.start_time is not None:
            self.step.execution_time_seconds = (
                datetime.datetime.now() - self.step.start_time
            ).total_seconds()

    def complete(self):
        """The step has completed. Save the outputs.
        If you create the lineage via a "with" statement, then this will be
        called for you automatically.
        """
        if not self.in_progress:
            print(
                "WARNING: Lineage.complete() called after complete() or abort() call for %s"
                % self.step.step_name,
                file=sys.stderr,
            )
        else:
            self.in_progress = False
        self._set_execution_time()
        self.store.store_entry(self.instance, self.step)

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is None:
            self.complete()
        else:
            self.abort()
        return False  # don't suppress any exception


class ResultsLineage(Lineage):
    """Lineage for a results step. This subclass is returned by the
    :class:`~LineageBuilder` when :func:`~LineageBuilder.as_results_step` is called.
    This marks the :class:`~Lineage` object as generating results.
    It adds the :func:`~write_results`
    method for writing a JSON summary of the final results.

    Results resources will also have a ``lineage.json`` file added
    when the next snapshot is taken. This file contains the full
    lineage graph collected for the resource.
    """

    def __init__(
        self,
        step_name: str,
        start_time: datetime.datetime,
        parameters: Dict[str, Any],
        inputs: List[Union[str, ResourceRef]],
        code: List[Union[str, ResourceRef]],
        results_dir_or_ref: Union[str, ResourceRef],
        workspace: Workspace,
        run_description: Optional[str] = None,
        command_line: Optional[List[str]] = None,
        current_directory: Optional[str] = None,
    ):
        super().__init__(
            step_name,
            start_time,
            parameters,
            inputs,
            code,
            workspace,
            command_line,
            current_directory,
        )
        if isinstance(results_dir_or_ref, str):
            self.results_ref = self.workspace.map_local_path_to_resource(results_dir_or_ref)
        else:
            self.results_ref = cast(ResourceRef, results_dir_or_ref)
        self.results_resource = self.workspace.get_resource(self.results_ref.name)
        self.add_output_ref(self.results_ref)
        self.run_description = run_description
        if not isinstance(self.results_resource, FileResourceMixin):
            raise ConfigurationError(
                "Resource '%s' does not support a file API and thus won't support writing results."
                % self.results_ref.name
            )

    def write_results(self, metrics: Dict[str, Any]):
        """Write a ``results.json`` file to the results directory
        specified when creating the lineage object (e.g. via
        :func:`~LineageBuilder.as_results_step`).
        This json file contains information
        about the step execution (e.g. start time), parameters,
        and the provided metrics.
        """
        self._set_execution_time()
        data = {
            "step": self.step.step_name,
            "start_time": self.step.start_time.isoformat(),
            "execution_time_seconds": self.step.execution_time_seconds,
            "parameters": self.step.parameters,
            "run_description": self.run_description,
            "metrics": metrics,
        }
        if self.results_ref.subpath is not None:
            results_relpath = join(self.results_ref.subpath, "results.json")
        else:
            results_relpath = "results.json"
        cast(FileResourceMixin, self.results_resource).add_results_file(data, results_relpath)
        print("Wrote results to %s:%s" % (self.results_ref.name, results_relpath))


class LineageBuilder:
    """Use this class to declaratively build :class:`~Lineage` objects. Instantiate
    a LineageBuilder instance, and call a sequence of configuration methods
    to specify your inputs, parameters, your workspace (if the script is not
    already inside the workspace), and whether this is a results step. Each
    configuration method returns the builder, so you can chain them together.
    Finally, call :func:`~eval` to instantiate the :class:`~Lineage` object.

    **Configuration Methods**

    To specify the workflow step's name, call one of:

    * :func:`~as_script_step` - the script's name will be used to infer the step
      and the associated code resource
    * with_step_name - explicitly specify the step name

    To specify the parameters of the step (e.g. command line arguments), use the
    :func:`~with_parameters` method.

    To specify the input of the step call one or more of:

    * :func:`~with_input_path` - resolve the local filesystem path to a resource and
      subpath and add it to the lineage as inputs. May be called more than once.
    * :func:`~with_input_paths` - resolve a list of local filesystem paths to
      resources and subpaths and add them to the lineage as inputs. May be called
      more than once.
    * :func:`~with_input_ref` - add the resource and subpath to the lineage as an input.
      May be called more than once.
    * :func:`~with_no_inputs` - mutually exclusive with the other input methods. This
      signals that there are no inputs to this step.

    To specify code resource dependencies for the step, you can call
    :func:`~with_code_ref`. For command-line Python scripts, the
    main code resource is handled automatically in :func:`~as_script_step`.
    Other subclasses of the LineageBuilder may provide similar functionality
    (e.g. the LineageBuilder for JupyterNotebooks will try to figure out the resource
    containing your notebook and set it in the lineage).

    If you need to specify the workspace's root directory, use the
    :func:`~with_workspace_directory` method. Otherwise, the lineage API will attempt
    to infer the workspace directory by looking at the path of the script.

    Call :func:`~as_results_step` to indicate that this step is producing results.
    This will add a method :func:`~ResultsLineage.write_results` to the :class:`~Lineage` object
    returned by :func:`~eval`. The method :func:`~as_results_step` takes two parameters:
    `results_dir` and, optionally, `run_description`. The results directory should
    correspond to either the root directory of a results resource or a subdirectory
    within the resource. If you have multiple steps of your workflow that produce results,
    you can create separate subdirectories for each results-producing step.

    **Example**

    Here is an example where we build a :class:`~Lineage` object for a script,
    that has one input, and that produces results::

      lineage = LineageBuilder()\\
                  .as_script_step()\\
                  .with_parameters({'gamma':0.001})\\
                  .with_input_path(args.intermediate_data)\\
                  .as_results_step('../results').eval()

    **Methods**
    """

    def __init__(self):
        self.step_name = None  # type: Optional[str]
        self.command_line = None  # type: Optional[List[str]]
        self.current_directory = None  # type: Optional[str]
        self.parameters = None  # type: Optional[Dict[str, Any]]
        self.inputs = None  # type: Optional[List[Union[str, ResourceRef]]]
        self.no_inputs = False  # type: Optional[bool]
        self.code = []  # type: List[Union[str, ResourceRef]]
        self.workspace_dir = None  # type: Optional[str]
        self.results_dir = None  # type: Optional[str]
        self.run_description = None  # type: Optional[str]

    def as_script_step(self) -> "LineageBuilder":
        assert self.step_name is None, "attempting to set step name twice!"
        self.step_name = infer_step_name()
        self.command_line = [sys.executable] + sys.argv
        self.current_directory = curdir
        self.code.append(infer_script_path())
        return self

    def with_step_name(self, step_name: str) -> "LineageBuilder":
        assert self.step_name is None, "attempting to set step name twice!"
        self.step_name = step_name
        return self

    def with_parameters(self, parameters: Dict[str, Any]) -> "LineageBuilder":
        assert self.parameters is None, "attemping to specify parameters twice"
        self.parameters = parameters
        return self

    def with_input_path(self, path: str) -> "LineageBuilder":
        assert self.no_inputs is False, "Cannot specify both inputs and no inputs"
        if self.inputs is None:
            self.inputs = [path]
        else:
            self.inputs.append(path)
        return self

    def with_input_paths(self, paths: List[str]) -> "LineageBuilder":
        assert self.no_inputs is False, "Cannot specify both inputs and no inputs"
        if self.inputs is None:
            self.inputs = cast(Optional[List[Union[str, ResourceRef]]], copy(paths))
        else:
            self.inputs.extend(paths)
        return self

    def with_input_ref(self, ref: ResourceRef) -> "LineageBuilder":
        assert self.no_inputs is False, "Cannot specify both inputs and no inputs"
        if self.inputs is None:
            self.inputs = [ref]
        else:
            self.inputs.append(ref)
        return self

    def with_no_inputs(self) -> "LineageBuilder":
        assert self.inputs is None, "Cannot specify inputs and with_no_inputs()"
        self.no_inputs = True
        return self

    def with_code_path(self, path: str) -> "LineageBuilder":
        self.code.append(path)
        return self

    def with_code_ref(self, ref: ResourceRef) -> "LineageBuilder":
        self.code.append(ref)
        return self

    def with_workspace_directory(self, workspace_dir: str) -> "LineageBuilder":
        load_workspace("git:" + workspace_dir, False, False)
        self.workspace_dir = workspace_dir  # does validation
        return self

    def as_results_step(
        self, results_dir: str, run_description: Optional[str] = None
    ) -> "LineageBuilder":
        assert self.results_dir is None, "attempting to specify results directory twice"
        self.results_dir = results_dir
        self.run_description = run_description
        return self

    def eval(self) -> Lineage:
        """Validate the current configuration, making sure all required
        properties have been specified, and return a :class:`~Lineage` object
        with the requested configuration.
        """
        assert self.step_name is not None, "Need to specify step name"
        assert self.parameters is not None, "Need to specify parameters"
        assert self.no_inputs or (
            self.inputs is not None
        ), "Need to specify either inputs or no inputs"
        inputs = self.inputs if self.inputs is not None else []  # type: List[Union[str, Any]]
        if self.workspace_dir is None:
            self.workspace_dir = _find_containing_workspace()
        if self.workspace_dir is None:
            raise ConfigurationError("Could not find a workspace, starting at %s" % curdir)
        # TODO: need to make this handle other backends as well.
        workspace = load_workspace("git:" + self.workspace_dir, False, False)
        if self.results_dir is not None:
            return ResultsLineage(
                self.step_name,
                datetime.datetime.now(),
                self.parameters,
                inputs,
                self.code,
                self.results_dir,
                workspace=workspace,
                run_description=self.run_description,
                command_line=self.command_line,
                current_directory=self.current_directory,
            )
        else:
            return Lineage(
                self.step_name,
                datetime.datetime.now(),
                self.parameters,
                inputs,
                self.code,
                workspace,
                self.command_line,
                self.current_directory,
            )


##########################################################################
#        Helper classes for defining program parameters
##########################################################################


class LineageParameter(ABC):
    def __init__(self, name: str, default: Any):
        self.name = name
        self.default = default

    @abstractmethod
    def get_value(self, parsed_args: Namespace):
        pass

    @abstractmethod
    def add_to_arg_parser(self, arg_parser: ArgumentParser):
        pass


class CmdLineParameter(LineageParameter):
    def __init__(self, name: str, default: Any, type: Type, help: str):
        super().__init__(name, default)
        self.type = type
        self.help = help

    def get_arg_name(self) -> str:
        return "--" + self.name.replace("_", "-")

    def add_to_arg_parser(self, arg_parser: ArgumentParser):
        arg_parser.add_argument(
            self.get_arg_name(), type=self.type, default=self.default, help=self.help
        )

    def get_value(self, parsed_args: Namespace):
        return getattr(parsed_args, self.name)


class BooleanParameter(CmdLineParameter):
    def __init__(self, name: str, default: bool, help: str):
        super().__init__(name, default, bool, help)
        if self.default:
            self.action = "store_false"
        else:
            self.action = "store_true"

    def get_arg_name(self) -> str:
        if self.default:
            return "--no-" + self.name.replace("_", "-")
        else:
            return "--" + self.name.replace("_", "-")

    def add_to_arg_parser(self, arg_parser: ArgumentParser):
        arg_parser.add_argument(
            self.get_arg_name(),
            default=self.default,
            action=self.action,
            help=self.help,
            dest=self.name,
        )


class ChoiceParameter(CmdLineParameter):
    def __init__(self, name: str, choices: Iterable[Any], default: Any, type: Type, help: str):
        super().__init__(name, default, type, help)
        self.choices = choices
        assert default in choices

    def add_to_arg_parser(self, arg_parser: ArgumentParser):
        arg_parser.add_argument(
            self.get_arg_name(),
            type=self.type,
            default=self.default,
            choices=self.choices,
            help=self.help,
        )


class ConstantParameter(LineageParameter):
    def get_value(self, parsed_args: Namespace):
        return self.default


def add_lineage_parameters_to_arg_parser(
    parser: ArgumentParser, params: Iterable[LineageParameter]
):
    for param in params:
        param.add_to_arg_parser(parser)


def get_lineage_parameter_values(params: Iterable[LineageParameter], parsed_args: Namespace):
    values = OrderedDict()  # type: Dict[str,Any]
    for param in params:
        values[param.name] = param.get_value(parsed_args)
    return values
