"""
API for tracking data lineage
"""
import sys
from abc import ABC, abstractmethod
import contextlib
from collections import OrderedDict
import datetime
from typing import List, Union, Any, Type, Iterable, Dict, Optional
import os
from os.path import exists, curdir, join
from argparse import ArgumentParser, Namespace
import json
from copy import copy

from dataworkspaces.utils.workspace_utils import get_workspace
from dataworkspaces.utils.lineage_utils import \
    ResourceRef, StepLineage, LineageStoreCurrent,\
    get_current_lineage_dir, infer_step_name
from dataworkspaces.resources.resource import CurrentResources



##########################################################################
#                   Main lineage API
##########################################################################

class Lineage(contextlib.AbstractContextManager):
    def __init__(self, step_name:str, start_time:datetime.datetime,
                 parameters:Dict[str,Any],
                 inputs:List[Union[str, ResourceRef]],
                 workspace_dir:str,
                 command_line:Optional[List[str]]=None,
                 current_directory:Optional[str]=None):
        self.lineage_dir = get_current_lineage_dir(workspace_dir)
        if not exists(self.lineage_dir):
            os.makedirs(self.lineage_dir)
        self.store = LineageStoreCurrent.load(self.lineage_dir)
        self.resources = CurrentResources.read_current_resources(workspace_dir,
                                                                 batch=True,
                                                                 verbose=False)
        input_resource_refs=[] # type: List[ResourceRef]
        for r_or_p in inputs:
            if isinstance(r_or_p, ResourceRef):
                self.resources.validate_resource_name(r_or_p.name, r_or_p.subpath)
                input_resource_refs.append(r_or_p)
            else:
                (name, subpath) = self.resources.map_local_path_to_resource(r_or_p)
                input_resource_refs.append(ResourceRef(name, subpath))
        self.step = StepLineage.make_step_lineage(step_name, start_time,
                                                  parameters, input_resource_refs,
                                                  self.store,
                                                  command_line=command_line)
        self.in_progress = True

    def add_output_path(self, path:str):
       (name, subpath) = self.resources.map_local_path_to_resource(path)
       self.step.add_output(self.store, ResourceRef(name, subpath))

    def add_output_ref(self, ref:ResourceRef):
        self.step.add_output(self.store, ref)

    def abort(self):
        """The step has failed, so we mark its outputs in an unknown state.
        If you create the lineage via a "with" statement, then this will be
        called for you automatically.
        """
        if not self.in_progress:
            print("WARNING: Lineage.abort() called after complete() or abort() call for %s" %
                  self.step.step_name, file=sys.stderr)
        else:
            self.in_progress = False
        self.store.invalidate_step_outputs(self.step.output_resources)
        self.store.save(self.lineage_dir)

    def complete(self):
        """The step has completed. Save the outputs.
        If you create the lineage via a "with" statement, then this will be
        called for you automatically.
        """
        if not self.in_progress:
            print("WARNING: Lineage.complete() called after complete() or abort() call for %s" %
                  self.step.step_name, file=sys.stderr)
        else:
            self.in_progress = False
        self.step.execution_time_seconds = (datetime.datetime.now() -
                                            self.step.start_time).total_seconds()
        self.store.add_step(self.step)
        self.store.save(self.lineage_dir)

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is None:
            self.complete()
        else:
            self.abort()
        return False # don't suppress any exception

class ResultsLineage(Lineage):
    """Lineage for a results step.
    """
    def __init__(self, step_name:str, start_time:datetime.datetime,
                 parameters:Dict[str,Any],
                 inputs:List[Union[str, ResourceRef]],
                 results_dir:str,
                 workspace_dir:str,
                 run_description:Optional[str]=None,
                 command_line:Optional[List[str]]=None,
                 current_directory:Optional[str]=None):
        super().__init__(step_name, start_time, parameters,
                         inputs, workspace_dir, command_line, current_directory)
        self.results_dir = results_dir
        (self.results_rname, self.results_subpath) = \
            self.resources.map_local_path_to_resource(results_dir)
        self.add_output_ref(ResourceRef(self.results_rname,
                                        self.results_subpath))
        self.run_description = run_description

    def write_results(self, metrics:Dict[str, Any]):
        data = {
            'step':self.step.step_name,
            'start_time':self.step.start_time.isoformat(),
            'execution_time_seconds':self.step.execution_time_seconds,
            'parameters': self.step.parameters,
            'run_description':self.run_description,
            'metrics': metrics
        }
        if self.results_subpath:
            results_relpath = join(self.results_subpath, "results.json")
        else:
            results_relpath = "results.json"
        self.resources.by_name[self.results_rname]\
            .add_results_file_from_buffer(json.dumps(data, indent=2),
                                          results_relpath)
        print("Wrote results to %s:%s" % (self.results_rname, results_relpath))


def make_lineage(parameters:Dict[str,Any], inputs:List[Union[str, ResourceRef]],
                 step_name:Optional[str]=None,
                 workspace_dir:Optional[str]=None)\
                 -> Lineage:
    print("WARNING: make_lineage is depricated, use the LineageBuilder instead!",
          file=sys.stderr)
    workspace_dir = get_workspace(workspace_dir)
    assert workspace_dir is not None # make the type checker happy
    if step_name is None:
        step_name = infer_step_name()
    return Lineage(step_name, datetime.datetime.now(), parameters, inputs,
                   workspace_dir=workspace_dir,
                   command_line=[sys.executable]+sys.argv)

class LineageBuilder:
    def __init__(self):
        self.step_name = None         # type: Optional[str]
        self.command_line = None      # type: Optional[List[str]]
        self.current_directory = None # type: Optional[str]
        self.parameters = None        # type: Optional[Dict[str, Any]]
        self.inputs = None            # type: Optional[List[Union[str, ResourceRef]]]
        self.no_inputs = False        # type: Boolean
        self.workspace_dir = None     # type: Optional[str]
        self.results_dir = None       # type: Optional[str]
        self.run_description = None   # type: Optional[str]

    def as_script_step(self) -> 'LineageBuilder':
        assert self.step_name is None, "attempting to set step name twice!"
        self.step_name = infer_step_name()
        self.command_line = [sys.executable]+sys.argv
        self.current_directory = curdir
        return self

    def with_step_name(self, step_name:str) -> 'LineageBuilder':
        assert self.step_name is None, "attempting to set step name twice!"
        self.step_name = step_name
        return self

    def with_parameters(self, parameters:Dict[str, Any]) -> 'LineageBuilder':
        assert self.parameters is None, "attemping to specify parameters twice"
        self.parameters = parameters
        return self

    def with_input_path(self, path:str) -> 'LineageBuilder':
        assert self.no_inputs is False, "Cannot specify both inputs and no inputs"
        if self.inputs is None:
            self.inputs = [path]
        else:
            self.inputs.append(path)
        return self

    def with_input_paths(self, paths:List[str]) -> 'LineageBuilder':
        assert self.no_inputs is False, "Cannot specify both inputs and no inputs"
        if self.inputs is None:
            self.inputs = copy(paths)
        else:
            self.inputs.extend(path)
        return self

    def with_input_ref(self, ref:ResourceRef) -> 'LineageBuilder':
        assert self.no_inputs is False, "Cannot specify both inputs and no inputs"
        if self.inputs is None:
            self.inputs = [ref]
        else:
            self.inputs.append(ref)
        return self

    def with_no_inputs(self) -> 'LineageBuilder':
        assert self.inputs is None, "Cannot specify inputs and with_no_inputs()"
        self.no_inputs = True
        return self

    def with_workspace_directory(self, workspace_dir:str) -> 'LineageBuilder':
        self.workspace_dir = get_workspace(workspace_dir) # does validation
        return self

    def as_results_step(self, results_dir:str, run_description:Optional[str]=None)\
        -> 'LineageBuilder':
        assert self.results_dir is None, \
            "attempting to specify results directory twice"
        self.results_dir = results_dir
        self.run_desciption = run_description
        return self

    def eval(self) -> Lineage:
        assert self.step_name is not None, "Need to specify step name"
        assert self.parameters is not None, "Need to specify parameters"
        assert self.no_inputs or (self.inputs is not None),\
            'Need to specify either inputs or no inputs'
        inputs = self.inputs if self.inputs is not None else [] # type: List[Union[str, Any]]
        if self.workspace_dir is None:
            self.workspace_dir = get_workspace()
        if self.results_dir is not None:
            return ResultsLineage(self.step_name, datetime.datetime.now(),
                                  self.parameters, inputs,
                                  self.results_dir,
                                  self.workspace_dir, self.run_description,
                                  self.command_line, self.current_directory)
        else:
            return Lineage(self.step_name, datetime.datetime.now(),
                           self.parameters, inputs,
                           self.workspace_dir,
                           self.command_line, self.current_directory)


##########################################################################
#        Helper classes for defining program parameters
##########################################################################

class LineageParameter(ABC):
    def __init__(self, name:str, default:Any):
        self.name = name
        self.default = default

    @abstractmethod
    def get_value(self, parsed_args:Namespace):
        pass

    @abstractmethod
    def add_to_arg_parser(self, arg_parser:ArgumentParser):
        pass


class CmdLineParameter(LineageParameter):
    def __init__(self, name:str, default:Any, type:Type, help:str):
        super().__init__(name, default)
        self.type = type
        self.help = help

    def get_arg_name(self) -> str:
        return '--' + self.name.replace('_', '-')

    def add_to_arg_parser(self, arg_parser:ArgumentParser):
        arg_parser.add_argument(self.get_arg_name(), type=self.type,
                                default=self.default,
                                help=self.help)

    def get_value(self, parsed_args:Namespace):
        return getattr(parsed_args, self.name)


class BooleanParameter(CmdLineParameter):
    def __init__(self, name:str, default:bool, help:str):
        super().__init__(name, default, bool, help)
        if self.default:
            self.action='store_false'
        else:
            self.action='store_true'

    def get_arg_name(self) -> str:
        if self.default:
            return '--no-' + self.name.replace('_', '-')
        else:
            return '--' + self.name.replace('_', '-')

    def add_to_arg_parser(self, arg_parser:ArgumentParser):
        arg_parser.add_argument(self.get_arg_name(), default=self.default,
                                action=self.action,
                                help=self.help, dest=self.name)


class ChoiceParameter(CmdLineParameter):
    def __init__(self, name:str, choices:Iterable[Any], default:Any, type:Type,
                 help:str):
        super().__init__(name, default, type, help)
        self.choices = choices
        assert default in choices

    def add_to_arg_parser(self, arg_parser:ArgumentParser):
        arg_parser.add_argument(self.get_arg_name(), type=self.type, default=self.default,
                                choices=self.choices,
                                help=self.help)


class ConstantParameter(LineageParameter):
    def get_value(self, parsed_args:Namespace):
        return self.default


def add_lineage_parameters_to_arg_parser(parser:ArgumentParser,
                                         params:Iterable[LineageParameter]):
    for param in params:
        param.add_to_arg_parser(parser)


def get_lineage_parameter_values(params:Iterable[LineageParameter],
                                 parsed_args:Namespace):
    values = OrderedDict() # type: Dict[str,Any]
    for param in params:
        values[param.name] = param.get_value(parsed_args)
    return values
