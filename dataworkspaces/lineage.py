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
from os.path import exists
from argparse import ArgumentParser, Namespace

from dataworkspaces.utils.workspace_utils import get_workspace
from dataworkspaces.utils.lineage_utils import \
    ResourceRef, StepLineage, LineageStoreCurrent,\
    get_current_lineage_dir, infer_step_name
from dataworkspaces.resources.resource import CurrentResources

##########################################################################
#        Classes for defining program parameters
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


##########################################################################
#                   Main lineage API
##########################################################################

class Lineage(contextlib.AbstractContextManager):
    def __init__(self, step_name:str, start_time:datetime.datetime,
                 parameters:Dict[str,Any],
                 inputs:List[Union[str, ResourceRef]],
                 workspace_dir:str,
                 command_line:Optional[List[str]]=None):
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

def make_lineage(parameters:Dict[str,Any], inputs:List[Union[str, ResourceRef]],
                 step_name:Optional[str]=None,
                 workspace_dir:Optional[str]=None)\
                 -> Lineage:
    workspace_dir = get_workspace(workspace_dir)
    assert workspace_dir is not None # make the type checker happy
    if step_name is None:
        step_name = infer_step_name()
    return Lineage(step_name, datetime.datetime.now(), parameters, inputs,
                   workspace_dir=workspace_dir,
                   command_line=[sys.executable]+sys.argv)
