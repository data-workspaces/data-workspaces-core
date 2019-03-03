"""
API for tracking data lineage
"""

from abc import ABC, abstractmethod
from collections import OrderedDict
import datetime
from typing import List, Union
import os
from os.path import exists

from dataworkspaces.utils.workspace_utils import get_workspace
from dataworkspaces.utils.lineage_utils import \
    ResourceRef, StepLineage, LineageStoreCurrent,\
    get_current_lineage_dir
from dataworkspaces.resources.resource import CurrentResources


class Lineage:
    def __init__(self, step_name:str, start_time:datetime.datetime,
                 parameters:OrderedDict, workspace_dir:str,
                 inputs=List[Union[str, ResourceRef]]):
        lineage_dir = get_current_lineage_dir(workspace_dir)
        if not exists(lineage_dir):
            os.makedirs(lineage_dir)
        self.store = LineageStoreCurrent.load(lineage_dir)
        self.resources = CurrentResources.read_current_resources(workspace_dir,
                                                                 batch=True,
                                                                 verbose=False)
        input_resource_refs=[]
        for r_or_p in inputs:
            if isinstance(r_or_p, ResourceRef):
                self.resources.validate_resource_ref(r_or_p)
                input_resource_refs.append(r_or_p)
            else:
                (name, subpath) = self.resources.map_local_path_to_resource(r_or_p)
                input_resource_refs.append(ResourceRef(name, subpath))
        self.step = StepLineage.make_step_lineage(step_name, start_time,
                                                  parameters, input_resource_refs)

    def add_output_path(self, path:str):
       (name, subpath) = self.resources.map_local_path_to_resource(path)
       self.step.add_output(self.store, ResourceRef(name, subpath))

    def add_output_ref(self, ref:ResourceRef):
        self.step.add_output(self.store, ref)
