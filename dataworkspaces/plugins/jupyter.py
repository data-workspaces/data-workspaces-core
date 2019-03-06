"""
Integration with Jupyter notebooks
"""

import ipykernel
import requests
import json
#from requests.compat import urljoin
from urllib.parse import urljoin
import re
from os.path import join, basename, dirname
from notebook.notebookapp import list_running_servers
from typing import Dict, Any, List, Optional
import datetime

from dataworkspaces.utils.lineage_utils import ResourceRef
from dataworkspaces.lineage import Lineage
from dataworkspaces.utils.workspace_utils import get_workspace
from dataworkspaces.resources.resource import CurrentResources


def _get_notebook_name():
    """
    Return the full path of the jupyter notebook.
    See https://github.com/jupyter/notebook/issues/1000
    """
    kernel_id = re.search('kernel-(.*).json',
                          ipykernel.connect.get_connection_file()).group(1)
    servers = list_running_servers()
    for ss in servers:
        response = requests.get(urljoin(ss['url'], 'api/sessions'),
                                params={'token': ss.get('token', '')})
        for nn in json.loads(response.text):
            if nn['kernel']['id'] == kernel_id:
                relative_path = nn['notebook']['path']
                return join(ss['notebook_dir'], relative_path)

class NotebookLineage(Lineage):
    def __init__(self, parameters:Dict[str,Any],
                 inputs:List[str],
                 results_path:str,
                 other_outputs:Optional[List[str]]=None,
                 run_description:Optional[str]=None):
        notebook_path = _get_notebook_name()
        step_name = basename(notebook_path)
        if step_name.endswith('.ipynb'):
            step_name = step_name[0:-6]
        elif step_name.endswith('.py'):
            step_name = step_name[0:-3]
        workspace_dir = get_workspace(current_dir=dirname(notebook_path))
        super().__init__(step_name, datetime.datetime.now(),
                         parameters, inputs, workspace_dir)
        self.results_path = results_path
        self.run_description = run_description
        (self.results_rname, self.results_subpath) = \
            self.resources.map_local_path_to_resource(results_path)
        self.add_output_ref(ResourceRef(self.results_rname,
                                        self.results_subpath))
        if other_outputs is not None:
            for output in other_outputs:
                self.add_output_path(output)

    def write_results(self, results:Dict[str, Any]):
        data = {
            'step':self.step.step_name,
            'start_time':self.step.start_time.isoformat(),
            'execution_time_seconds':self.step.execution_time_seconds,
            'pameters': self.step.parameters,
            'run_description':self.run_description,
            'results': results
        }
        self.resources.by_name[self.results_rname]\
            .add_results_file_from_buffer(json.dumps(data, indent=2),
                                          "results.json")
        print("Wrote results to %s" % join(self.results_path, 'results.json'))
