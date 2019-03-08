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

from dataworkspaces.lineage import LineageBuilder


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

def get_step_name_for_notebook():
    notebook_path = _get_notebook_name()
    step_name = basename(notebook_path)
    if step_name.endswith('.ipynb'):
        step_name = step_name[0:-6]
    elif step_name.endswith('.py'):
        step_name = step_name[0:-3]
    return step_name


def is_notebook():
    try:
        shell = get_ipython().__class__.__name__
        if shell == 'ZMQInteractiveShell':
            return True   # Jupyter notebook or qtconsole
        elif shell == 'TerminalInteractiveShell':
            return False  # Terminal running IPython
        else:
            return False  # Other type (?)
    except NameError:
        return False      # Probably standard Python interpreter or a script


class NotebookLineageBuilder(LineageBuilder):
    """Notebooks are the final step in a pipeline
    (and potentially the only step). We customizer
    the standard lineage builder to get the step
    name from the notebook's name and to always have
    a results directory.
    """
    def __init__(self, results_dir:str,
                 run_description:Optional[str]=None):
        super().__init__()
        self.step_name = step_name
        self.results_dir = get_step_name_for_notebook()
        self.run_description = run_description


