"""
Integration with Jupyter notebooks. This module provides a
:class:`~LineageBuilder` subclass to simplify Lineage for Notebooks.
"""

import ipykernel
import requests
import json
#from requests.compat import urljoin
from urllib.parse import urljoin
import re
from os.path import join, basename, dirname, abspath, curdir
from notebook.notebookapp import list_running_servers
from typing import Dict, Any, List, Optional
import datetime
import sys

from dataworkspaces.lineage import LineageBuilder
from dataworkspaces.utils.lineage_utils import infer_script_path, ResourceRef


def _get_notebook_name() -> Optional[str]:
    """
    Return the full path of the jupyter notebook.
    See https://github.com/jupyter/notebook/issues/1000

    In some situations (e.g. running on the command line via nbconvert),
    the notebook name is not available. We return None in those cases.
    """
    # kernel_id = re.search('kernel-(.*).json',
    #                       ipykernel.connect.get_connection_file()).group(1)
    connection_file = ipykernel.connect.get_connection_file()
    mo=re.search('kernel-(.*).json', connection_file)
    if mo is not None:
        kernel_id = mo.group(1)
        servers = list_running_servers()
        for ss in servers:
            response = requests.get(urljoin(ss['url'], 'api/sessions'),
                                    params={'token': ss.get('token', '')})
            for nn in json.loads(response.text):
                if nn['kernel']['id'] == kernel_id:
                    relative_path = nn['notebook']['path']
                    return join(ss['notebook_dir'], relative_path)
    else:
        return None  # not running in the server


def get_step_name_for_notebook() -> Optional[str]:
    """
    Get the step name for a notebook by getting the path and then
    extracting the base name.
    In some situations (e.g. running on the command line via nbconvert),
    the notebook name is not available. We return None in those cases.
    """
    notebook_path = _get_notebook_name()
    if notebook_path is not None:
        step_name = basename(notebook_path)
        if step_name.endswith('.ipynb'):
            step_name = step_name[0:-6]
        elif step_name.endswith('.py'):
            step_name = step_name[0:-3]
        return step_name
    else:
        return None


def is_notebook() -> bool:
    """Return true if this code is running in a notebook.
    """
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


def get_notebook_directory():
    notebook_path = _get_notebook_name()
    if notebook_path is not None:
        return dirname(notebook_path)
    else:
        return curdir


class NotebookLineageBuilder(LineageBuilder):
    """Notebooks are the final step in a pipeline
    (and potentially the only step). We customize
    the standard lineage builder to get the step
    name from the notebook's name and to always have
    a results directory.

    If you are not running this notebook in a server
    context (e.g. via nbconvert), the step name won't be
    available. In that case, you can explicitly pass in the
    step name to the constructor.
    """
    def __init__(self, results_dir:str,
                 step_name:Optional[str]=None,
                 run_description:Optional[str]=None):
        super().__init__()
        if step_name is not None:
            self.step_name = step_name
        else:
            notebook_step_name = get_step_name_for_notebook()
            self.step_name = notebook_step_name if notebook_step_name is not None \
                             else 'UnknownNotebook'
        notebook_path = _get_notebook_name()
        if notebook_path is not None:
            self.code.append(notebook_path)
        else:
            # if we are not running in a server content,
            # use the current directory as the code resource path
            self.code.append(abspath(curdir))
        self.results_dir = results_dir
        self.run_description = run_description


