"""
Integration with Jupyter notebooks. This module provides a
:class:`~LineageBuilder` subclass to simplify Lineage for Notebooks.
"""

import ipykernel
from IPython.core.getipython import get_ipython
from IPython.core.magic import (Magics, magics_class, line_magic)
import requests
import json
#from requests.compat import urljoin
from urllib.parse import urljoin
import re
from os.path import join, basename, dirname, abspath, expanduser, curdir, isabs
from notebook.notebookapp import list_running_servers
from typing import Optional
import shlex
import argparse

from collections import namedtuple


from dataworkspaces.lineage import LineageBuilder
from dataworkspaces.workspace import _find_containing_workspace
from dataworkspaces.api import take_snapshot, get_snapshot_history
from dataworkspaces.errors import ConfigurationError


def _get_notebook_name(verbose=False) -> Optional[str]:
    """
    Return the full path of the jupyter notebook.
    See https://github.com/jupyter/notebook/issues/1000

    In some situations (e.g. running on the command line via nbconvert),
    the notebook name is not available. We return None in those cases.
    """
    # kernel_id = re.search('kernel-(.*).json',
    #                       ipykernel.connect.get_connection_file()).group(1)
    try:
        ipy = get_ipython()
        info = ipy.ev("DWS_JUPYTER_INFO")
        return info.notebook_path
    except Exception as e:
        if verbose:
            print("DWS Jupyter extension was not loaded: %s" % e)
    try:
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
            print("Did not find a matching notebook server for %s" % connection_file)
            return None
    except Exception as e:
        if verbose:
            print("Unable to use notebook API to access session info: %s" % e)
    # all our atempts failed
    return None


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
        # if running in ipython, get_ipython() will be in the global contect
        shell = get_ipython().__class__.__name__ # type: ignore
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
            if notebook_step_name is None:
                raise ConfigurationError("Unable to infer the name of this notebook. "+
                                         "Please either use the DWS notebook magic or pass the name in explicitly to the lineage builder.")
            self.step_name = notebook_step_name
        notebook_path = _get_notebook_name()
        if notebook_path is not None:
            self.code.append(notebook_path)
        else:
            # if we are not running in a server content,
            # use the current directory as the code resource path
            self.code.append(abspath(curdir))
        self.results_dir = results_dir
        self.run_description = run_description


############################################################################
#                 Code for IPython magic extension                         #
############################################################################

DwsJupyterInfo = namedtuple('DwsJupyterInfo',
                            ['notebook_name', 'notebook_path', 'workspace_dir', 'error'])



init_jscode=r"""
%%javascript
var dws_initialization_msg = "Ran DWS initialization. The following magic commands have been added to your notebook:\n- `%dws_info` - print information about your dws environment\n- `%dws_history` - print a history of snapshots in this workspace\n- `%dws_snapshot` - save and create a new snapshot\n\nRun any command with the `--help` option to see a list\nof options for that command.\n\nThe variable `DWS_JUPYTER_NOTEBOOK` has been added to\nyour variables, for use in future DWS calls.";
if (typeof Jupyter == "undefined") {
    alert("Unable to initialize DWS magic. This version only works with Jupyter Notebooks, not nbconvert or JupyterLab.");
    throw "Unable to initialize DWS magic. This version only works with Jupyter Notebooks, not nbconvert or JupyterLab.";
}
else if (Jupyter.notebook.hasOwnProperty('kernel') && Jupyter.notebook.kernel!=null) {
    var DWSComm = Jupyter.notebook.kernel.comm_manager.new_comm('dws_comm_target', {})
    DWSComm.on_msg(function(msg) {
        console.log("DWS Got msg status: " + msg.content.data.status);
        console.log("DWS msg type: " + msg.content.data.msg_type);
        if (msg.content.data.status != 'ok') {
            if (msg.content.data.hasOwnProperty('cell')) {
                var cell = Jupyter.notebook.get_cell(msg.content.data.cell-1);
                cell.output_area.append_output({'data':{'text/plain':msg.content.data.status}, 'metadata':{}, 'output_type':'display_data' });
            }
            alert(msg.content.data.status);
            return;
        }
        if (msg.content.data.msg_type == "snapshot-result") {
            var cell = Jupyter.notebook.get_cell(msg.content.data.cell-1);
            cell.output_area.append_output({'data':{'text/plain':msg.content.data.message}, 'metadata':{}, 'output_type':'display_data' });
            alert(msg.content.data.message);
        }
        else if (msg.content.data.msg_type == "init-ack") {
            var cell = Jupyter.notebook.get_cell(msg.content.data.cell-1);
            cell.output_area.append_output({'data':{'text/markdown':dws_initialization_msg}, 'metadata':{}, 'output_type':'display_data' });
            //alert(dws_initialization_msg);
        }
    });
    // Send data
    var cellno = Jupyter.notebook.find_cell_index(Jupyter.notebook.get_selected_cell());
    DWSComm.send({'msg_type':'init',
                  'notebook_name': Jupyter.notebook.notebook_name,
                  'notebook_path': Jupyter.notebook.notebook_path,
                  'cell':cellno});
    window.DWSComm = DWSComm;
} else {
    // this happens when evaluating the javascript upon loading the notebook
    console.log("kernal was null");
}
"""

snapshot_jscode="""
%%javascript
Jupyter.notebook.save_notebook();
"""

snapshot_jscode2="""
%%javascript
if (window.hasOwnProperty('DWSComm')) {
    window.DWSComm.send({'msg_type':'snapshot',
                         'cell':Jupyter.notebook.find_cell_index(Jupyter.notebook.get_selected_cell())});
    console.log("sending snapshot");
}
"""

initialization_msg ='''
Ran DWS initialization. The following magic commands have been added to your notebook:

* %dws_info     - print information about your dws environment
* %dws_history  - print a history of snapshots in this workspace
* %dws_snapshot - save the notebook and create a new snapshot

Run any command with the --help option to see a list of options for that
command.

The variable DWS_JUPYTER_NOTEBOOK will be added to your variables, for
use in future DWS calls.
'''

class DwsMagicError(ConfigurationError):
    pass

class DwsMagicArgParseExit(Exception):
    """Throw this in our overriding the exit method"""
    pass

class DwsMagicParseArgs(argparse.ArgumentParser):
    """Specialized version of the argument parser that can
    work in the context of ipython magic commands. Should
    never call sys.exit() and needs its own line parsing.
    """
    def parse_magic_line(self, line):
        return self.parse_args(shlex.split(line))
    def error(self, msg):
        raise DwsMagicError(msg)
    def exit(self, status=0, message=None):
        assert status==0, "Expecting a status of 0"
        raise DwsMagicArgParseExit()


@magics_class
class DwsMagics(Magics):
    def __init__(self, shell):
        super().__init__(shell)
        self._snapshot_args = None
        def target_func(comm, open_msg):
            self.comm = comm
            @comm.on_msg
            def _recv(msg):
                ipy = get_ipython()
                data = msg['content']['data']
                msg_type = data['msg_type']
                if msg_type=='init':
                    npath = data['notebook_path']
                    if not isabs(npath):
                        npath = join(abspath(expanduser(curdir)), npath)
                    notebook_dir=dirname(npath)
                    workspace_dir = _find_containing_workspace(notebook_dir)
                    error = None
                    if workspace_dir is None:
                        error = "Unable to find a containing workspace for note book at %s" % npath
                    DWS_JUPYTER_INFO=DwsJupyterInfo(data['notebook_name'],
                                      npath,
                                      workspace_dir,
                                      error)
                    ipy.push({'DWS_JUPYTER_INFO': DWS_JUPYTER_INFO})
                    if error:
                        comm.send({'status':error})
                        raise Exception(error)
                    else:
                        comm.send({'status':'ok', 'msg_type':'init-ack', 'cell':data['cell']})
                        self.dws_jupyter_info = DWS_JUPYTER_INFO
                elif msg_type=='snapshot':
                    cell = data['cell']
                    try:
                        r = take_snapshot(self.dws_jupyter_info.workspace_dir,
                                          tag=self._snapshot_args.tag,
                                          message=self._snapshot_args.message)
                        self._snapshot_args = None
                        comm.send({'msg_type':'snapshot-result',
                                   'status':'ok',
                                   'message':'Successfully completed snapshot. Hash is %s'%r[0:8],
                                   'cell':cell})
                    except Exception as e:
                        comm.send({'msg_type':'snapshot-result',
                                   'status':"Snapshot failed with error '%s'"% e,
                                   'cell':cell})
                        raise
                else:
                    raise Exception("Uknown message type %s" % msg_type)
        self.shell.kernel.comm_manager.register_target('dws_comm_target', target_func)
        self.shell.run_cell(init_jscode, store_history=False, silent=True)

    async def _call_snapshot(self):
        await self.shell.run_cell_async(snapshot_jscode)
        await self.shell.run_cell_async(snapshot_jscode2)

    @line_magic
    def dws_info(self, line):
        print("Notebook name:       %s" % self.dws_jupyter_info.notebook_name)
        print("Notebook path:       %s"  % self.dws_jupyter_info.notebook_path)
        print("Workspace directory: %s" % self.dws_jupyter_info.workspace_dir)
        if self.dws_jupyter_info.error is not None:
            print("Error message:       %s" % self.dws_jupyter_info.error)

    @line_magic
    def dws_snapshot(self, line):
        parser = DwsMagicParseArgs("dws_snapshot")
        parser.add_argument('-m', '--message', type=str, default=None,
                            help="Message describing the snapshot")
        parser.add_argument('-t', '--tag', type=str, default=None,
                            help="Tag for the snapshot. Note that a given tag can "+
                                 "only be used once (without deleting the old one).")
        try:
            args = parser.parse_magic_line(line)
        except DwsMagicArgParseExit:
            return # user asked for help
        self._snapshot_args = args
        msg = "Initiating snapshot"
        if args.tag:
            msg += " with tag '%s'" % args.tag
        if args.message:
            msg += " with message '%s'" % args.message
        print(msg + '...')
        import tornado.ioloop
        tornado.ioloop.IOLoop.current().spawn_callback(self._call_snapshot)

    @line_magic
    def dws_history(self, line):
        import pandas as pd # TODO: support case where pandas wasn't installed
        parser = DwsMagicParseArgs("dws_history")
        parser.add_argument('--max-count', type=int, default=None,
                            help="Maximum number of snapshots to show")
        parser.add_argument('--tail', default=False, action='store_true',
                            help="Just show the last 10 entries in reverse order")
        try:
            args = parser.parse_magic_line(line)
        except DwsMagicArgParseExit:
            return # user asked for help
        if args.max_count and args.tail:
            max_count = args.max_count
        elif args.tail:
            max_count = 10
        else:
            max_count = None
        history = get_snapshot_history(max_count=max_count,
                                       reverse=args.tail)
        entries = []
        index = []
        for s in history:
            d = {'timestamp':s.timestamp,
                 'hash':s.hashval[0:8],
                 'tags':', '.join([tag for tag in s.tags]),
                 'message':s.message}
            if s.metrics is not None:
                for (m, v) in s.metrics.items():
                    d[m] = v
            entries.append(d)
            index.append(s.snapshot_number)
        history_df = pd.DataFrame(entries, index=index)
        return history_df


def load_ipython_extension(ipython):
    ipython.register_magics(DwsMagics)

