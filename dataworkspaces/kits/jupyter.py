"""
Integration with Jupyter notebooks. This module provides a
:class:`~LineageBuilder` subclass to simplify Lineage for Notebooks.

It also provides a collection of IPython *magics* (macros) for working
in Jupyter notebooks.
"""
import os
import sys
import ipykernel
from IPython.core.getipython import get_ipython
from IPython.core.magic import (Magics, magics_class, line_magic)
from IPython.core.display import display
from IPython.display import IFrame, HTML, Markdown

import requests
import json
from urllib.parse import urljoin
import re
from os.path import join, basename, dirname, abspath, expanduser, curdir, exists
from notebook.notebookapp import list_running_servers
from typing import Optional, List, Any, Dict, Tuple, Callable, Sequence
assert Dict # keep pyflakes happy
import shlex
import argparse

from collections import namedtuple


from dataworkspaces.lineage import LineageBuilder
from dataworkspaces.workspace import _find_containing_workspace
from dataworkspaces.api import take_snapshot, get_snapshot_history,\
                               make_lineage_table, make_lineage_graph,\
                               get_results, get_resource_info
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


def _remove_notebook_extn(notebook_name):
    if notebook_name.endswith('.ipynb'):
        return notebook_name[0:-6]
    elif notebook_name.endswith('.py'):
        return notebook_name[0:-3]
    else:
        return notebook_name

def get_step_name_for_notebook() -> Optional[str]:
    """
    Get the step name for a notebook by getting the path and then
    extracting the base name.
    In some situations (e.g. running on the command line via nbconvert),
    the notebook name is not available. We return None in those cases.
    """
    notebook_path = _get_notebook_name()
    if notebook_path is not None:
        return _remove_notebook_extn(basename(notebook_path))
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
                            ['notebook_name', 'notebook_path', 'workspace_dir', 'notebook_server_dir',
                             'error'])



init_jscode=r"""%%javascript
var dws_initialization_msg = "Ran DWS initialization. The following magic commands have been added to your notebook:\n- `%dws_info` - print information about your dws environment\n- `%dws_history` - print a history of snapshots in this workspace\n- `%dws_snapshot` - save and create a new snapshot\n- `%dws_lineage_table` - show a table of lineage for the workspace resources\n- `%dws_lineage_graph` - show a graph of lineage for a resource\n- `%dws_results` - show results from a run (results.json file)\n\nRun any command with the `--help` option to see a list\nof options for that command.\n\nThe variable `DWS_JUPYTER_NOTEBOOK` has been added to\nyour variables, for use in future DWS calls.\n\nIf you want to disable the DWS magic commands (e.g. when running in a batch context), set the variable `DWS_MAGIC_DISABLE` to `True` ahead of the `%load_ext` call.";
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

snapshot_jscode="""%%javascript
Jupyter.notebook.save_notebook();
"""

snapshot_jscode2="""%%javascript
if (window.hasOwnProperty('DWSComm')) {
    window.DWSComm.send({'msg_type':'snapshot',
                         'cell':Jupyter.notebook.find_cell_index(Jupyter.notebook.get_selected_cell())});
    console.log("sending snapshot");
}
"""


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

# Colormaps for heatmaps
# These were generated using seaborn:
#     def tobyte(c):
#         return int(round(255*c))
#     MINIMIZE_COLORMAP = ['rgb(%s,%s,%s)'%(tobyte(c[0]),tobyte(c[1]),tobyte(c[2]))
#                          for c in seaborn.diverging_palette(150, 10, s=50, l=50, n=7)]
#     MAXIMIZE_COLORMAP = ['rgb(%s,%s,%s)'%(tobyte(c[0]),tobyte(c[1]),tobyte(c[2]))
#                          for c in seaborn.diverging_palette(10, 150, s=50, l=50, n=7)]
# The two maps are just the reverse of each other with maximize having greener colors toward
# the high indexes and redder colors toward the low indexes, and minimize being the opposite.
# By pre-generating the colormaps, we avoid a dependency on seaborn.
MINIMIZE_COLORMAP=['rgb(84,128,107)', 'rgb(138,168,153)', 'rgb(193,210,201)', 'rgb(242,242,242)', 'rgb(232,190,192)', 'rgb(212,136,140)', 'rgb(193,84,89)']
MAXIMIZE_COLORMAP=['rgb(193,84,89)', 'rgb(212,136,140)', 'rgb(232,190,192)', 'rgb(242,242,242)', 'rgb(193,210,201)', 'rgb(138,168,153)', 'rgb(84,128,107)']

def _fmt_scalar(s):
    """Helper function to round metrics"""
    if not isinstance(s, float) and (not hasattr(s, 'dtype') or s.dtype!='f'):
        return s # non-floats left alone
    elif s >=1.0:
        return round(s, 1)
    elif s>=0.01:
        return round(s, 3)
    else:
        return s

_BINS_TO_LABELS={
  1: [3],
  2: [2,4],
  3: [2,3,4],
  4: [1,2,4,5],
  5: [1,2,3,4,5],
  6: [0,1,2,4,5,6],
  7: [0,1,2,3,4,5,6]
} # type: Dict[int,Sequence[int]]
def _metric_col_to_colormap(col):
    """Given a metric column, return a series representing
    the heatmap indexes (0 through 6).
    Returns a series with the same number of elements as the column.
    """
    import pandas as pd
    import numpy as np
    nunique = len(col.dropna().unique())
    num_bins = min(nunique, 7)
    if num_bins<2:
        return col.apply(lambda v: -1 if pd.isna(v) else 3)
    elif num_bins==2:
        minval=col.min()
        return col.apply(lambda v: -1 if pd.isna(v)
                         else (2 if v==minval else 4))
    # qcut() may collapse bins, so we need to figure out how many bins it will
    # actually give us.
    num_actual_bins = len(pd.qcut(col, num_bins, duplicates='drop').dtype.categories)
    labels=_BINS_TO_LABELS[num_actual_bins]
    try:
        return pd.qcut(col, num_bins, labels=labels, duplicates='drop').astype(np.float32).fillna(-1.0).astype(np.int32)
    except Exception as e:
        print(e, file=sys.stderr)
        print("problem binning columns, unique=%s, num_actual_bins=%s, labels=%s"%
              (nunique, num_actual_bins, labels), file=sys.stderr)
        print("col: %s" % repr(col), file=sys.stderr)
        raise


@magics_class
class DwsMagics(Magics):
    def __init__(self, shell):
        super().__init__(shell)
        try:
            self.disabled = get_ipython().ev('DWS_MAGIC_DISABLE')
        except NameError:
            self.disabled = False
        if self.disabled:
            print("Loaded Data Workspaces magic commands in disabled state.", file=sys.stderr)
            return
        self._snapshot_args = None # type: Optional[argparse.Namespace] 
        def target_func(comm, open_msg):
            self.comm = comm
            @comm.on_msg
            def _recv(msg):
                ipy = get_ipython()
                data = msg['content']['data']
                msg_type = data['msg_type']
                if msg_type=='init':
                    # It looks like the notebook is always running with the cwd set to the notebook
                    # However, the notebook path from the browser is relative to where the
                    # notebook server was started
                    #npath = data['notebook_path']
                    #if not isabs(npath):
                    #    npath = join(abspath(expanduser(curdir)), npath)
                    abscwd = abspath(expanduser(os.getcwd()))
                    npath = join(abscwd, data['notebook_name'])
                    assert exists(npath), "Wrong calculation for absolute notebook path, got %s" % npath
                    assert npath.endswith(data['notebook_path']), \
                        "Unexpacted notebook path from client, got %s, but absolute is %s" %\
                        (data['notebook_path'], npath)
                    notebook_server_dir = npath[0:-(len(data['notebook_path'])+1)]
                    notebook_dir=dirname(npath)
                    workspace_dir = _find_containing_workspace(notebook_dir)
                    error = None
                    if workspace_dir is None:
                        error = "Unable to find a containing workspace for note book at %s" % npath
                    DWS_JUPYTER_INFO=DwsJupyterInfo(data['notebook_name'],
                                                    npath,
                                                    workspace_dir,
                                                    notebook_server_dir,
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
                        assert self._snapshot_args is not None
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
                    raise Exception("Unknown message type %s" % msg_type)
        self.shell.kernel.comm_manager.register_target('dws_comm_target', target_func)
        self.shell.run_cell(init_jscode, store_history=False, silent=True)

    async def _call_snapshot(self):
        await self.shell.run_cell_async(snapshot_jscode)
        await self.shell.run_cell_async(snapshot_jscode2)

    @line_magic
    def dws_info(self, line):
        import pandas as pd # TODO: support case where pandas wasn't installed
        parser = DwsMagicParseArgs("dws_info",
                                   description="Print some information about this workspace")
        try:
            parser.parse_magic_line(line)
        except DwsMagicArgParseExit:
            return # user asked for help
        if self.disabled:
            display(Markdown("DWS magic commands are disabled. To enable, set `DWS_MAGIC_DISABLE` to `False` and restart kernel."))
            return
        print("Notebook name:       %s" % self.dws_jupyter_info.notebook_name)
        print("Notebook path:       %s"  % self.dws_jupyter_info.notebook_path)
        print("Workspace directory: %s" % self.dws_jupyter_info.workspace_dir)
        print("Notebook server dir: %s" % self.dws_jupyter_info.notebook_server_dir)
        if self.dws_jupyter_info.error is not None:
            print("Error message:       %s" % self.dws_jupyter_info.error)
            return

        resources = get_resource_info(self.dws_jupyter_info.workspace_dir)
        df = pd.DataFrame({
            'Resource':[r.name for r in resources],
            'Role':[r.role for r in resources],
            'Type':[r.resource_type for r in resources],
            'Local Path':[r.local_path for r in resources]
        })
        with pd.option_context('display.max_colwidth', 80):
            display(df)

    @line_magic
    def dws_snapshot(self, line):
        parser = DwsMagicParseArgs("dws_snapshot",
                                   description="Save the notebook and create a new snapshot")
        parser.add_argument('-m', '--message', type=str, default=None,
                            help="Message describing the snapshot")
        parser.add_argument('-t', '--tag', type=str, default=None,
                            help="Tag for the snapshot. Note that a given tag can "+
                                 "only be used once (without deleting the old one).")
        try:
            args = parser.parse_magic_line(line)
        except DwsMagicArgParseExit:
            return # user asked for help
        if self.disabled:
            display(Markdown("DWS magic commands are disabled. To enable, set `DWS_MAGIC_DISABLE` to `False` and restart kernel."))
            return
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
        parser = DwsMagicParseArgs("dws_history",
                                   description="Print a history of snapshots in this workspace")
        parser.add_argument('--max-count', type=int, default=None,
                            help="Maximum number of snapshots to show")
        parser.add_argument('--tail', default=False, action='store_true',
                            help="Just show the last 10 entries in reverse order")
        parser.add_argument('--baseline', default=None, type=str,
                            help="Snapshot tag or hash to use as a basis for metrics comparison. "+
                                 "Will color the fonts of values green or red, "+
                                 "depending on whether they are better (worse) than "+
                                 "the baseline.")
        parser.add_argument('--heatmap', default=False, action='store_true',
                            help="Show a heatmap for metrics columns")
        parser.add_argument('--maximize-metrics', default=None, type=str,
                            help="Metrics where larger values are better (e.g. accuracy)")
        parser.add_argument('--minimize-metrics', default=None, type=str,
                            help="Metrics where smaller values are better (e.g. loss)")
        # TODO: future feature
        # parser.add_argument('--round-metrics', type=int, default=None,
        #                     help="If specified, round metrics to this many decimal places")
        try:
            args = parser.parse_magic_line(line)
        except DwsMagicArgParseExit:
            return # user asked for help
        if self.disabled:
            display(Markdown("DWS magic commands are disabled. To enable, set `DWS_MAGIC_DISABLE` to `False` and restart kernel."))
            return
        import pandas as pd # TODO: support case where pandas wasn't installed
        import numpy as np
        if args.heatmap:
            if args.baseline is not None:
                print("Cannot specify both --baseline and --heatmap", file=sys.stderr)
                return
        if args.max_count and args.tail:
            max_count = args.max_count
        elif args.tail:
            max_count = 10
        else:
            max_count = None
        history = get_snapshot_history(self.dws_jupyter_info.workspace_dir,
                                       max_count=max_count,
                                       reverse=args.tail)
        entries = []
        index = []
        columns = ['timestamp', 'hash', 'tags', 'message']
        baseline_snapshot = None # type: Optional[int]
        # not every snapshot has the same metrics, so we build an inclusive list
        metrics = [] # type: List[str]
        for s in history:
            d = {'timestamp':s.timestamp[0:19],
                 'hash':s.hashval[0:8],
                 'tags':', '.join([tag for tag in s.tags]),
                 'message':s.message if s.message is not None else ''}
            if s.metrics is not None:
                for (m, v) in s.metrics.items():
                    d[m] = v
                    if m not in columns:
                        columns.append(m)
                        metrics.append(m)
            entries.append(d)
            index.append(s.snapshot_number)
            if (args.baseline is not None):
                if args.baseline in s.tags:
                    baseline_snapshot = s.snapshot_number
                elif s.hashval[0:min(len(args.baseline),8)]==args.baseline[0:8]:
                    baseline_snapshot = s.snapshot_number
        if (args.baseline is not None) and (baseline_snapshot is None):
            print("Did not find a tag or hash corresponding to baseline '%s'"
                  % args.baseline, file=sys.stderr)
            return
        history_df = pd.DataFrame(entries, index=index, columns=columns)
        maximize_metrics = set(['accuracy', 'precision', 'recall'])
        if args.maximize_metrics:
            maximize_metrics = maximize_metrics.union(set(args.maximize_metrics.split(',')))
        minimize_metrics = set(['loss'])
        if args.minimize_metrics:
            minimize_metrics = minimize_metrics.union(set(args.minimize_metrics.split(',')))
        def truncate(v, l=30):
            s = repr(v)
            return s if len(s)<=(l-3) else s[0:l-3]+'...'
        def cleanup_dict_or_string_metric(val):
            if isinstance(val, dict) or isinstance(val, str):
                return truncate(val)
            else:
                return val
        element_styling_fns = [] # type: List[Tuple[str, Callable[[Any], None]]]
        if args.heatmap:
            heatmap_maximize_cols = [] # type: List[str]
            heatmap_minimize_cols = [] # type: List[str]
            color_templ="border: 1px solid darkgrey; background-color: %s; color: %s"
            # TODO: split this out to a separate function
            def color_max_metric_col(col):
                bins = _metric_col_to_colormap(col)
                return bins.apply(lambda b: color_templ%(MAXIMIZE_COLORMAP[b], 'white' if b<2 or b>4 else  'black') if b!=-1
                                            else color_templ%('lightgrey', 'black'))
            def color_min_metric_col(col):
                bins = _metric_col_to_colormap(col)
                return bins.apply(lambda b: color_templ%(MINIMIZE_COLORMAP[b], 'white' if b<2 or b>4 else  'black') if b!=-1
                                            else color_templ%('lightgrey', 'black'))
        class BaselineElementStyle:
            def __init__(self, metric:str, baseline, maximize:bool):
                self.metric=metric
                self.baseline=baseline
                self.baseline_round = abs(self.baseline*0.005)
                self.maximize=maximize
            def __call__(self, val):
                # if a value is within 0.5% of the baseline, we consider it baseline
                if pd.isna(val):
                    return 'color: grey'
                elif val>(self.baseline+self.baseline_round):
                    return 'color: green' if self.maximize else 'color: red'
                elif val<(self.baseline-self.baseline_round):
                    return 'color: red' if self.maximize else 'color: green'
                else: # within baseline rounding
                    return 'color: black; font-weight: bold'
        for metric in metrics:
            if history_df[metric].dtype.kind in ('f', 'i'):
                # float or int
                if baseline_snapshot is not None:
                    baseline_val = history_df.loc[baseline_snapshot][metric]
                    if metric in maximize_metrics:
                        element_styling_fns.append((metric, BaselineElementStyle(metric, baseline_val, maximize=True)),)
                    elif metric in minimize_metrics:
                        element_styling_fns.append((metric, BaselineElementStyle(metric, baseline_val, maximize=False)),)
                elif args.heatmap:
                    if metric in maximize_metrics:
                        heatmap_maximize_cols.append(metric)
                    elif metric in minimize_metrics:
                        heatmap_minimize_cols.append(metric)
            elif history_df[metric].dtype==np.dtype('object'):
                history_df[metric] = history_df[metric].apply(cleanup_dict_or_string_metric)
        result = history_df
        def get_style(df_or_style):
            return df_or_style.style if isinstance(df_or_style, pd.DataFrame) else df_or_style
        for (metric, styling_fn) in element_styling_fns:
            result = get_style(result).applymap(styling_fn, subset=[metric])
        if args.heatmap:
            result = get_style(result).apply(color_max_metric_col, subset=heatmap_maximize_cols)
            result = get_style(result).apply(color_min_metric_col, subset=heatmap_minimize_cols)
        return result

    @line_magic
    def dws_lineage_table(self, line):
        import pandas as pd # TODO: support case where pandas wasn't installed
        parser = DwsMagicParseArgs("dws_lineage_table",
                                   description="Show a table of lineage for the workspace's resources")
        parser.add_argument('--snapshot', default=None, type=str,
                            help="If specified, print lineage as of the specified snapshot hash or tag")
        try:
            args = parser.parse_magic_line(line)
        except DwsMagicArgParseExit:
            return # user asked for help
        if self.disabled:
            display(Markdown("DWS magic commands are disabled. To enable, set `DWS_MAGIC_DISABLE` to `False` and restart kernel."))
            return
        rows = [r for r in make_lineage_table(self.dws_jupyter_info.workspace_dir, args.snapshot)]
        return pd.DataFrame(rows, columns=['Resource', 'Lineage Type', 'Details', 'Inputs']).set_index('Resource')

    @line_magic
    def dws_lineage_graph(self, line):
        parser = DwsMagicParseArgs("dws_lineage_table",
                                   description="Show a graph of lineage for a resource")
        parser.add_argument('--resource', default=None, type=str,
                            help="Graph lineage from this resource. Defaults to the results resource. Error if not specified and there is more than one.")
        parser.add_argument('--snapshot', default=None, type=str,
                            help="If specified, graph lineage as of the specified snapshot hash or tag")
        try:
            args = parser.parse_magic_line(line)
        except DwsMagicArgParseExit:
            return # user asked for help
        if self.disabled:
            display(Markdown("DWS magic commands are disabled. To enable, set `DWS_MAGIC_DISABLE` to `False` and restart kernel."))
            return
        output_file = join(dirname(self.dws_jupyter_info.notebook_path),
                           'lineage_'+_remove_notebook_extn(self.dws_jupyter_info.notebook_name)+'.html')
        make_lineage_graph(output_file, self.dws_jupyter_info.workspace_dir,
                           resource_name=args.resource, tag_or_hash=args.snapshot,
                           width=780, height=380)
        return display(IFrame(basename(output_file), width=800, height=400))

    @line_magic
    def dws_results(self, line):
        parser = DwsMagicParseArgs("dws_results",
                                   description="Show results from a run (results.json file)")
        parser.add_argument('--resource', default=None, type=str,
                            help="Look for the results.json file in this resource. Otherwise, will look in all results resources and return the first match.")
        parser.add_argument('--snapshot', default=None, type=str,
                            help="If specified, get results as of the specified snapshot or tag. Otherwise, looks at current workspace and then most recent snapshot.")
        try:
            args = parser.parse_magic_line(line)
        except DwsMagicArgParseExit:
            return # user asked for help
        if self.disabled:
            display(Markdown("DWS magic commands are disabled. To enable, set `DWS_MAGIC_DISABLE` to `False` and restart kernel."))
            return
        rtn = get_results(self.dws_jupyter_info.workspace_dir,
                          tag_or_hash=args.snapshot, resource_name=args.resource)
        if rtn is None:
            print("Did not find a results.json file.", file=sys.stderr)
            return
        (results, rpath) = rtn
        import pandas as pd
        html_list = ['<h3>%s</h3>' % rpath]

        def truncate_dict(d, maxlen=50, roundme=False):
            d2 = {}
            for (k, v) in d.items():
                if roundme:
                    d2[k] = _fmt_scalar(v)
                else:
                    d2[k] = v
            s = repr(d2)
            if len(s)>maxlen:
                return s[0:(maxlen-3)]+'...'
            else:
                return s
        def subdict_to_df(d, parent_name, name, roundme=False):
            keys=[]
            values = []
            for (k, v) in d.items():
                if not isinstance(v, dict):
                    keys.append(k)
                    if roundme:
                        values.append(_fmt_scalar(v))
                    else:
                        values.append(v)
                else:
                    keys.append(k)
                    values.append(truncate_dict(v, roundme=roundme))
            df = pd.DataFrame({'Property':keys, 'Value':values}).set_index('Property')
            html_list.append("<h5>%s: %s</h5>"% (parent_name, name))
            html_list.append(df.to_html())
        def dict_to_df(d, name, roundme=False):
            keys=[]
            values = []
            subdicts = []
            for (k, v) in d.items():
                if not isinstance(v, dict):
                    keys.append(k)
                    if roundme:
                        values.append(_fmt_scalar(v))
                    else:
                        values.append(v)
                elif k not in ('parameters', 'metrics'):
                    subdicts.append((k, v))
            df = pd.DataFrame({'Property':keys, 'Value':values}).set_index('Property')
            html_list.append("<h4>%s</h4>"% name)
            html_list.append(df.to_html())
            for (k, v) in subdicts:
                subdict_to_df(v, name, k, roundme=roundme)
        dict_to_df(results, 'General Properties')
        if 'parameters' in results:
            dict_to_df(results['parameters'], 'Parameters')
        if 'metrics' in results:
            dict_to_df(results['metrics'], 'Metrics', roundme=True)
        return HTML('\n'.join(html_list))



def load_ipython_extension(ipython):
    ipython.register_magics(DwsMagics)

