.. _kits:

5. Kits Reference
=================
In this section, we cover *kits*, integrations with various data science
libraries and infrastructure provided by Data Workspaces.

Jupyter
-------
.. automodule:: dataworkspaces.kits.jupyter
   :no-undoc-members:
   :members: NotebookLineageBuilder, is_notebook, get_step_name_for_notebook

Magics
~~~~~~
This module also provides a collection of IPython `magics <https://ipython.readthedocs.io/en/stable/interactive/magics.html>`_
(macros) to simplify interactions with your data workspace when develping in a Jupyter Notebook.

Limitations
...........
Currently these magics are only supported in interactive Jupyter Notebooks. They do not run properly
within JupyterLab (we are currently working on an extension specific to JupyterLab),
the ``nbconvert`` command, or if you run the entire notebook with "Run All Cells".

To develop a notebook interactively using the DWS magic commands and then run the same notebook
in batch mode, you can set the variable ``DWS_MAGIC_DISABLE`` in your notebook, ahead of the
call to load the magics (``%load_ext``). If you set it to ``True``, the commands will be
loaded in a disabled state and will run with no effect. Setting ``DWS_MAGIC_DISABLE`` to
``False`` will load the magics in the enabled state and run all commands normally.

Loading the magics
..................
To load the magics, run the following in an interactive cell of your Jupyter Notebook::

  import dataworkspaces.kits.jupyter
  %load_ext dataworkspaces.kits.jupyter

If the load runs correctly, you should see output like this in your cell:

  *Ran DWS initialization. The following magic commands have been added to your notebook:*

  * ``%dws_info`` *- print information about your dws environment*
  * ``%dws_history`` *- print a history of snapshots in this workspace*
  * ``%dws_snapshot`` *- save and create a new snapshot*
  * ``%dws_lineage_table`` *- show a table of lineage for the workspace resources*
  * ``%dws_lineage_graph`` *- show a graph of lineage for a resource*
  * ``%dws_results`` *- show results from a run (results.json file)*

  *Run any command with the* ``--help`` *option to see a list of options for that command.*
  *The variable* ``DWS_JUPYTER_NOTEBOOK`` *has been added to your variables, for use in future DWS calls.*

  *If you want to disable the DWS magic commands (e.g. when running in a batch context),*
  *set the variable* ``DWS_MAGIC_DISABLE`` *to* ``True`` *ahead of the* ``%load_ext`` *call.*


Magic Command reference
.......................
We now describe the command options for the individual magics.

**%dws_info**

  usage: dws_info [-h]
  
  Print some information about this workspace
  
  optional arguments:
    -h, --help  show this help message and exit

**%dws_history**

  usage: dws_history [-h] [--max-count MAX_COUNT] [--tail]
  
  Print a history of snapshots in this workspace
  
  optional arguments:
    -h, --help            show this help message and exit
    --max-count MAX_COUNT
                          Maximum number of snapshots to show
    --tail                Just show the last 10 entries in reverse order

**%dws_snapshot**

  usage: dws_snapshot [-h] [-m MESSAGE] [-t TAG]
  
  Save the notebook and create a new snapshot
  
  optional arguments:
    -h, --help            show this help message and exit
    -m MESSAGE, --message MESSAGE
                          Message describing the snapshot
    -t TAG, --tag TAG     Tag for the snapshot. Note that a given tag can only
                          be used once (without deleting the old one).

**%dws_lineage_table**

  usage: dws_lineage_table [-h] [--snapshot SNAPSHOT]
  
  Show a table of lineage for the workspace's resources
  
  optional arguments:
    -h, --help           show this help message and exit
    --snapshot SNAPSHOT  If specified, print lineage as of the specified
                         snapshot hash or tag

**%dws_lineage_graph**

  usage: dws_lineage_table [-h] [--resource RESOURCE] [--snapshot SNAPSHOT]
  
  Show a graph of lineage for a resource
  
  optional arguments:
    -h, --help           show this help message and exit
    --resource RESOURCE  Graph lineage from this resource. Defaults to the
                         results resource. Error if not specified and there is
                         more than one.
    --snapshot SNAPSHOT  If specified, graph lineage as of the specified
                         snapshot hash or tag

**%dws_results**

  usage: dws_results [-h] [--resource RESOURCE] [--snapshot SNAPSHOT]
  
  Show results from a run (results.json file)
  
  optional arguments:
    -h, --help           show this help message and exit
    --resource RESOURCE  Look for the results.json file in this resource.
                         Otherwise, will look in all results resources and
                         return the first match.
    --snapshot SNAPSHOT  If specified, get results as of the specified snapshot
                         or tag. Otherwise, looks at current workspace and then
                         most recent snapshot.


Scikit-learn
------------

.. automodule:: dataworkspaces.kits.scikit_learn
   :no-undoc-members:
   :members: load_dataset_from_resource,LineagePredictor,Metrics,BinaryClassificationMetrics,MulticlassClassificationMetrics


TensorFlow
----------

.. automodule:: dataworkspaces.kits.tensorflow
   :no-undoc-members:
   :members: DwsModelCheckpoint,CheckpointConfig,add_lineage_to_keras_model_class
  
