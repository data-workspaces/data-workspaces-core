===============
Data Workspaces
===============
Easy mangement of source data, intermediate data, and results for
data science projects.

To install for development
==========================
First, create a virtual environment. If you are using Anaconda3,
here are the steps::

    conda create --name dws

To activate the environment::

    source activate dws

Now, install the data workspaces library via pip::

    pip install --editable `pwd`


You can edit the files directly in your git repo -- the changes will
be reflected when you run the commands.

Command Line Interface
======================
To run the command line interface, you use the ``dws`` command,
which should have been installed into your environment by ``pip install``.
``dws`` operations have the form::

    dws [GLOBAL_OPTIONS] SUBCOMMAND [SUBCOMMAND_OPTIONS] [SUBCOMMAND_ARGS]

Just run ``dws --help`` for a list of global options and subcommands.

Here is a summary of the (planned) subcommands:

* ``init`` - initialize a new workspace in the current directory
* ``add`` - add a *resource* (a git repo, a directory, an s3 bucket, etc.)
  to the current workspace
* ``snapshot`` - take a snapshot of the current state of the workspace
* ``restore`` - restore the state to a prior snapshot
* ``run`` - run a command and capture the lineage. This will be done by
  inspecting the arguments and asking the user interactively to provide
  any missing information. This information is saved in a file for
  future calls to the same command.

Design
======
A *workspace* contains references to *resources*, where a resource is a versioned
collection of files that may be stored as a local directory, a git repository,
an S3 bucket, etc. A *snapshot* is a particular state of all the resources
that can be restored upon request. The metadata about resources and snapshots
is stored in JSON files, in the subdirectory ``.dataworkspace`` under the root
directory of a given workspace.

Resource Roles
--------------
A resource (collection of files) may have one of four roles:

1. **Source Data Set** - this should be treated read-only by the ML
   pipeline. Source data sets can be versioned.
2. **Intermediate Data** - derived data created from the source data set(s)
   via one or more data pipeline stages.
3. **Results** - the outputs of the machine learning / data science process.
4. **Code** - code used to create the intermediate data and results, typically
   in a git repository or Docker container.

The treatment of resources may vary based on the role. For example:

* We want the ability to name source data sets and swap them in and out without
  changing other parts of the workspace.
* For intermediate data, we may want to delete it from the current state of
  the workspace if it becomes out of date (e.g. a data source version is changed
  or swapped out).
* Results should be additive. For example, if we revert the workspace to an
  older state, we should not revert the results database. It should always
  be kept at the latest version.

Code Organization
-----------------
We use the Python library ``click`` (http://click.pocoo.org/6/) to implement
the command argument parsing. The implementations of individual commands
may be found in the ``commands/`` subdirectory.

Actions
~~~~~~~
We wish to perform all the
checks of a command up front and then only run the steps when we know they
will succeed. This is done through *actions*, as defined in ``commands/actions.py``.
Each ``Action`` subclass performs any necesary checks in its ``__init__()`` method.
The actual execution of the action is in the ``run()`` method. Commands instantiate
the actions they need, add them to a list (called the *plan*), and when all
checks have been performed, execute the actions via the function
``actions.run_plan()``. When running in verbose mode, we also print the
list of actions to perform and ask the user for confirmation.

Resources
~~~~~~~~~
Resources are orthoginal to actions and represent the collections of
files to be versioned.

Example Workflows
=================
Here are a few example workflows using the command line interface.
Lines with user input start with the shell prompt ``$``.

First, we create our workspace and define our resources
(a remote s3 bucket, a local git repo and two subdirectories):

.. code:: bash

   $ cd /home/joe/example-workspace
   $ dws init
   Created workspace 'example-workspace'.
   $ dws add source-data s3://data-bucket
   Added s3 resource 'data-bucket' as source data.
   $ dws add code ./myrepo
   Added git resource './myrepo' as code.
   $ dws add intermediate-data ./intermediate
   Added local resource './intermediate' as intermediate data.
   $ dws add results ./results
   Added local resource './results' as result data.
   $ dws set-hook merge merge-json ./results/results.csv

The last line indicates that, when we take a snapshot, we merge ``results.csv`` with
the previous version, creating a combined csv file that includes all the results.
By default, overwriting a results file will cause the previous version to be renamed
upon taking the snapshot (e.g. the previous version becomes results.csv.v1 if the previous
snapshot was tagged with "v1").

Now, we can run our scripts and then take a snapshot:

.. code:: bash

   $ python ./myrepo/extract_features.py -o ./intermediate/features.csv s3://data-bucket
   $ python ./myrepo/train.py --solver=SVC ./intermediate/features.csv ./results/results.csv
   $ dws snapshot v1
   Created snapshot with hash '34A440983F' and tag 'v1'.

If we list the local files in our workspace at this point, we see:

.. code:: bash

   $ ls -R
   ./intermediate:
   features.csv

   ./myrepo:
   extract_featues.py            train.py

   ./results:
   results.csv

We make some changes to the code, do another run, and take a second snapshot:

.. code:: bash

   $ cd myrepo; vi extract_features.py
   $ git add extract_features.py; git commit -m "some changes to feature extraction"
   $ cd ..
   $ python ./myrepo/extract_features.py -o ./intermediate/features.csv s3://data-bucket
   $ python ./myrepo/train.py --solver=SVC ./intermediate/features.csv ./results/results.csv
   $ dws snapshot v2
   Created snapshot with hash 'FF83830484' and tag 'v2'.

Let's say we wanted to go back to the previous version, but run with a different solver.
We do not need to rerun the first step, as the intermediate data has been restored
as well.

.. code:: bash

   $ dws revert v1
   Reverted to snapshot with hash '34A440983F' and tag 'v1'.
   $ python ./myrepo/train.py --solver=SVC ./intermediate/features.csv ./results/results.csv
   $ dws snapshot v3
   Created snapshot with hash 'A3838492B3' and tag 'v3'.

