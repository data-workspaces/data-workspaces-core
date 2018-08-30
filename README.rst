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
