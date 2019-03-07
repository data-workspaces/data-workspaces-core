.. _internals:

5. Internals: Developer's Guide
===============================
This section is a guide for people working on the development of Data Workspaces
or people who which to extend it (e.g. through their own resource types or
kits).

Installation and setup for development
--------------------------------------
In summary:

1. Install Python 3 via the Anaconda distribution.
2. Make sure you have the ``git`` and ``make`` utilties on your system.
3. Create a virtual environment called ``dws`` and activate it.
4. Install ``mypy`` via ``pip``.
5. Clone the main ``data-workspaces-python`` repo.
6. Install the ``dataworkpaces`` package into your virtual environment.
7. Download and configure ``rclone``.
8. Run the tests.

Here are the details:

We recommend using `Anaconda3 <https://www.anaconda.com/distribution/>`_
for development, as it can be easily installed on Mac and Linux and includes
most of the packages you will need for development and data science projects.
You will also need some basic system utilities: ``git`` and ``make`` (which may
already be installed).

Once you have Anaconda3 installed, create a virtual environment for your
work::

  conda create --name dws

To activate the environment::

  conda activate dws

You will need the `mypy <https://mypy.readthedocs.io/en/latest/>`_
type checker, which is run as part of the tests.
It is best to install via pip to get the latest version (some older versions
may be buggy). Once you have activated your environment, ``mypy`` may be installed
as follows::

  pip install mypy

Next, clone the Data Workspaces main source tree::

  git clone git@github.com:jfischer/data-workspaces-python.git

Now, we install the data workspaces library, via ``pip``, using an editable
mode so that our source tree changes are immediately visible::

  cd data-workspaces-python
  pip install --editable `pwd`

With this setup, you should not have to configure ``PYTHONPATH``.

Next, we install ``rclone``, a file copying utility used by the *rclone resource*.
You can download the latest ``rclone`` executable from http://rclone.org. Just make
sure that the executable is available in your executable path. Alternatively,
on Linux, you can install ``rclone`` via your system's package manager. To
configure ``rclone``, see the instructions :ref:`here <rclone_config>` in the
Resource Reference.

Now, you should be ready to run the tests::

  cd tests
  make test

The tests will print a lot to the console. If all goes well, it should
end with something like this::

  ----------------------------------------------------------------------
  Ran 40 tests in 23.664s

  OK

Overall Design
--------------
A data workspace is contained within a Git repository. The metadata about resources,
snapshots and lineage is stored in the subdirectory ``.dataworkspace``. The various
resources can be other subdirectories of the workspace's repository or may be
external to the main Git repo.

The `click <https://click.palletsprojects.com/en/7.x/>`_ package is used to
structure the command line interface.

Database Layout
~~~~~~~~~~~~~~~
The layout for the files under the ``.dataworkspace`` directory is as follows:

  * ``.dataworkspace/``

    * ``config.json`` - overall configuration (e.g. workspace name, global params)
    * ``resources.json`` - lists all resources and their config parameters
    * ``resource_local_params.json`` - configuration for resources that is local to
      this machine (e.g. path to the resource); not checked into git
    * ``current_lineage/`` - contains lineage files reflecting current state of each
      resource; not checked into git
    * ``file/`` - contains metadata for *local files* based resources; in particular,
      has the file-level hash information for snapshots
    * ``snapshots/`` - snapshot metadata

      * ``snapshot-<HASHCODE>.json`` - lists the hashes for each resource in the
        snapshot. The hash of this file is the hash of the overall snapshot.
      * ``snapshot_history.json`` - metadata for the past snapshots

    * ``snapshot_lineage/`` - contains lineage data for past snapshots

      * ``<HASHCODE>/`` - directory containing the current lineage files at
        the time of the snapshot associated with the hashcode.
        Unlike ``current_lineeage``, this is checked into git.

In designing the workspace database, we try to follow the following
guidelines:

1. Use JSON as our file format where possible - it is human readable and editable
   and easy to work with from within Python.
2. Local or uncommitted state is not stored in Git (we use .gitignore to keep
   the files outside of the repo). Such files are initialized by ``dws init``
   and ``dws clone``.
3. Avoid git merge conflicts by storing data in seperate files where possible.
   For example, the resources.json file should really be broken up into
   one file per resource, stored under a common directory (see issue #13).
4. Use git's design as an inspiration. It provides an efficient and flexible
   representation.

Code Layout
~~~~~~~~~~~
The code is organized as follows:

  * ``dataworkspaces/``

    * ``api.py`` - API to run a subset of the workspace commands from Python.
      This is useful for building integrations.
    * ``dws.py`` - the command line interface
    * ``errors.py`` - common exception class definitions
    * ``lineage.py`` - the generic lineage api
    * ``utils/`` - lower level utilities used by the upper layers
    * ``resources/`` - base class for resources and implementations of the resource types
    * ``commands/`` - implementations of the individual dws commands
    * ``third_party/`` - third-party code (e.g. git-fat)
    * ``kits/`` - adapters to specific external technologies
    
Command Design
--------------
Each command has a *validation* phase and an *execution* phase. The goal is to
do all the checks up front before making any changes to the state of the
resources or the workspace. This is supported by the ``Action`` class
and associated infrastructure.

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

Snapshot
~~~~~~~~
Taking a snapshot involves instantiating resource objects for each resource
in resources.json and calling ``snapshot_prechecks()`` and ``snapshot()``.

Restore
~~~~~~~
Restore has some options to let you specify which resources to restore
and which to leave in their current state (``--only`` and ``--leave``). Restore may
create a new snapshot if the state of the resources does not exactly match
the original snapshot's state. If ``--no-new-snapshot`` is
specified, we adjust the individual resource
states without taking a new snapshot.

To implement restore for a new resource type, you just need to implement the
``restore_prechecks()`` and ``restore()`` methods. Both take a hashval parameter. In the
``restore_prechecks()`` call, you should validate that there is a state corresponding
to that hash.

There are a few edge cases that may need further thought:

* It is possible for the restore command to create a snapshot matching a previous one. We detect this situation, but don't do anything about it. It should be fine - there will just be an extra snapshot_history entry, but only one snapshot file.
* The restore for the git resource does a hard reset, which resets both the current workspace of the repo and the HEAD. I'm not sure whether we want that behavior or just to reset the workspace.

Resource Design
---------------
Resources are orthoginal to commands and represent the collections of
files to be versioned.

A resource may have one of four roles:

1. **Source Data Set** - this should be treated read-only by the ML
   pipeline. Source data sets can be versioned.
2. **Intermediate Data** - derived data created from the source data set(s)
   via one or more data pipeline stages.
3. **Results** - the outputs of the machine learning / data science process.
4. **Code** - code used to create the intermediate data and results, typically
   in a git repository or Docker container.

The treatment of resources may vary based on the role. We now look at
resource functionality per role.

Source Data Sets
~~~~~~~~~~~~~~~~
We want the ability to name source data sets and swap them in and out without
changing other parts of the workspace. This still needs to be implemented.

Intrermediate Data
~~~~~~~~~~~~~~~~~~
For intermediate data, we may want to delete it from the current state of
the workspace if it becomes out of date (e.g. a data source version is changed
or swapped out). This still needs to be implemented.

Results
~~~~~~~
In general, results should be additive.

For the ``snapshot`` command, we move the results to a specific subdirectory per
snapshot. The name of this subdirectory is determined by a template that can
be changed by setting the parameter ``results.subdir``. By default, the template
is: ``{DAY}/{DATE_TIME}-{USER}-{TAG}``. The moving of files is accomplished via the
method ``results_move_current_files(rel_path, exclude)`` on the `Resource <resources>`
class. The ``snapshot()`` method of the resource is still called as usual, after
the result files have been moved.

Individual files may be excluded from being moved to a subdirectory. This is done
through a configuration command. Need to think about where this would be stored --
in the resources.json file? The files would be passed in the exclude set to
``results_move_current_files``.

If we run ``restore`` to revert the workspace to an
older state, we should not revert the results database. It should always
be kept at the latest version. This is done by always putting results
resources into the leave set, as if specified in the ``--leave`` option.
If the user puts a results resource in the ``--only`` set, we will error
out for now.
