.. _resources:

6. Resource Reference
=====================
This section provide a little detail on how to use specific
resource types. For specific command line options, please see the
:ref:`Command Reference <commands>`.

Git resources
-------------
The ``git`` resource type provides project tracking and management for Git repositories.
There are actually two types of git resources supported:

1. A standalone repository. This can be either one controller by the user or, for
   source data, a 3rd party repository to be treated as a read-only resource.
2. A subdirectory of the data workspace's git repository can be treated as a separate
   resource. This is especially convenient for small projects, where multiple types
   of data (source data, code, results, etc.) can be kept in a single repository but
   versioned independently.

When running the ``dws add git ...`` commend, the type of repository (standalone vs.
subdirectory of the main workspace) is automatically detected. In either case, it is
expected that there is a local copy of the repository available when adding it as
a resource to the workspace. It is recommended, but not required, to have a remote
``origin``, so that the ``push``, ``pull`` and ``clone`` commands can work with
the resource.

Examples
~~~~~~~~
When initializing a new workspace, one can add sub-directory resources for any and
each of the resource roles (source-data, code, intermediate-data, and results).
This is done via the ``--create-resources`` option as follows::

  $ mkdir example-ws
  $ cd example-ws/
  $ dws init --create-resources=code,results
  $ dws init --create-resources=code,results
    Have now successfully initialized a workspace at /Users/dws/code/t/example-ws
    Will now create sub-directory resources for code, results
    Added code to git repository
    Have now successfully Added Git repository subdirectory code in role 'code' to workspace
    Added results to git repository
    Have now successfully Added Git repository subdirectory results in role 'results' to workspace
    Finished initializing resources:
      code: ./code
      results: ./results
  $ ls
  code	results

Here is an example from the :ref:`Quick Start <quickstart>` where we
add an entire third party repository to our workspace as a read-only resource.
We first clone it into a subdirectory of the workspace and then tell ``dws``
about it:

.. code-block:: bash

  git clone https://github.com/jfischer/sklearn-digits-dataset.git
  dws add git --role=source-data --read-only ./sklearn-digits-dataset

Git-fat integration: Support for Large Files
--------------------------------------------
It can be nice to manage your golden source data in a Git repository.
Unfortunately, due to its architecture and focus as a source code tracking
system, Git can have significant performance issues with large files.
Furthermore, hosting services like GitHub place limits on the size of individual
files and on commit sizes. To get around this, various extensions to Git
have sprung up. Data Workspaces currently integrates with one of them,
`git-fat <https://github.com/jedbrown/git-fat>`_.

Git-fat allows you to
store your large files on a host you control that is accessible via
``ssh`` (or other protocols supported through ``rsync``). The large
files themselves are hashed and stored on the (remote) server. The
metadata for these files is stored in the git repository and versioned
with the rest of your git files.


Git-fat is just
a Python script, which we ship as a part of the ``dataworkspaces`` Python
package. [#gitfat1]_  Running ``pip install dataworkspaces`` will put ``git-fat``
into your path and make it available to your ``git`` commands and ``dws``
commands.

We support the following integration points with ``git-fat``:

1. The git repo for the workspace itself can be git-fat enabled when it is
   created. This is done through command line options on ``dws init``.
   See the :ref:`Command Reference <commands>` entry for details (or the
   example below).
2. Any ``dws push`` or ``dws pull`` of a git-fat-enabled workspace will
   automatically call the associated git-fat command for the workspace's main
   repo.
3. If you add a git repository as a resource to the workspace, and it has a
   ``.gitfat`` file, then any ``dws push`` or ``dws pull`` commands will
   automatically call the associated git-fat commands.
4. As mentioned above, git-fat is included in the dataworkspaces package and
   installed in your path.


.. [#gitfat1] Unfortunately, ``git-fat`` is written in Python 2, so you will need to have Python 2 available on your system to use it.

Example
~~~~~~~
Here is an example using git-fat to store all gzipped files of the workspace's main
git repo on a remote server.

First, we set up a directory on our remote server to store the large files:

.. code-block:: bash

   fat@remote-server $ mkdir ~/fat-store

Now, back on our personal machine, we initialize a workspace, specifying the
remote server and that .gz files should be managed by git-fat:

.. code-block:: bash

  local $ mkdir git-fat-example
  local $ cd git-fat-example/
  local $ dws init --create-resources=source-data \
                   --git-fat-remote=remote-server:/home/fat/fat-store \
                   --git-fat-user=fat --git-fat-attributes='*.gz'
  local $ ls
  source-data

A bit later, we've added some .gz files to our source data resource. We
take a snapshot and then ``dws push`` to the origin:

.. code-block:: bash

   local $ ls source-data
   README.txt			census-state-populations.csv.gz	zipcode.csv.gz
   local $ dws snapshot s1
   local $ dws push # this will also push to the remote fat store

If we now go to the remote store, we can see the hashed files:

.. code-block:: bash

  fat@remote-server $ ls fat-store
  26f2cac452f70ad91da3ccd05fc40ba9f03b9f48  d9cc0c11069d76fe9435b9c4ca64a335098de2d7

Our local workspace has our full files, which can be used by our
scripts as-is. However, if you look at the origin repository, you
will find the content of each .gz file replaced by a single line
referencing the hash. If you clone this repo, you will get the
full files, through the magic of git-fat.


.. _rclone_config:

Adding resources using rclone
-----------------------------
The rclone resource type leverages the
`rclone <https://rclone.org>`_ command line utility to
provide synchronization with a variety of remote data services.

``dws add rclone [options] source-repo target-repo``

*dws add rclone* adds a remote repository set up using rclone.

We use rclone to set up remote repositories.

Example
~~~~~~~
We use rclone config to set up a repository pointing to a local directory::

  $ rclone config show
  ; empty config

  $ rclone config create localfs local unc true

The configuration file (typically at ``~/.config/rclone/rclone.conf``)
now looks like this::

  [localfs]
  type = local
  config_automatic = yes
  unc = true


Next, we use the backend to add a repository to dws::

  $ dws add rclone --role=source-data my_local_files:/Users/rupak/tmp tmpfiles

This creates a local directory tmpfiles and copies the contents of /Users/rupak/tmp to it.

Similarly, we can make a remote S3 bucket::

  $ rclone config
  mbk-55-51:docs rupak$ rclone --config=rclone.conf config
  Current remotes:

  Name                 Type
  ====                 ====
  localfs              local

  e) Edit existing remote
  n) New remote
  d) Delete remote
  r) Rename remote
  c) Copy remote
  s) Set configuration password
  q) Quit config
  e/n/d/r/c/s/q> n
  name> s3bucket
  Type of storage to configure.

  # Pick choice 4 for S3 and configure the bucket
  ...
  # set configuration parameters

Once the S3 bucket is configured, we can get files from it::

  $ dws add rclone --role=source-data s3bucket:mybucket s3files


Configuration Files
~~~~~~~~~~~~~~~~~~~

By default, we use the default configuration file used by rclone. This is the file printed out by::

  $ rclone config file

and usually resides in ``$HOME/.config/rclone/rclone.conf``

However, you can specify a different configuration file::

  $ dws add rclone --config=/path/to/configfile --role=source-data localfs:/Users/rupak/tmp tmpfiles

In this case, make sure the config file you are using has the remote ``localfs`` defined.
