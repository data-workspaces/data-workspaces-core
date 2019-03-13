.. Data Workspaces documentation master file, created by
   sphinx-quickstart on Wed Mar  6 12:09:47 2019.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

***************
Data Workspaces
***************

Data management for reproducability and collaboration
#####################################################

Data Workspaces is an open source framework for maintaining the
state of a data science project, including data sets, intermediate
data, results, and code. It supports reproducability through snapshotting
and lineage models and collaboration through a push/pull model
inspired by source control systems like Git.

Data Workspaces is installed as a Python 3 package and provides a
Git-like command line interface and programming APIs. Specific data
science tools and workflows are supported through extensions
called *kits*. The goal is to provide the reproducibility and collaboration
benefits with minimal changes to your current projects and processes.

Data Workspaces runs on
Unix-like systems, including Linux, MacOS, and on Windows via the
`Windows Subsystem for Linux <https://docs.microsoft.com/en-us/windows/wsl/install-win10>`_.

.. toctree::
   :maxdepth: 2
   :caption: Contents:

   intro
   tutorial
   commands
   resources
   lineage
   internals


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
