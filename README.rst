===============
Data Workspaces
===============
`Data Workspaces <https://dataworkspaces.ai>`_ is an open source framework for maintaining the
state of a data science project, including data sets, intermediate
data, results, and code. It supports reproducability through snapshotting
and lineage models and collaboration through a push/pull model
inspired by source control systems like Git.

Data Workspaces is installed as a Python 3 package and provides a
Git-like command line interface and programming APIs. Specific data
science tools and workflows are supported through extensions
called *kits*. Currently, this includes Scikit-learn, TensorFlow,
and Jupyter Notebooks. The goal is to provide the reproducibility and collaboration
benefits with minimal changes to your current projects and processes.

Data Workspaces runs on
Unix-like systems, including Linux, MacOS, and on Windows via the
Windows Subsystem for Linux.

.. image:: https://travis-ci.org/data-workspaces/data-workspaces-core.svg?branch=master

Quick Start
===========
Please see the
`Quickstart Section <https://data-workspaces-core.readthedocs.io/en/latest/intro.html#quick-start>`_
of the documentation.

Documentation
=============
The documentation is available here: https://data-workspaces-core.readthedocs.io/en/latest/. The source for the documentation is under ``docs``. To build it locally, install
`Sphinx <https://www.sphinx-doc.org/en/master/>`_ and run the following::

  cd docs
  pip install -r requirements.txt # extras needed to build the docs
  make html

To view the local documentation, open the file ``docs/_build/html/index.html`` in your
browser.

License
=======
This code is copyright 2018 - 2020 by the Max Planck Institute for Software Systems and Data-ken
Research. It is licensed under the Apache 2.0 license. See the file LICENSE.txt for details.
