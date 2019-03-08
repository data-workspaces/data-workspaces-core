===============
Data Workspaces
===============
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
Windows Subsystem for Linux.

Quick Start
===========
Here is a quick example to give you a flavor of the project, using
`scikit-learn <https://scikit-learn.org>`_
and the famous digits dataset running in a Jupyter Notebook.

First, install the libary::

  pip install dataworkspaces

Now, we will create a workspace::

  mkdir quickstart
  cd ./quickstart
  dws init --create-resources code,results

This created our *workspace* (which is a git repository under the covers)
and initialized it with two subdirectories,
one for the source code, and one for the results. These are special
subdirectories, in that they are *resources* which can be tracked and versioned
independently.

Now, we are going to add our source data to the workspace. This resides in an
external, third-party git repository. It is simple to add::

  git clone https://github.com/jfischer/sklearn-digits-dataset.git
  dws add git --role=source-data ./sklearn-digits-dataset

Now, we can create a Jupyter notebook for running our experiments::

  cd ./code
  jupyter notebook

This will bring up the Jupyter app in your brower. Click on the *New*
dropdown (on the right side) and select "Python 3". Once in the notebook,
click on the current title ("Untitled", at the top, next to "Jupyter")
and change the title to ``digits-svc``.

Now, type the following Python code in the first cell::

  import numpy as np
  from os.path import join
  from sklearn.svm import SVC
  from dataworkspaces.kits.sklearn import train_and_predict_with_cv
  
  DATA_DIR='../sklearn-digits-dataset'
  RESULTS_DIR='../results'
  
  data = np.loadtxt(join(DATA_DIR, 'data.csv'), delimiter=',')
  target = np.loadtxt(join(DATA_DIR, 'target.csv'), delimiter=',')
  train_and_predict_with_cv(SVC, {'gamma':[0.01, 0.001, 0.0001]}, data, target,
                            DATA_DIR, RESULTS_DIR, random_state=42)

Now, run the cell. It will take a few seconds to train and test the
model. You should then see::

  Best params were: {'gamma': 0.001}
  accuracy: 0.99
  classification report:
                precision    recall  f1-score   support
  
           0.0       1.00      1.00      1.00        33
           1.0       1.00      1.00      1.00        28
           2.0       1.00      1.00      1.00        33
           3.0       1.00      0.97      0.99        34
           4.0       1.00      1.00      1.00        46
           5.0       0.98      0.98      0.98        47
           6.0       0.97      1.00      0.99        35
           7.0       0.97      0.97      0.97        34
           8.0       1.00      1.00      1.00        30
           9.0       0.97      0.97      0.97        40
  
     micro avg       0.99      0.99      0.99       360
     macro avg       0.99      0.99      0.99       360
  weighted avg       0.99      0.99      0.99       360
  
  Wrote results to results:results.json

Now, you can save and shut down your notebook. If you look at the
directory ``quickstart/results``, you should see a ``results.json``
file with information about your run.

Next, let us take a *snapshot*, which will record the state of
the workspace and save the data lineage along with our results::

  dws snapshot -m "first run with SVC" SVC-1

``SVC-1`` is the *tag* of our snapshot.
If you look in ``quickstart/results``, you will see that the results
(currently just ``results.json``) have been moved to the subdirectory
``snapshots/HOSTNAME-SVC-1``). A file, ``lineage.json``, containing a full
data lineage graph for our experiment has also been
created in that directory.

Some things you can do from here:

* Run more experiments and save their results by snapshotting the workspace.
  If, at some point, we want to go back to our first experiment, we can run:
  ``dws restore SVC-1``. This will restore the state of the source data and
  code subdirectories, but leave the full history of the results.
* Upload your workspace on GitHub or an any other Git hosting application.
  This can be to have a backup copy or to share with others.
  Others can download it via ``dws clone``.
* More complex scenarios involving multi-step data pipelines can easily
  be automated. See the documentation for details.

Documentation
=============
The documentation is available here: https://data-workspaces-core.readthedocs.io/en/latest/. The source for the documentation is under ``docs``. To build it locally, install
`Sphinx <https://www.sphinx-doc.org/en/master/>`_ and run the following::

  cd docs
  make html

To view the local documentation, open the file ``docs/_build/html/index.html`` in your
browser.

License
=======
This code is copyright 2018, 2019 by the Max Planck Institute for Software Systems and Data-ken
Research. It is licensed under the Apache 2.0 license. See the file LICENSE.txt for details.
