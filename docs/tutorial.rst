.. _tutorial:

2. Tutorial
===========
Let's build on the `Quick Start <:ref:quickstart>`_.  If you haven't already, run
through it so that you have a `quickstart` workspace with one tag (``SVC-1``).

Status Command
--------------
We can check the status and history of our workspace with the ``dws status --history``
command::

  $ dws status --history
  Role source-data
  ----------------
    git repo sklearn-digits-dataset
  Role intermediate-data
  ----------------------
    No items with role intermediate-data
  Role code
  ---------
    git subdirectory code
  Role results
  ------------
    git subdirectory results
  
  History of snapshots
  Hash     Tags                 Created             Metric             Value        Message
  5ac8708  SVC-1                2019-04-13T16:47:28 accuracy           0.989        first run wih SVC
  Showing 1 of 1 snapshots
  Have now successfully shown the current status

Further Experiments
-------------------
Now, let's try to use Logistic Regression. Create a new Jupyter notebook called
``digits-lr`` in the ``code`` subdirectory of ``quickstart``. Enter the following
code into a notebook cell::

  import numpy as np
  from os.path import join
  from sklearn.linear_model import LogisticRegression
  from dataworkspaces.kits.scikit_learn import train_and_predict_with_cv
  
  DATA_DIR='../sklearn-digits-dataset'
  RESULTS_DIR='../results'
  
  data = np.loadtxt(join(DATA_DIR, 'data.csv'), delimiter=',')
  target = np.loadtxt(join(DATA_DIR, 'target.csv'), delimiter=',')
  train_and_predict_with_cv(LogisticRegression,
                            {'C':[1e-3, 1e-2, 1e-1, 1, 1e2], 'solver':['lbfgs'],
                             'multi_class':['multinomial']},
                            data, target,
                            DATA_DIR, RESULTS_DIR, random_state=42)

Note the only differences in our call to ``train_and_predict_with_cv`` are that
we pass a different classifier (``LogisticRegression``) and a ``param_grid``
with parameters appropriate to that classifier. If you run this cell,
you should see several no-convergence warnings (some of the values for ``C``
must be bad for this data set) and then a final result::

  Best params were: {'C': 0.01, 'multi_class': 'multinomial', 'solver': 'lbfgs'}
  accuracy: 0.97
  classification report:
                precision    recall  f1-score   support
  
           0.0       1.00      1.00      1.00        33
           1.0       0.97      1.00      0.98        28
           2.0       0.97      1.00      0.99        33
           3.0       1.00      0.97      0.99        34
           4.0       1.00      0.98      0.99        46
           5.0       0.94      0.94      0.94        47
           6.0       0.97      0.97      0.97        35
           7.0       1.00      0.97      0.99        34
           8.0       0.97      0.97      0.97        30
           9.0       0.95      0.97      0.96        40
  
     micro avg       0.97      0.97      0.97       360
     macro avg       0.98      0.98      0.98       360
  weighted avg       0.98      0.97      0.98       360
  
  Wrote results to results:results.json


Ok, so our Logistic Regression
accuracy of 0.97 is not as good as we obtained from the
Support Vector Classifier (0.989). Let's take a snapshot anyway,
so we have this experiment for future reference. Maybe someone will
ask us, "Did you try Logistic Regession?", and we can show them
the full results and even use a ``restore`` command to re-run the
experiment for them. Here's how to take the snapshot::

  dws snapshot -m "Logistic Regession experiment" LR-1

Saving a trained model
----------------------
Since the Support Vector Classifier gave the best results, let us train
a model with the full data set and save it to our results directory.
``train_and_predict_with_cv`` can do that for us if we specify the
``model_name`` parameter. Start the ``digits-svc`` notebook and add
``model_name='svc-best'`` to the call as follows::

  train_and_predict_with_cv(SVC, {'gamma':[0.01, 0.001, 0.0001]}, data, target,
                            DATA_DIR, RESULTS_DIR, random_state=42,
                            model_name='svc-best')

Now, run the cell. It should print the metrics as before and then the message:
"Wrote trained model to /path/to/results/svc-best.pkl". Save and quit
the notebook. From the ``code`` directory, let's run an ``ls`` command to see
what was generated::

  $ ls ../results
  README.txt	results.json	snapshots	svc-best.pkl

We see that the results.json file was generated as before and we have a new
file, ``svc-best.pkl``, which contains the pickled model. Let's now take a
snapshot: ``dws snapshot -m "trained the best model (SVC)" SVC-2``. If we
run the status command we can see the history of our experiments::

  dws status --history
  Role source-data
  ----------------
    git repo sklearn-digits-dataset
  Role intermediate-data
  ----------------------
    No items with role intermediate-data
  Role code
  ---------
    git subdirectory code
  Role results
  ------------
    git subdirectory results
  
  History of snapshots
  Hash     Tags                 Created             Metric             Value        Message
  69c469b  SVC-2                2019-04-14T08:05:17 accuracy           0.989        trained the best model (SVC)
  ce702b1  LR-1                 2019-04-14T07:37:24 accuracy           0.975        Logistic Regession experiment
  5ac8708  SVC-1                2019-04-13T16:47:28 accuracy           0.989        first run wih SVC
  Showing 3 of 3 snapshots

Publishing a workspace
----------------------
