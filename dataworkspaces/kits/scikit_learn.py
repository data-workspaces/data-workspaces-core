"""
This module (``dataworkspaces.kits.scikit_learn``)
provides integration with the `scikit-learn <https://scikit-learn.org>`_
framework. The main function provided here is
:func:`~train_and_predict_with_cv`, which
runs a common sklearn classification workflow. This module
also provides :class:`~Metrics` and its subclasses, which
support the computation of common metrics and the writing of them
to a results file.
"""

from typing import Optional, Union, Dict, List, Any
from sklearn.base import ClassifierMixin
from sklearn import metrics
from sklearn.model_selection import GridSearchCV, train_test_split
from sklearn.externals import joblib
from sklearn.utils import Bunch
import sys
import numpy as np
import os
from os.path import join, abspath, expanduser, exists
import json
import glob

from dataworkspaces.errors import ConfigurationError
from dataworkspaces.utils.workspace_utils import get_workspace
from dataworkspaces.resources.resource import CurrentResources
from dataworkspaces.lineage import LineageBuilder
from dataworkspaces.utils.lineage_utils import ResourceRef
from .jupyter import is_notebook, get_step_name_for_notebook, get_notebook_directory

def _load_dataset_file(dataset_path, filename):
    filepath = join(dataset_path, filename)
    if filename.endswith('.txt') or filename.endswith('.rst'):
        with open(filepath, 'r') as f:
            return f.read()
    elif filename.endswith('.csv') or filename.endswith('.csv.gz') or \
         filename.endswith('.csv.bz2'):
        try:
            return np.loadtxt(filepath, delimiter=',')
        except ValueError:
            # try with pandas
            import pandas
            df = pandas.read_csv(filepath)
            if len(df.values.shape)==2 and df.values.shape[1]==1: # this is just a list
                return df.values.reshape(df.values.shape[0])
            else:
                return df.values
    elif filename.endswith('.npy'):
        return np.load(filepath)


def load_dataset_from_resource(resource_name:str, subpath:Optional[str]=None,
                               workspace_dir:Optional[str]=None)\
                               -> Bunch:
    """
    Load a datset (data and targets) from the specified resource, and returns an
    sklearn-style Bunch (a dictionary-like object). The bunch will include at least
    three attributes:

    * ``data`` - a NumPy array of shape number_samples * number_features
    * ``target`` - a NumPy array of length number_samples
    * ``resource`` - a :class:`~ResourceRef` that provides the resource name and
      subpath (if any) for the data

    Some other attributes that may also be present, depending on the data set:

    * ``DESCR`` - text containing a full description of the data set (for humans)
    * ``feature_names`` - an array of length number_features containing the name
      of each feature.
    * ``target_names`` - an array containing the name of each target class

    Data sets may define their own attributes as well (see below).

    The ``data`` and ``target`` attributes can be used directly (e.g. passed to
    ``train_test_split()``) or the entire bunch used as a parameter to
    :func:`~train_and_predict_with_cv`.

    **Parameters**

    resource_name
        The name of the resource containing the dataset.

    subpath
        Optional subpath within the resource where this specific dataset is located.
        If not specified, the root of the resource is used.

    workspace_dir
       The root directory of your workspace in the local file system. Usually,
       this can be left unspecified and inferred by DWS, which will search up
       from the current working directory.

    **Creating a Dataset**

    To create a dataset in your resource that is suitable for importing by this function,
    you simply need to create a file for each attribute you want in the bunch and place
    all these files in the same directory within your resource.
    The names of the files should be ``ATTRIBUTE.extn`` where ``ATTRIBUTE`` is the
    attribute name (e.g. ``data`` or ``DESCR``) and ``.extn`` is a file extension
    indicating the format. Supported file extensions are:

    * ``.txt`` or ``.rst`` - text files
    * ``.csv`` - csv files. These are read in using ``numpy.loadtxt()``. If this
      fails because the csv does not contain all numeric data, pandas is used to read
      in the file. It is then converted back to a numpy array.
    * ``.csv.gz`` or ``.csv.bz2`` - these are compressed csv files which are treated
      the same was as csv files (numpy and pandas will automatically uncompress before parsing).
    * ``.npy`` - this a a file containing a serialized NumPy array saved via ``numpy.save()``.
      It is loaded using ``numpy.load()``.
    """
  
    workspace_dir = get_workspace(workspace_dir)
    resources = CurrentResources.read_current_resources(workspace_dir, batch=True,
                                                        verbose=False)
    resources.validate_resource_name(resource_name, subpath)
    dataset_name = 'Resource ' + resource_name + ' subpath ' + subpath \
                   if subpath is not None \
                   else 'Resource ' + resource_name
    r = resources.by_name[resource_name]
    local_path = r.get_local_path_if_any()
    if local_path is None:
        # TODO: Support a data access api
        raise ConfigurationError("Unable to instantiate a data set for resource '%s': currently not supported for non-local resources"%
                                 resource_name)
    dataset_path = join(local_path, subpath) if subpath is not None else local_path
    result = {} # this will be the args to the result Bunch
    # First load data and target files, which are required
    data_file = join(dataset_path, 'data.csv')
    if exists(data_file):
        pass
    elif exists(data_file+'.gz'):
        data_file += '.gz'
    elif exists(data_file+'.bz2'):
        data_file += '.bz2'
    else:
        raise ConfigurationError("Did not find data file for %s at '%s'"%
                                 (dataset_name, data_file))
    result['data'] = np.loadtxt(data_file, delimiter=',')
    target_file = join(dataset_path, 'target.csv')
    if exists(target_file):
        pass
    elif exists(target_file+'.gz'):
        target_file += '.gz'
    elif exists(target_file+'.bz2'):
        target_file += '.bz2'
    else:
        raise ConfigurationError("Did not find target file for %s at '%s'"%
                                 (dataset_name, target_file))
    result['target'] = np.loadtxt(target_file, delimiter=',')
    if result['data'].shape[0]!=result['target'].shape[0]:
        raise ConfigurationError("Data matrix at '%s' has %d rows, but target at '%s' has %d rows"%
                                 (data_file, result['data'].shape[0],
                                  target_file, result['target'].shape[0]))
    result['resource'] = ResourceRef(resource_name, subpath)
    # check for and load any other attributes
    for fname in os.listdir(dataset_path):
        if fname.endswith('.txt'):
            result[fname[:-4]] = _load_dataset_file(dataset_path, fname)
        elif fname.endswith('.rst'):
            result[fname[:-4]] = _load_dataset_file(dataset_path, fname)
        elif fname.endswith('.csv'):
            result[fname[:-4]] = _load_dataset_file(dataset_path, fname)
        elif fname.endswith('.csv.gz'):
            result[fname[:-7]] = _load_dataset_file(dataset_path, fname)
        elif fname.endswith('.csv.bz2'):
            result[fname[:-8]] = _load_dataset_file(dataset_path, fname)
        elif fname.endswith('.npy'):
            result[fname[:-4]] = _load_dataset_file(dataset_path, fname)
    return Bunch(**result)


class Metrics:
    """Metrics and its subclasses are convenience classes
    for sklearn metrics. The subclasses
    of Matrics are used by :func:`~train_and_predict_with_cv`
    in printing a metrics report and generating the metrics
    json file.
    """
    def __init__(self, expected, predicted):
        self.expected = expected
        self.predicted = predicted

    def to_dict(self):
        pass
    def print_metrics(self, file=sys.stdout):
        pass


class BinaryClassificationMetrics(Metrics):
    """Given an array of expected (target) values
    and the actual predicted values from a classifier,
    compute metrics that make sense for a binary
    classifier, including accuracy, precision, recall, roc auc,
    and f1 score.
    """
    def __init__(self, expected, predicted):
        super().__init__(expected, predicted)
        self.accuracy = metrics.accuracy_score(expected, predicted)
        self.precision = metrics.precision_score(expected, predicted)
        self.recall = metrics.recall_score(expected, predicted)
        self.roc_auc = metrics.roc_auc_score(expected, predicted)
        self.f1_score = metrics.f1_score(expected, predicted)

    def to_dict(self):
        return {
            'accuracy':self.accuracy,
            'precision':self.precision,
            'recall':self.recall,
            'roc_auc_score':self.roc_auc,
            'f1_score':self.f1_score
        }

    def print_metrics(self, file=sys.stdout):
        for k, v in self.to_dict():
            print("%13s: %.02f" % (k, v), file=file)


class MulticlassClassificationMetrics(Metrics):
    """Given an array of expected (target) values
    and the actual predicted values from a classifier,
    compute metrics that make sense for a multi-class
    classifier, including accuracy and sklearn's
    "classification report" showing per-class metrics.
    """
    def __init__(self, expected, predicted):
        super().__init__(expected, predicted)
        self.accuracy = metrics.accuracy_score(expected, predicted)
        self.classification_report = \
            metrics.classification_report(expected, predicted,
                                          output_dict=True)

    def to_dict(self):
        return {
            'accuracy':self.accuracy,
            'classification_report':self.classification_report
        }

    def print_metrics(self, file=sys.stdout):
        print("accuracy: %.02f" % self.accuracy, file=file)
        print("classification report:", file=file)
        print(metrics.classification_report(self.expected, self.predicted))


def train_and_predict_with_cv(classifier_class:ClassifierMixin,
                              param_grid:Union[Dict[str,List[Any]],
                                               List[Dict[str,List[Any]]]],
                              dataset:Bunch,
                              results_dir:str,
                              test_size:float=0.2, folds:int=5,
                              cv_scoring:str='accuracy',
                              model_name:Optional[str]=None,
                              random_state:Optional[int]=None,
                              run_description:Optional[str]=None) -> None:
    """This function implements a common workflow for sklearn classifiers:

    1. Splits the data into training set and a final validation set.
    2. Runs a grid search cross validation to find the best combination
       of hyperparameters for the classifier on the training data.
    3. Trains the model on the training data using the best hyperparameter
       values.
    4. Predicts the classes of the validation test data set and computes
       common metrics comparing the training and testing data.
    5. Writes the metrics to a results file at RESULTS_DIR/results.json.
    6. If the ``model_name`` parameter was specified, retrain the classifier
       on all the data and save the (pickled) model to the results directory.
    7. Write out the lineage data for this experiment.

    **Parameters**

    classifier_class
        An sklearn classifier or a class implementing the same interface
    param_grid
        As described in the documentation for
        `GridSearchCV <https://scikit-learn.org/stable/modules/generated/sklearn.model_selection.GridSearchCV.html#sklearn.model_selection.GridSearchCV>`_,
        a dictionary with parameters names as keys and lists of parameter settings to
        try as values, or a list of such dictionaries. The various combinations
        of these parameters will be searched to find the best classifiation results
        on the training data.
    dataset
        A sklean Bunch object with members for data, target, and resource. This can
        be loaded by calling :func:`~load_dataset_from_resource`
    results_dir
        The directory on the local filesystem to which the results should be
        written.
    test_size
        The fraction of the input data samples to be held back for the final
        validation. Defaults to 0.2 (20%).
    folds
        Number of cross validation folds. Defaults to 5.
    cv_scoring
        Name of scoring algorithm to use in evaluating the hyperparameter
        combinations in cross validation. Defaults to 'accuracy'. See
        `here <https://scikit-learn.org/stable/modules/model_evaluation.html#scoring-parameter>`_
        for details.
    model_name
        If specified, retrain the model using the entire data set (train + test)
        and the best parameters found during cross validation. Pickle and save
        this model to the file RESULTS_DIR/MODEL_NAME.pkl. If the model name is
        not specified, skip this step.
    random_state
        Optional integer to be used as a random seed.
    run_description
        Optional text describing this particular run. This is saved in the results
        file and the lineage.

    **Example**

    Here is an example (taken from the :ref:`Quick Start <quickstart>`)::

        import numpy as np
        from os.path import join
        from sklearn.svm import SVC
        from dataworkspaces.kits.scikit_learn import load_dataset_from_resource,\
                                                     train_and_predict_with_cv
        
        RESULTS_DIR='../results'
        
        dataset = load_dataset_from_resource('sklearn-digits-dataset')
        train_and_predict_with_cv(SVC, {'gamma':[0.01, 0.001, 0.0001]}, dataset,
                                  RESULTS_DIR, random_state=42)

    This trains a Support Vector Classifier with three different values of gamma
    (0.01, 0.001, and 0.0001) and then evaluates the trained classifier on the
    holdout data. The results are writen to ``../results/results.json``.
    """
    X_train, X_test, y_train, y_test = \
        train_test_split(dataset.data, dataset.target, test_size=test_size,
                         random_state=random_state)
    # find the best combination of hyperparameters
    search = GridSearchCV(classifier_class(), param_grid=param_grid, scoring=cv_scoring,
                          cv=folds)
    search.fit(X_train, y_train)
    best_params = search.best_params_
    print("Best params were: %s" % repr(best_params))

    lineage_params = {
        'classifier':classifier_class.__name__,
        'test_size':test_size,
        'cv_params': param_grid,
        'cv_scoring':cv_scoring,
        'random_state':random_state
    }
    for (k, v) in best_params.items():
        lineage_params[k] = v

    lb = LineageBuilder().with_parameters(lineage_params)\
                         .as_results_step(results_dir, run_description)\
                         .with_input_ref(dataset.resource)
    lb = lb.with_step_name(get_step_name_for_notebook())\
           .with_code_path(get_notebook_directory()) \
         if is_notebook() \
         else lb.as_script_step()

    with lb.eval() as lineage:
        # Instantiate a classifier with the best parameters and train
        classifier = classifier_class(**best_params)
        classifier.fit(X_train, y_train)

        # Now predict the value of the digit on the test set
        predicted = classifier.predict(X_test)
        m = MulticlassClassificationMetrics(y_test, predicted) \
            if len(np.unique(dataset.target))>2 \
            else BinaryClassificationMetrics(y_test, predicted) # type: Metrics
        m.print_metrics()
        lineage.write_results(m.to_dict())

        if model_name is not None:
            classifier = classifier_class(**best_params)
            classifier.fit(dataset.data, dataset.target)
            model_file = join(abspath(expanduser(results_dir)), model_name+'.pkl')
            joblib.dump(classifier, model_file)
            print("Wrote trained model to %s"% model_file)
