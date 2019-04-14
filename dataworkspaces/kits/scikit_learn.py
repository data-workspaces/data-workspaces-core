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
import sys
import numpy as np
from os.path import join, abspath, expanduser

from dataworkspaces.lineage import LineageBuilder
from .jupyter import is_notebook, get_step_name_for_notebook, get_notebook_directory


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
                              data:np.ndarray, target:np.ndarray,
                              input_dir:str, results_dir:str,
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
    data
        A 2-d NumPy array where each column is a feature and each row is a
        collection of features comprising a sample.
    target
        A 1-d NumPy array where each value represents the class number of the
        corresponding sample row in the data array.
    input_dir
        The directory on the local filesystem from which the data and target arrays
        were read. This is used for lineage.
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
        from dataworkspaces.kits.scikit_learn import train_and_predict_with_cv
        
        DATA_DIR='../sklearn-digits-dataset'
        RESULTS_DIR='../results'
        
        data = np.loadtxt(join(DATA_DIR, 'data.csv'), delimiter=',')
        target = np.loadtxt(join(DATA_DIR, 'target.csv'), delimiter=',')
        train_and_predict_with_cv(SVC, {'gamma':[0.01, 0.001, 0.0001]}, data, target,
                                  DATA_DIR, RESULTS_DIR, random_state=42)

    This trains a Support Vector Classifier with three different values of gamma
    (0.01, 0.001, and 0.0001) and then evaluates the trained classifier on the
    holdout data. The results are writen to ``../results/results.json``.
    """
    X_train, X_test, y_train, y_test = \
        train_test_split(data, target, test_size=test_size,
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
                         .with_input_path(input_dir)
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
            if len(np.unique(target))>2 \
            else BinaryClassificationMetrics(y_test, predicted) # type: Metrics
        m.print_metrics()
        lineage.write_results(m.to_dict())

        if model_name is not None:
            classifier = classifier_class(**best_params)
            classifier.fit(data, target)
            model_file = join(abspath(expanduser(results_dir)), model_name+'.pkl')
            joblib.dump(classifier, model_file)
            print("Wrote trained model to %s"% model_file)
