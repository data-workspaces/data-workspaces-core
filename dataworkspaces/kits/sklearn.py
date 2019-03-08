"""
Integration with scikit-learn
"""

from typing import Type
from sklearn import metrics, model_selection
from sklearn.model_selection import GridSearchCV, train_test_split
import sys
import numpy as np

from dataworkspaces.lineage import LineageBuilder
from .jupyter import is_notebook, get_step_name_for_notebook


class Metrics:
    """Base class for metrics.
    """
    def __init__(self, expected, predicted):
        self.expected = expected
        self.predicted = predicted

    def to_dict(self):
        pass
    def print_metrics(self, file=sys.stdout):
        pass


class BinaryClassificationMetrics(Metrics):
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


def train_and_predict_with_cv(classifier_class, param_grid, data, target,
                              input_dir, results_dir,
                              test_size=0.2, folds=5,
                              cv_scoring='accuracy',
                              random_state=None,
                              run_description=None):
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
    lb = lb.with_step_name(get_step_name_for_notebook()) if is_notebook()\
         else lb.as_script_step()

    with lb.eval() as lineage:
        # Instantiate a classifier with the best parameters and train
        classifier = classifier_class(**best_params)
        classifier.fit(X_train, y_train)

        # Now predict the value of the digit on the test set
        predicted = classifier.predict(X_test)
        if len(np.unique(target))>2:
            m = MulticlassClassificationMetrics(y_test, predicted)
        else:
            m = BinaryClassificationMetrics(y_test, predicted)
        m.print_metrics()
        lineage.write_results(m.to_dict())

