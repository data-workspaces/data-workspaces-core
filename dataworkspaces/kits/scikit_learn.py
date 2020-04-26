"""
This module (``dataworkspaces.kits.scikit_learn``)
provides integration with the `scikit-learn <https://scikit-learn.org>`_
framework. The main class provided here is :class:`~LineagePredictor`,
which wraps any class following sklearn's predictor protocol. It captures
inputs, model parameters and results. This module also provides
:class:`~Metrics` and its subclasses, which
support the computation of common metrics and the writing of them
to a results file. Finally, there is
:func:`~train_and_predict_with_cv`, which
runs a common sklearn classification workflow, including grid search.
"""

from typing import Optional, Union, Dict, List, Any, cast
from abc import ABCMeta, abstractmethod
from sklearn.base import ClassifierMixin
from sklearn import metrics
from sklearn.model_selection import GridSearchCV, train_test_split
from sklearn.utils import Bunch
import sys
import numpy as np
import os
from os.path import join, abspath, expanduser, exists, isabs
from tempfile import NamedTemporaryFile


from dataworkspaces.errors import ConfigurationError
from dataworkspaces.lineage import LineageBuilder
from dataworkspaces.workspace import (
    find_and_load_workspace,
    LocalStateResourceMixin,
    FileResourceMixin,
)
from dataworkspaces.utils.lineage_utils import ResourceRef
from dataworkspaces.kits.wrapper_utils import _DwsModelState, _add_to_hash

from .jupyter import is_notebook, get_step_name_for_notebook, get_notebook_directory

try:
    import joblib
except ImportError as e:
    raise ConfigurationError('Please install the joblib package (via "pip install joblib")') from e


def _load_dataset_file(dataset_path, filename):
    filepath = join(dataset_path, filename)
    if filename.endswith(".txt") or filename.endswith(".rst"):
        with open(filepath, "r") as f:
            return f.read()
    elif filename.endswith(".csv") or filename.endswith(".csv.gz") or filename.endswith(".csv.bz2"):
        try:
            return np.loadtxt(filepath, delimiter=",")
        except ValueError:
            # try with pandas
            import pandas

            df = pandas.read_csv(filepath)
            if len(df.values.shape) == 2 and df.values.shape[1] == 1:  # this is just a list
                return df.values.reshape(df.values.shape[0])
            else:
                return df.values
    elif filename.endswith(".npy"):
        return np.load(filepath)


def load_dataset_from_resource(
    resource_name: str, subpath: Optional[str] = None, workspace_dir: Optional[str] = None
) -> Bunch:
    """
    Load a datset (data and targets) from the specified resource, and returns an
    sklearn-style Bunch (a dictionary-like object). The bunch will include at least
    three attributes:

    * ``data`` - a NumPy array of shape number_samples * number_features
    * ``target`` - a NumPy array of length number_samples
    * ``resource`` - a :class:`~dataworkspaces.workspace.ResourceRef` that provides the resource name and
      subpath (if any) for the data

    Some other attributes that may also be present, depending on the data set:

    * ``DESCR`` - text containing a full description of the data set (for humans)
    * ``feature_names`` - an array of length number_features containing the name
      of each feature.
    * ``target_names`` - an array containing the name of each target class

    Data sets may define their own attributes as well (see below).

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

    workspace = find_and_load_workspace(True, False, workspace_dir)
    workspace.validate_resource_name(resource_name, subpath)
    dataset_name = (
        "Resource " + resource_name + " subpath " + subpath
        if subpath is not None
        else "Resource " + resource_name
    )
    r = workspace.get_resource(resource_name)
    if not isinstance(r, LocalStateResourceMixin) or (r.get_local_path_if_any() is None):
        # TODO: Support a data access api
        raise ConfigurationError(
            "Unable to instantiate a data set for resource '%s': currently not supported for non-local resources"
            % resource_name
        )
    local_path = r.get_local_path_if_any()
    assert local_path is not None
    dataset_path = join(local_path, subpath) if subpath is not None else local_path
    result = {}  # this will be the args to the result Bunch
    # First load data and target files, which are required
    data_file = join(dataset_path, "data.csv")
    if exists(data_file):
        pass
    elif exists(data_file + ".gz"):
        data_file += ".gz"
    elif exists(data_file + ".bz2"):
        data_file += ".bz2"
    else:
        raise ConfigurationError(
            "Did not find data file for %s at '%s'" % (dataset_name, data_file)
        )
    result["data"] = np.loadtxt(data_file, delimiter=",")
    target_file = join(dataset_path, "target.csv")
    if exists(target_file):
        pass
    elif exists(target_file + ".gz"):
        target_file += ".gz"
    elif exists(target_file + ".bz2"):
        target_file += ".bz2"
    else:
        raise ConfigurationError(
            "Did not find target file for %s at '%s'" % (dataset_name, target_file)
        )
    result["target"] = np.loadtxt(target_file, delimiter=",")
    if result["data"].shape[0] != result["target"].shape[0]:
        raise ConfigurationError(
            "Data matrix at '%s' has %d rows, but target at '%s' has %d rows"
            % (data_file, result["data"].shape[0], target_file, result["target"].shape[0])
        )
    result["resource"] = ResourceRef(resource_name, subpath)
    # check for and load any other attributes
    for fname in os.listdir(dataset_path):
        if fname.endswith(".txt"):
            result[fname[:-4]] = _load_dataset_file(dataset_path, fname)
        elif fname.endswith(".rst"):
            result[fname[:-4]] = _load_dataset_file(dataset_path, fname)
        elif fname.endswith(".csv"):
            result[fname[:-4]] = _load_dataset_file(dataset_path, fname)
        elif fname.endswith(".csv.gz"):
            result[fname[:-7]] = _load_dataset_file(dataset_path, fname)
        elif fname.endswith(".csv.bz2"):
            result[fname[:-8]] = _load_dataset_file(dataset_path, fname)
        elif fname.endswith(".npy"):
            result[fname[:-4]] = _load_dataset_file(dataset_path, fname)
    return Bunch(**result)


class Metrics(metaclass=ABCMeta):
    """Metrics and its subclasses are convenience classes
    for sklearn metrics. The subclasses
    of Matrics are used by :func:`~train_and_predict_with_cv`
    in printing a metrics report and generating the metrics
    json file.
    """

    def __init__(self, expected, predicted, sample_weight=None):
        self.expected = expected
        self.predicted = predicted
        self.sample_weight = sample_weight

    @abstractmethod
    def to_dict(self) -> Dict[str, Any]:
        pass

    @abstractmethod
    def score(self) -> float:
        """Given the expected and predicted values, compute the metric
        for this type of predictor, as needed for the predictor's score()
        method. This is used in the wrapped classes to avoid multiple
        calls to predict()."""
        pass

    @abstractmethod
    def print_metrics(self, file=sys.stdout) -> None:
        """Print the metrics to a file
        """
        pass


class BinaryClassificationMetrics(Metrics):
    """Given an array of expected (target) values
    and the actual predicted values from a classifier,
    compute metrics that make sense for a binary
    classifier, including accuracy, precision, recall, roc auc,
    and f1 score.
    """

    def __init__(self, expected, predicted, sample_weight=None):
        super().__init__(expected, predicted, sample_weight)
        self.accuracy = metrics.accuracy_score(expected, predicted, sample_weight=sample_weight)
        self.precision = metrics.precision_score(expected, predicted, sample_weight=sample_weight)
        self.recall = metrics.recall_score(expected, predicted, sample_weight=sample_weight)
        self.roc_auc = metrics.roc_auc_score(expected, predicted, sample_weight=sample_weight)
        self.f1_score = metrics.f1_score(expected, predicted, sample_weight=sample_weight)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "accuracy": self.accuracy,
            "precision": self.precision,
            "recall": self.recall,
            "roc_auc_score": self.roc_auc,
            "f1_score": self.f1_score,
        }

    def score(self) -> float:
        """Metric for binary classification is accuracy
        """
        return self.accuracy

    def print_metrics(self, file=sys.stdout) -> None:
        for k, v in self.to_dict().items():
            print("%13s: %.02f" % (k, cast(Union[int, float], v)), file=file)


class MulticlassClassificationMetrics(Metrics):
    """Given an array of expected (target) values
    and the actual predicted values from a classifier,
    compute metrics that make sense for a multi-class
    classifier, including accuracy and sklearn's
    "classification report" showing per-class metrics.
    """

    def __init__(self, expected, predicted, sample_weight=None):
        super().__init__(expected, predicted, sample_weight)
        self.accuracy = metrics.accuracy_score(expected, predicted, sample_weight=sample_weight)
        self.classification_report = metrics.classification_report(
            expected, predicted, sample_weight=sample_weight, output_dict=True
        )

    def score(self) -> float:
        """Metric for multiclass classification is accuracy
        """
        return self.accuracy

    def to_dict(self):
        return {"accuracy": self.accuracy, "classification_report": self.classification_report}

    def print_metrics(self, file=sys.stdout):
        print("accuracy: %.02f" % self.accuracy, file=file)
        print("classification report:", file=file)
        print(metrics.classification_report(self.expected, self.predicted))


class RegressionMetrics(Metrics):
    """For regression, we capture the r-squared score and
    the mean squared error.
    """

    def __init__(self, expected, predicted, sample_weight=None):
        super().__init__(expected, predicted, sample_weight)
        self.r2_score = metrics.r2_score(self.expected, self.predicted, sample_weight=sample_weight)
        self.mean_squared_error = metrics.mean_squared_error(
            self.expected, self.predicted, sample_weight=sample_weight
        )

    def score(self) -> float:
        """Metric for regression is r2_score
        """
        return self.r2_score

    def to_dict(self):
        return {"r2_score": self.r2_score, "mean_squared_error": self.mean_squared_error}

    def print_metrics(self, file=sys.stdout):
        print("r2_score: %.02f" % self.r2_score, file=file)
        print("mean_squared_error: %.02f" % self.mean_squared_error, file=file)


_METRICS = {
    "binary_classification": BinaryClassificationMetrics,
    "multiclass_classification": MulticlassClassificationMetrics,
    "regression": RegressionMetrics,
}

import sklearn.utils.metaestimators


class LineagePredictor(sklearn.utils.metaestimators._BaseComposition):
    """This is a wrapper for adding lineage to any predictor in sklearn.
    To use it, instantiate the predictor (for classification or regression)
    and then create a new instance of :class:`~LineagePredictor`.

    The initializer finds the associated workspace and initializes a
    :class:`~dataworkspaces.lineage.Lineage` instance. The input_resource
    is recorded in this lineage. Other methods call the underlying wrapped
    predictor's methods, with additional functionality as needed (see below).

    **Parameters**

    predictor
        Any sklearn predictor instance. It must have ``fit`` and ``predict``
        methods.

    metrics
        Either a string naming a metrics type or a subclass of :class:`~Metrics`.
        If a string, it should be one of: binary_classification,
        multiclass_classification, or regression.

    input_resource
        Resource providing the input data to this model. May be
        specified by name, by a local file path, or via a
        :class:`~dataworkspaces.workspace.ResourceRef`.

    resource_resource
        (optional) Resource where the results are to be stored.
        May be specified by name, by a local file path, or via a
        :class:`!ResourceRef`.
        If not specified, will try to infer from the workspace.

    model_save_file
        (optional) Name of file to store a (joblib-formmatted)
        serialization of the trained model upon completion of the ``fit()``
        method. This should be a relative path, as it is stored under
        the results resource. If model_save_file is not specified,
        no model is saved.

    workspace_dir
        (optional) Directory specifying the workspace. Usually can be
        inferred from the current directory.

    verbose
        If True, print a lot of detailed information about the execution
        of Data Workspaces.

    **Example**

    Here is an example useage of the wrapper, taken from the
    :ref:`Quick Start <quickstart>`::

      from sklearn.svm import SVC
      from sklearn.model_selection import train_test_split
      from dataworkspaces.kits.scikit_learn import load_dataset_from_resource
      from dataworkspaces.kits.scikit_learn import LineagePredictor

      dataset = load_dataset_from_resource('sklearn-digits-dataset')
      X_train, X_test, y_train, y_test = train_test_split(
          dataset.data, dataset.target, test_size=0.5, shuffle=False)
      classifier = LineagePredictor(SVC(gamma=0.001),
                                    metrics='multiclass_classification',
                                    input_resource=dataset.resource,
                                    model_save_file='digits.joblib')

      classifier.fit(X_train, y_train)
      score = classifier.score(X_test, y_test)

    **Methods**

    """

    _dws_model_wrap = True

    def __init__(
        self,
        predictor,
        metrics: Union[str, type],
        input_resource: Union[str, ResourceRef],
        results_resource: Optional[Union[str, ResourceRef]] = None,
        model_save_file: Optional[str] = None,
        workspace_dir: Optional[str] = None,
        verbose: bool = False,
    ):
        if hasattr(predictor, "_dws_model_wrap") and predictor._dws_model_wrap is True:  # type: ignore
            print("dws>> %s is already wrapped" % repr(predictor))
            return predictor  # already wrapped
        self.predictor = predictor
        assert metrics in _METRICS.keys() or (
            isinstance(metrics, type) and issubclass(metrics, Metrics)
        ), (
            "%s is not a subclass of Metrics and not one of %s"
            % (repr(metrics), ", ".join([repr(s) for s in _METRICS.keys()]))
        )
        self.metrics = metrics
        self.input_resource = input_resource
        self.results_resource = results_resource
        self.model_save_file = model_save_file
        if model_save_file is not None:
            assert not isabs(model_save_file), "Model save file should not be an absolute path"
        self.workspace_dir = workspace_dir
        self.metrics = metrics
        self.verbose = verbose
        self.score_has_been_run = False
        self._init_dws_state()

    def _init_dws_state(self):
        workspace = find_and_load_workspace(
            batch=True, verbose=self.verbose, uri_or_local_path=self.workspace_dir
        )
        self._dws_state = _DwsModelState(workspace, self.input_resource, self.results_resource)

    def _save_model(self):
        assert self.model_save_file
        if not self.model_save_file.endswith(".joblib"):
            model_save_file = self.model_save_file + ".joblib"
        else:
            model_save_file = self.model_save_file
        tempname = None
        try:
            with NamedTemporaryFile(delete=False, suffix=".joblib") as f:
                tempname = f.name
            joblib.dump(self, tempname)
            resource = self._dws_state.workspace.get_resource(self._dws_state.results_ref.name)
            if self._dws_state.results_ref.subpath is not None:
                target_name = join(self._dws_state.results_ref.subpath, model_save_file)
            else:
                target_name = model_save_file
            cast(FileResourceMixin, resource).upload_file(tempname, target_name)
        finally:
            if (tempname is not None) and exists(tempname):
                os.remove(tempname)
            if self.verbose:
                print("dws> saved model file to %s:%s" % (resource.name, target_name))

    def __getstate__(self):
        state = super().__getstate__()
        if "_dws_state" in state:
            del state["_dws_state"]
        return state

    def __setstate__(self, state):
        super().__setstate__(state)
        self._init_dws_state()

    def set_params(self, **params):
        """"""
        super().set_params(**params)
        self._init_dws_state()
        return self

    def fit(self, X, y, *args, **kwargs):
        """The underlying fit() method of a predictor trains the predictio based
        on the input data (X) and labels (y).

        If the input resource is an api resource, the wrapper captures the hash of
        the inputs.
        If ``model_save_file`` was specified, it also saves the trained model."""
        api_resource = self._dws_state.find_input_resources_and_return_if_api(X, y)
        if api_resource is not None:
            api_resource.init_hash_state()
            hash_state = api_resource.get_hash_state()
            _add_to_hash(X, hash_state)
            _add_to_hash(y, hash_state)
            api_resource.save_current_hash()  # in case we evaluate in a separate process
        result = self.predictor.fit(X, y, *args, **kwargs)
        if self.model_save_file is not None:
            self._save_model()
        return result

    def score(self, X, y, sample_weight=None):
        """This method make predictions from a trained model and scores them
        according to the metrics specified when instantiated the wrapper.

        If the input resource is an api resource, the wrapper captures its hash.
        The wapper runs the wrapped predictor's :meth:`~predict` method to
        generate predictions. A `metrics` object is instantiated to compute the metrics
        for the predictions and a ``results.json`` file is written to the
        results resource. The lineage data is saved and finally the score
        is computed from the predictions and returned to the caller."""
        if self.score_has_been_run:
            # This might be from a saved model, so we reset the
            # execution time, etc.
            self._dws_state.reset_lineage()
        for (param, value) in self.predictor.get_params(deep=True).items():
            self._dws_state.lineage.add_param(param, value)
        api_resource = self._dws_state.find_input_resources_and_return_if_api(X, y)
        if api_resource is not None:
            api_resource.dup_hash_state()
            hash_state = api_resource.get_hash_state()
            _add_to_hash(X, hash_state)
            if y is not None:
                _add_to_hash(y, hash_state)
            api_resource.save_current_hash()
            api_resource.pop_hash_state()
        predictions = self.predictor.predict(X)
        if isinstance(self.metrics, str):
            metrics_inst = _METRICS[self.metrics](y, predictions, sample_weight=sample_weight)  # type: ignore
        else:
            metrics_inst = self.metrics(y, predictions, sample_weight=sample_weight)
        self._dws_state.write_metrics_and_complete(metrics_inst.to_dict())
        self.score_has_been_run = True
        return metrics_inst.score()

    def predict(self, X):
        """The underlying :meth:`~predict` method is called directly,
        without affecting the lineage."""
        return self.predictor.predict(X)


def train_and_predict_with_cv(
    classifier_class: ClassifierMixin,
    param_grid: Union[Dict[str, List[Any]], List[Dict[str, List[Any]]]],
    dataset: Bunch,
    results_dir: str,
    test_size: float = 0.2,
    folds: int = 5,
    cv_scoring: str = "accuracy",
    model_name: Optional[str] = None,
    random_state: Optional[int] = None,
    run_description: Optional[str] = None,
) -> None:
    """NOTE: This function is under consideration for DEPRECATION and
    may be removed from a future version.

    This function implements a common workflow for sklearn classifiers:

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
    X_train, X_test, y_train, y_test = train_test_split(
        dataset.data, dataset.target, test_size=test_size, random_state=random_state
    )
    # find the best combination of hyperparameters
    search = GridSearchCV(classifier_class(), param_grid=param_grid, scoring=cv_scoring, cv=folds)
    search.fit(X_train, y_train)
    best_params = search.best_params_
    print("Best params were: %s" % repr(best_params))

    lineage_params = {
        "classifier": classifier_class.__name__,
        "test_size": test_size,
        "cv_params": param_grid,
        "cv_scoring": cv_scoring,
        "random_state": random_state,
    }
    for (k, v) in best_params.items():
        lineage_params[k] = v

    lb = (
        LineageBuilder()
        .with_parameters(lineage_params)
        .as_results_step(results_dir, run_description)
        .with_input_ref(dataset.resource)
    )
    if is_notebook():
        lb = lb.with_code_path(get_notebook_directory())
        step_name = get_step_name_for_notebook()
        if step_name is not None:
            lb = lb.with_step_name(step_name)  # not always able to determine this
    else:
        lb = lb.as_script_step()

    with lb.eval() as lineage:
        # Instantiate a classifier with the best parameters and train
        classifier = classifier_class(**best_params)
        classifier.fit(X_train, y_train)

        # Now predict the value of the digit on the test set
        predicted = classifier.predict(X_test)
        m = (
            MulticlassClassificationMetrics(y_test, predicted)
            if len(np.unique(dataset.target)) > 2
            else BinaryClassificationMetrics(y_test, predicted)
        )  # type: Metrics
        m.print_metrics()
        lineage.write_results(m.to_dict())

        if model_name is not None:
            classifier = classifier_class(**best_params)
            classifier.fit(dataset.data, dataset.target)
            model_file = join(abspath(expanduser(results_dir)), model_name + ".pkl")
            joblib.dump(classifier, model_file)
            print("Wrote trained model to %s" % model_file)
