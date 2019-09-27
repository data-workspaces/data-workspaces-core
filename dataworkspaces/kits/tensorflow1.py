"""Integration with Tensorflow 1.x

This is an experimental API and subject to change.

**Wrapping a Karas Model**

Below is an example of wrapping one of the standard tf.keras model classes,
based on https://www.tensorflow.org/tutorials/keras/basic_classification.
Assume we have a workspace already set up, with two resources: a *Source Data*
resource of  type `api-resource`, which is used to capture the hash of
input data as it is passed to the model, and a *Results* resource to
keep the metrics. The only change we need to do to capture the lineage from
the model is to wrap the model's class, using
:func:`~add_lineage_to-keras_model_class`.

Here is the code::
    
    # TensorFlow and tf.keras
    import tensorflow as tf
    from tensorflow import keras
    
    from dataworkspaces.kits.tensorflow1 import add_lineage_to_keras_model_class
    
    # Wrap our model class. This is the only DWS-specific change needed.
    keras.Sequential = add_lineage_to_keras_model_class(keras.Sequential)
    
    fashion_mnist = keras.datasets.fashion_mnist
    
    (train_images, train_labels), (test_images, test_labels) = fashion_mnist.load_data()
    
    model = keras.Sequential([
        keras.layers.Flatten(input_shape=(28, 28)),
        keras.layers.Dense(128, activation=tf.nn.relu),
        keras.layers.Dense(10, activation=tf.nn.softmax)
    ])
    
    model.compile(optimizer='adam',
                  loss='sparse_categorical_crossentropy',
                  metrics=['accuracy'])
    
    model.fit(train_images, train_labels, epochs=5)
    
    test_loss, test_acc = model.evaluate(test_images, test_labels)
    print('Test accuracy:', test_acc)

This will create a ``results.json`` file in the results resource. It will
look like this::

    {
      "step": "test",
      "start_time": "2019-09-26T11:33:22.100584",
      "execution_time_seconds": 26.991521,
      "parameters": {
        "optimizer": "adam",
        "loss_function": "sparse_categorical_crossentropy",
        "epochs": 5,
        "fit_batch_size": null,
        "evaluate_batch_size": null
      },
      "run_description": null,
      "metrics": {
        "loss": 0.3657455060243607,
        "acc": 0.8727999925613403
      }
    }

If you subclass from a Keras Model class, you can just use
:func:`~add_lineage_to-keras_model_class` as a decorator. Here is an example::
    
    @add_lineage_to_keras_model_class
    class MyModel(keras.Model):
      def __init__(self):
        print("In MyModel init")
        #super(MyModel, self).__init__()
        super().__init__()
        self.dense1 = tf.keras.layers.Dense(4, activation=tf.nn.relu)
        self.dense2 = tf.keras.layers.Dense(5, activation=tf.nn.softmax)
    
      def call(self, inputs):
        print("Inputs: %s" % repr(inputs))
        x1 = self.dense1(inputs)
        return self.dense2(x1)
    
    model = MyModel()
    
    import numpy as np
    
    model.compile(optimizer='adam',
                  loss='sparse_categorical_crossentropy',
                  metrics=['accuracy'])
    model.fit(np.zeros(20).reshape((5,4)), np.ones(5), epochs=5)
    test_loss, test_acc = model.evaluate(np.zeros(16).reshape(4,4), np.ones(4))
    
    print('Test accuracy:', test_acc)

"""
import hashlib
from typing import Optional, Union
import numpy as np
import datetime

import tensorflow.keras.optimizers as optimizers
import tensorflow.losses as losses

from dataworkspaces.workspace import find_and_load_workspace, ResourceRef, \
                                     ResourceRoles, JSONDict, Workspace
assert JSONDict # make pyflakes happy
from dataworkspaces.lineage import ResultsLineage
from dataworkspaces.utils.lineage_utils import LineageError, infer_step_name
from dataworkspaces.resources.api_resource import API_RESOURCE_TYPE, ApiResource
assert ApiResource # make pyflakes happy
from dataworkspaces.kits.jupyter import get_step_name_for_notebook

#from dataworkspaces.utils.patch_utils import patch_method
#def add_lineage_to_model_class(Cls):
#    if hasattr(Cls, '_dws_model_wrap') and Cls._dws_model_wrap is True:
#        print("%s is already wrapped" % Cls.__name__)
#        return Cls # already wrapped
#def make_model__init__(original_method):
#    def init(self, **kwargs):
#        self.workspace = find_and_load_workspace(batch=True, verbose=False)
#        self.hash_state = hashlib.sha1()
#        original_method(self, **kwargs)
#    return init
#patch_method(keras.Model, '__init__', make_model__init__)
#
#def make_model_fit(original_method):
#    def fit(self, x, y, **kwargs):
#        self.hash_state.update(x.data.tobytes())
#        self.hash_state.update(y.data.tobytes())
#        print("captured hash of training data: %s" % self.hash_state.hexdigest())
#        return original_method(self, x, y, **kwargs)
#    return fit
#patch_method(keras.Model, 'fit', make_model_fit)
#
#def make_model_evaluate(original_method):
#    def evaluate(self, x, y, **kwargs):
#        h = self.hash_state.copy()
#        h.update(x.data.tobytes())
#        h.update(y.data.tobytes())
#        print("hash of input data is %s" % h.hexdigest())
#        results = original_method(self, x, y, **kwargs)
#        assert len(results)==len(self.metrics_names)
#        metrics = {n:v for (n, v) in zip(self.metrics_names, results)}
#        print("Metrics: %s" % metrics)
#        return results
#    return evaluate
#patch_method(keras.Model, 'evaluate', make_model_evaluate)

def _find_resource(workspace:Workspace, role:str,
                   name_or_ref:Optional[Union[str, ResourceRef]]=None) -> ResourceRef:
    if isinstance(name_or_ref, str):
        return workspace.map_local_path_to_resource(name_or_ref, expecting_a_code_resource=False)
    elif isinstance(name_or_ref, ResourceRef):
        workspace.validate_resource_name(name_or_ref.name, name_or_ref.subpath)
        return name_or_ref
    else:
        for rname in workspace.get_resource_names():
            if workspace.get_resource_role(rname)==role:
                return ResourceRef(rname, subpath=None)
        raise LineageError("Could not find a %s resource in your workspace" % role)


def _infer_step_name() -> str:
    """Come up with a step name by looking at whether this is a notebook
    and then the command line arguments.
    """
    # TODO: this should be moved to a utility module (e.g. lineage_utils)
    try:
        notebook_name = get_step_name_for_notebook()
        if notebook_name is not None:
            return notebook_name
    except:
        pass # not a notebook
    return infer_step_name()


def _metric_val_to_json(v):
    if isinstance(v, int) or isinstance(v, str):
        return v
    elif isinstance(v, np.int64) or isinstance(v, np.int32):
        return int(v)
    elif isinstance(v, np.float64) or isinstance(v, np.float32):
        return float(v)
    else:
        return v


def add_lineage_to_keras_model_class(Cls:type,
                                     input_resource:Optional[Union[str, ResourceRef]]=None,
                                     results_resource:Optional[Union[str, ResourceRef]]=None):
    """This function wraps a Keras model class with a subclass that overwrites
    key methods to make calls to the data lineage API.
    """
    if hasattr(Cls, '_dws_model_wrap') and Cls._dws_model_wrap is True: # type: ignore
        print("%s or a superclass is already wrapped" % Cls.__name__)
        return Cls # already wrapped
    workspace = find_and_load_workspace(batch=True, verbose=False)
    results_ref = _find_resource(workspace, ResourceRoles.RESULTS, results_resource)

    class WrappedModel(Cls): # type: ignore
        _dws_model_wrap = True
        def __init__(self,*args,**kwargs):
            super().__init__(*args, **kwargs)
            print("In wrapped init")
            self._dws_workspace = workspace
            self._dws_results_ref = results_ref
            self._dws_input_resource = input_resource
            self._dws_hash_state = hashlib.sha1()
            self._dws_api_resource = None # type: Optional[ApiResource]
            self._dws_params = {} # type: JSONDict
        def compile(self, optimizer,
                    loss=None,
                    metrics=None,
                    loss_weights=None,
                    sample_weight_mode=None,
                    weighted_metrics=None,
                    target_tensors=None,
                    distribute=None,
                    **kwargs):
            if isinstance(optimizer, str):
                self._dws_params['optimizer'] = optimizer
            elif isinstance(optimizer, optimizers.Optimizer):
                self._dws_params['optimizer'] = optimizer.__class__.__name__
            if isinstance(loss, str):
                self._dws_params['loss_function'] = loss
            elif isinstance(loss, losses.Loss):
                self._dws_params['loss_function'] = loss.__class__.__name__
            return super().compile(optimizer, loss, metrics, loss_weights,
                                   sample_weight_mode, weighted_metrics,
                                   target_tensors, distribute, **kwargs)
        def fit(self, x, y, **kwargs):
            print("fit: in wrap of %s" % Cls.__name__)
            if 'epochs' in kwargs:
                self._dws_params['epochs'] = kwargs['epochs']
            else:
                self._dws_params['epochs'] = 1
            if 'batch_size' in kwargs:
                self._dws_params['fit_batch_size'] = kwargs['batch_size']
            else:
                self._dws_params['fit_batch_size'] = None
            if isinstance(x, np.ndarray):
                input_ref = _find_resource(self._dws_workspace, ResourceRoles.SOURCE_DATA_SET,
                                                self._dws_input_resource)
                if self._dws_workspace.get_resource_type(input_ref.name)==API_RESOURCE_TYPE:
                    # capture the hash of the data coming in...
                    self._dws_api_resource = self._dws_workspace.get_resource(input_ref.name)
                    self._dws_hash_state.update(x.data.tobytes())
                    self._dws_hash_state.update(y.data.tobytes())
                    hashval = self._dws_hash_state.hexdigest()
                    self._dws_api_resource.save_current_hash(hashval)
                    print("captured hash of training data: %s" % hashval)
            elif hasattr(x, 'resource'):
                input_ref = x.resource
                if self._dws_workspace.get_resource_type(input_ref.name)==API_RESOURCE_TYPE:
                    assert 0, "Need to implement obtaining of hash from dataset"
            else:
                raise LineageError("No way to determine resource associated with model input. Please specify in model wrapping function or use a wapped data set.")
            self._dws_lineage = ResultsLineage(_infer_step_name(), datetime.datetime.now(),
                                          self._dws_params, [input_ref], [], self._dws_results_ref,
                                          self._dws_workspace)
            return super().fit(x, y, **kwargs)
        def evaluate(self, x, y, **kwargs):
            if 'batch_size' in kwargs:
                self._dws_params['evaluate_batch_size'] = kwargs['batch_size']
            else:
                self._dws_params['evaluate_batch_size'] = None
            if self._dws_api_resource is not None:
                h = self._dws_hash_state.copy()
                h.update(x.data.tobytes())
                h.update(y.data.tobytes())
                hashval = h.hexdigest()
                print("hash of input data is %s" % hashval)
                self._dws_api_resource.save_current_hash(hashval)
            results = super().evaluate(x, y, **kwargs)
            assert len(results)==len(self.metrics_names)
            metrics = {n:_metric_val_to_json(v) for (n, v) in zip(self.metrics_names, results)}
            print("Metrics: %s" % repr(metrics))
            self._dws_lineage.write_results(metrics)
            self._dws_lineage.complete()
            return results
    return WrappedModel
