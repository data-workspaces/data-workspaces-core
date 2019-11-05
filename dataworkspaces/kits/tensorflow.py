"""Integration with Tensorflow 1.x and 2.0

This is an experimental API and subject to change.

**Wrapping a Karas Model**

Below is an example of wrapping one of the standard tf.keras model classes,
based on https://www.tensorflow.org/tutorials/keras/basic_classification.
Assume we have a workspace already set up, with two resources: a *Source Data*
resource of  type `api-resource`, which is used to capture the hash of
input data as it is passed to the model, and a *Results* resource to
keep the metrics. The only change we need to do to capture the lineage from
the model is to wrap the model's class, using
:func:`~add_lineage_to_keras_model_class`.

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


**Subclassing from a Keras Model**

If you subclass from a Keras Model class, you can just use
:func:`~add_lineage_to-keras_model_class` as a decorator. Here is an example::
    
    @add_lineage_to_keras_model_class
    class MyModel(keras.Model):
      def __init__(self):
        # The Tensorflow documentation tends to specify the class name
        # when calling the superclass __init__ function. Don't do this --
        # it breaks if you use class decorators!
        #super(MyModel, self).__init__()
        super().__init__()
        self.dense1 = tf.keras.layers.Dense(4, activation=tf.nn.relu)
        self.dense2 = tf.keras.layers.Dense(5, activation=tf.nn.softmax)
    
      def call(self, inputs):
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


**Supported datatypes for API Resources**

If you are using the *API Resource Type* for your input resource,
the model wrapper will hash the incoming data parameters and include
the hash values in the data lineage. To compute the hashes, Data
Workspaces must access the underlying data representation. The following data
types are currently supported:

* NumPy ``ndarray``
* Pandas ``DataFrame`` and ``Series``
* Tensorflow ``Tensor`` and ``Dataset``, as well as tuples and dictionaries
  containing these types. These types supported if you are either running
  Tensorflow 2.x (graph or eager mode) or 1.x only in eager mode. This restriction is
  due to the inability to access the underlying tensor representation when Tensorflow
  is running in graph mode in version 1.x

If you are using another data representation, or running Tensorflow 1.x in graph
mode, you can always use a resource type that stores the data in files
(e.g. git or local-files) and pass in the input resource name to the
wrapper function.


**API**

"""
from typing import Optional, Union, List
assert List

import tensorflow
if tensorflow.__version__.startswith('2.'): # type: ignore
    USING_TENSORFLOW2=True
else:
    USING_TENSORFLOW2=False
import tensorflow.keras.optimizers as optimizers
if USING_TENSORFLOW2:
    import tensorflow.keras.losses as losses
else:
    import tensorflow.losses as losses

from dataworkspaces.workspace import find_and_load_workspace, ResourceRef
from dataworkspaces.kits.wrapper_utils import _DwsModelState, _add_to_hash,\
                                              NotSupportedError


def _verify_eager_if_dataset(x, y, api_resource):
    """If this is tensorflow 1.x and non-eager mode, there's no way
    to evaluate the dataset outside the tensor graph.
    """
    if (not USING_TENSORFLOW2) and \
       (isinstance(x, tensorflow.data.Dataset) or
        isinstance(y, tensorflow.data.Dataset)) and \
       (not tensorflow.executing_eagerly()):
        raise NotSupportedError("Using an API resource ("+ api_resource.name+
                                ") with non-eager datasets is not "+
                                "supported with TensorFlow 1.x.")


def add_lineage_to_keras_model_class(Cls:type,
                                     input_resource:Optional[Union[str, ResourceRef]]=None,
                                     results_resource:Optional[Union[str, ResourceRef]]=None,
                                     workspace_dir=None,
                                     verbose=False):
    """This function wraps a Keras model class with a subclass that overwrites
    key methods to make calls to the data lineage API.

    **Parameters:**

    * ``Cls`` -- the class being wrapped
    * ``input_resources`` -- optional list of input resources to this model.
      Each resource may be specified by name, by a local file path, or via a
      ``ResourceRef``. If no inputs are specified, will try to infer from the
      workspace.
    * ``results_resource`` -- optional resource where the results are to be stored.
      My be specified by name, by a local file path, or via a ``ResourceRef``.
      if not specified, will try to infer from the workspace.
    * ``workspace-dir`` -- Optional directory specifying the workspace. Usually can be
      inferred from the current directory.
    * ``verbose`` -- If True, print extra debugging information.

    The following methods are wrapped:

    * :func:`~__init__` - loads the workspace and adds dws-specific class members
    * :func:`~compile` - captures the ``optimizer`` and ``loss_function`` parameter values
    * :func:`~fit` - captures the ``epochs`` and ``batch_size`` parameter values;
      if input is an API resource, capture hash values of training data, otherwise capture
      input resource name.
    * :func:`~evaluate` - captures the ``batch_size`` paramerter value; if input is an
      API resource, capture hash values of test data, otherwise capture input resource
      name; capture metrics and write them to results resource.
    """
    if hasattr(Cls, '_dws_model_wrap') and Cls._dws_model_wrap is True: # type: ignore
        print("dws>> %s or a superclass is already wrapped" % Cls.__name__)
        return Cls # already wrapped
    workspace = find_and_load_workspace(batch=True, verbose=verbose,
                                        uri_or_local_path=workspace_dir)

    class WrappedModel(Cls): # type: ignore
        _dws_model_wrap = True
        def __init__(self,*args,**kwargs):
            super().__init__(*args, **kwargs)
            self._dws_state = _DwsModelState(workspace, input_resource, results_resource)
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
                self._dws_state.lineage.add_param('optimizer', optimizer)
            elif isinstance(optimizer, optimizers.Optimizer):
                self._dws_state.lineage.add_param('optimizer', optimizer.__class__.__name__)
            if isinstance(loss, str):
                self._dws_state.lineage.add_param('loss_function', loss)
            elif isinstance(loss, losses.Loss):
                self._dws_state.lineage.add_param('loss_function', loss.__class__.__name__)
            return super().compile(optimizer, loss, metrics, loss_weights,
                                   sample_weight_mode, weighted_metrics,
                                   target_tensors, distribute, **kwargs)
        def fit(self, x,y=None,  **kwargs):
            if 'epochs' in kwargs:
                self._dws_state.lineage.add_param('epochs', kwargs['epochs'])
            else:
                self._dws_state.lineage.add_param('epochs', 1)
            if 'batch_size' in kwargs:
                self._dws_state.lineage.add_param('fit_batch_size', kwargs['batch_size'])
            else:
                self._dws_state.lineage.add_param('fit_batch_size', None)
            api_resource =  self._dws_state.find_input_resources_and_return_if_api(x, y)
            if api_resource is not None:
                _verify_eager_if_dataset(x, y, api_resource)
                api_resource.init_hash_state()
                hash_state = api_resource.get_hash_state()
                _add_to_hash(x, hash_state)
                if y is not None:
                    _add_to_hash(y, hash_state)
            return super().fit(x, y, **kwargs)

        def evaluate(self, x, y=None, **kwargs):
            if 'batch_size' in kwargs:
                self._dws_state.lineage.add_param('evaluate_batch_size', kwargs['batch_size'])
            else:
                self._dws_state.lineage.add_param('evaluate_batch_size', None)
            api_resource =  self._dws_state.find_input_resources_and_return_if_api(x, y)
            if api_resource is not None:
                _verify_eager_if_dataset(x, y, api_resource)
                api_resource.dup_hash_state()
                hash_state = api_resource.get_hash_state()
                _add_to_hash(x, hash_state)
                if y is not None:
                    _add_to_hash(y, hash_state)
                api_resource.save_current_hash()
                api_resource.pop_hash_state()
            results = super().evaluate(x, y, **kwargs)
            assert len(results)==len(self.metrics_names)
            self._dws_state.write_metrics_and_complete({n:v for (n, v) in
                                                        zip(self.metrics_names, results)})
            return results
    WrappedModel.__name__ = Cls.__name__ # this is to fake things out for the reporting
    if workspace.verbose:
        print("dws>> Wrapped model class %s" % Cls.__name__)
    return WrappedModel
