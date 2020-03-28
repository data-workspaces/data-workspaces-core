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
    # We add an optional checkpoint configuration, which will cause checkpoints
    # to be written to the workspace's scratch directory and then the best
    # checkpoint copied to the results resource.
    keras.Sequential = add_lineage_to_keras_model_class(keras.Sequential,
                           checkpoint_config=CheckpointConfig(model='fashion',
                                                              monitor='loss'))
    
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
from typing import Optional, Union, List, Dict, cast, NamedTuple

assert List
import os
from os.path import join, isdir, exists, basename
import re
import glob

import tensorflow

if tensorflow.__version__.startswith("2."):  # type: ignore
    USING_TENSORFLOW2 = True
else:
    USING_TENSORFLOW2 = False
import tensorflow.keras.optimizers as optimizers
import tensorflow.keras.utils as kerasutils
from tensorflow.keras.callbacks import ModelCheckpoint

if USING_TENSORFLOW2:
    import tensorflow.keras.losses as losses
else:
    import tensorflow.losses as losses

from dataworkspaces.workspace import (
    find_and_load_workspace,
    ResourceRef,
    ResourceRoles,
    FileResourceMixin,
)
from dataworkspaces.errors import ConfigurationError
from dataworkspaces.kits.wrapper_utils import (
    _DwsModelState,
    _add_to_hash,
    NotSupportedError,
    _find_resource,
)


def _verify_eager_if_dataset(x, y, api_resource):
    """If this is tensorflow 1.x and non-eager mode, there's no way
    to evaluate the dataset outside the tensor graph.
    """
    if (
        (not USING_TENSORFLOW2)
        and (isinstance(x, tensorflow.data.Dataset) or isinstance(y, tensorflow.data.Dataset))  # type: ignore
        and (not tensorflow.executing_eagerly())  # type: ignore
    ):
        raise NotSupportedError(
            "Using an API resource ("
            + api_resource.name
            + ") with non-eager datasets is not "
            + "supported with TensorFlow 1.x."
        )


def _wrap_generator(wrapped, hash_state):
    """Return a generator such that it hashes
    the values returned for each iterator
    """

    def wrapper():
        for v in wrapped:
            if len(v) == 2:
                (inputs, targets) = v
                sample_weights = None
            else:
                (inputs, targets, sample_weights) = v
            _add_to_hash(inputs, hash_state)
            _add_to_hash(targets, hash_state)
            if sample_weights is not None:
                _add_to_hash(sample_weights, hash_state)
            yield v

    return wrapper()


class _TfKerasSequenceWrapper(kerasutils.Sequence):
    def __init__(self, wrapped, hash_state):
        self.wrapped = wrapped
        self.hash_state = hash_state

    def __getitem__(self, idx):
        v = self.wrapped.__getitem__(idx)
        if len(v) == 2:
            (inputs, targets) = v
            sample_weights = None
        else:
            (inputs, targets, sample_weights) = v
        _add_to_hash(inputs, self.hash_state)
        _add_to_hash(targets, self.hash_state)
        if sample_weights is not None:
            _add_to_hash(sample_weights, self.hash_state)
        return v

    def __len__(self):
        return self.wrapped.__len__()

    def __iter__(self):
        return _wrap_generator(self.wrapped, self.hash_state)

    def on_epoch_end(self):
        return self.on_epoch_end()


class DwsModelCheckpoint(ModelCheckpoint):
    """
    Subclass of tf.keras.callbacks.ModelCheckpoint which will save checkpoints
    to the workspace's stratch space and then move the most recent/best checkpoint
    to the results directory at the end of the run.

    You can instantiate this class directly and pass it to the ``callbacks``
    parameter of the model's ``fit()`` method::

          model.fit(train_images, train_labels, epochs=10,
                    callbacks=[DwsModelCheckpoint('fashion', monitor='loss', save_best_only=True)])

    You can also pass :class:`~CheckpointConfig` instance to the
    :func:`~add_lineage_to_keras_model_class` wrapper function.
    """

    def __init__(
        self,
        model_name: str,
        monitor: str = "val_loss",
        save_best_only: bool = False,
        mode: str = "auto",
        save_freq: Union[str, int] = "epoch",
        results_resource: Optional[Union[str, ResourceRef]] = None,
        workspace_dir: Optional[str] = None,
        verbose: Union[int, bool] = 0,
    ):
        """
        model_name is used to create the checkpoint filenames. The checkpoints
        will be saved as MODEL_NAME_{epoch}.

        Currently, only supports save_weights_only option.

        verbose can be either 0,1 in the style of tensorflow or a True,False
        in the style of Data Workspaces.

        """
        self.dws_model_name = model_name
        if verbose == 0 or verbose == False:
            tf_verbose = 0
            dws_verbose = False
        else:
            tf_verbose = 1
            dws_verbose = True

        self.workspace = find_and_load_workspace(
            batch=True, verbose=dws_verbose, uri_or_local_path=workspace_dir
        )

        results_ref = _find_resource(self.workspace, ResourceRoles.RESULTS, results_resource)
        self.results_resource = self.workspace.get_resource(results_ref.name)
        if not isinstance(self.results_resource, FileResourceMixin):
            raise ConfigurationError("Resource %s is not a file-based resource" % results_ref.name)
        self.results_subdir = results_ref.subpath  # type: Optional[str]
        scratch_dir = self.workspace.get_scratch_directory()
        assert isdir(scratch_dir), "missing scratch directory %s" % scratch_dir
        self.dws_checkpoint_path = join(scratch_dir, "checkpoints")  # type: str
        if not isdir(self.dws_checkpoint_path):
            os.mkdir(self.dws_checkpoint_path)
        self.checkpoint_filepath_template = join(self.dws_checkpoint_path, model_name + "_{epoch}")
        super().__init__(
            filepath=self.checkpoint_filepath_template,
            monitor=monitor,
            save_best_only=save_best_only,
            mode=mode,
            save_freq=save_freq,
            save_weights_only=True,
            verbose=tf_verbose,
        )

    def on_train_begin(self, logs: Optional[Dict] = None):
        files_to_delete = []  # type: List[str]
        files_to_delete.extend(
            glob.glob(join(self.dws_checkpoint_path, self.dws_model_name + "_*[0-9].index"))
        )
        files_to_delete.extend(
            glob.glob(
                join(
                    self.dws_checkpoint_path, self.dws_model_name + "_*[0-9].data-*[0-9]-of-*[0-9]"
                )
            )
        )
        checkpoint_metadata_file = join(self.dws_checkpoint_path, "checkpoint")
        if exists(checkpoint_metadata_file):
            files_to_delete.append(checkpoint_metadata_file)
        for f in files_to_delete:
            os.remove(f)
        print(
            "dws> Removed %d old checkpoint files for model %s ahead of training"
            % (len(files_to_delete), self.dws_model_name)
        )
        return super().on_train_begin(logs)

    def on_train_end(self, logs: Optional[Dict] = None):
        checkpoint_metadata_file = join(self.dws_checkpoint_path, "checkpoint")
        assert exists(checkpoint_metadata_file), (
            "Missing checkpoint metadata file %s" % checkpoint_metadata_file
        )
        # find the checkpoint that we want to save
        with open(checkpoint_metadata_file, "r") as f:
            MODEL_CHECKPOINT_PATH = re.compile(
                "^"
                + re.escape("model_checkpoint_path:")
                + r'\s+"('
                + re.escape(self.dws_model_name + "_")
                + r'\d+)"$'
            )
            checkpoint_base = None
            for line in f:
                mo = MODEL_CHECKPOINT_PATH.match(line.rstrip())
                if mo is not None:
                    checkpoint_base = mo.group(1)
                    break
            assert checkpoint_base is not None, (
                "Did not find model checkpoint path in %s" % checkpoint_metadata_file
            )
        copy_files = []  # type: List[str]
        copy_files.append(join(self.dws_checkpoint_path, checkpoint_base + ".index"))
        copy_files.extend(
            glob.glob(join(self.dws_checkpoint_path, checkpoint_base + ".data-*[0-9]-of-*[0-9]"))
        )
        copy_files.append(
            join(self.dws_checkpoint_path, "checkpoint")
        )  # copy index file to make it easy to load checkpoint
        for src_file in copy_files:
            if self.results_subdir is not None:
                dest_path = join(self.results_subdir, basename(src_file))
            else:
                dest_path = basename(src_file)
            cast(FileResourceMixin, self.results_resource).upload_file(src_file, dest_path)
        if self.results_subdir is not None:
            print(
                "dws> Copied checkpoint %s to resource %s:%s"
                % (checkpoint_base, self.results_resource.name, self.results_subdir)
            )
        else:
            print(
                "dws> Copied checkpoint %s to resource %s"
                % (checkpoint_base, self.results_resource.name)
            )

        return super().on_train_end(logs)


class CheckpointConfig(NamedTuple):
    """Configuration for checkpoints, to be passed as a parameter
    to :func:`~add_lineage_to_keras_model_class`, instead of
    directly instantiating :class:`~DwsModelChecpoint`.

    The checkpoints are initially written under the workspace's
    scratch space. At the end of training, the best checkpoint is
    copied to the results resource.

    The configuration fields are:

    * ``model_name`` - name of the model to use in checkpoint files
    * ``monitor`` - metric to monitor - defaults to val_loss
    * ``save_best_only`` - if True, only checkpoints better than the
      previous are kept.
    * ``mode`` - how to determine whether a metric is the "best" - auto, min, or max
    * ``save_freq`` - 'epoch' or an interger
    """

    model_name: str
    monitor: str = "val_loss"
    save_best_only: bool = False
    mode: str = "auto"
    save_freq: Union[str, int] = "epoch"


def add_lineage_to_keras_model_class(
    Cls: type,
    input_resource: Optional[Union[str, ResourceRef]] = None,
    results_resource: Optional[Union[str, ResourceRef]] = None,
    workspace_dir: Optional[str] = None,
    checkpoint_config: Optional[CheckpointConfig] = None,
    verbose: bool = False,
) -> type:
    """This function wraps a Keras model class with a subclass that overwrites
    key methods to make calls to the data lineage API.

    **Parameters:**

    * ``Cls`` -- the class being wrapped
    * ``input_resources`` -- optional list of input resources to this model.
      Each resource may be specified by name, by a local file path, or via a
      ``ResourceRef``. If no inputs are specified, will try to infer from the
      workspace.
    * ``results_resource`` -- optional resource where the results are to be stored.
      May be specified by name, by a local file path, or via a ``ResourceRef``.
      if not specified, will try to infer from the workspace.
    * ``workspace-dir`` -- Optional directory specifying the workspace. Usually can be
      inferred from the current directory.
    * ``checkpoint_config`` -- Optional instance of :class:`~CheckpointConfig`, which
      is used to enable checkpointing on fit and fit_generator()
    * ``verbose`` -- If True, print extra debugging information.

    The following methods are wrapped:

    * :func:`~__init__` - loads the workspace and adds dws-specific class members
    * :func:`~compile` - captures the ``optimizer`` and ``loss_function`` parameter values
    * :func:`~fit` - captures the ``epochs`` and ``batch_size`` parameter values;
      if input is an API resource, capture hash values of training data, otherwise capture
      input resource name.
    * :func:`~fit_generator` - captues the ``epochs`` and ``steps_per_epoch`` parameter
      values; if input is an API resource, wraps the generator and captures the hashes
      of returned values from the generator as it is iterated through.
    * :func:`~evaluate` - captures the ``batch_size`` parameter value; if input is an
      API resource, capture hash values of test data, otherwise capture input resource
      name; capture metrics and write them to results resource.
    * :func:`~evaluate_generator` - captures the ``steps`` parameter value; if input is
      an API resource, wraps the generator and captures the hashes of returned values
      from the generator as it is iterated through.
    """
    if hasattr(Cls, "_dws_model_wrap") and Cls._dws_model_wrap is True:  # type: ignore
        print("dws>> %s or a superclass is already wrapped" % Cls.__name__)
        return Cls  # already wrapped
    workspace = find_and_load_workspace(
        batch=True, verbose=verbose, uri_or_local_path=workspace_dir
    )

    class WrappedModel(Cls):  # type: ignore
        _dws_model_wrap = True

        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self._dws_state = _DwsModelState(workspace, input_resource, results_resource)
            if checkpoint_config is not None:
                self.checkpoint_cb = DwsModelCheckpoint(
                    checkpoint_config.model_name,
                    monitor=checkpoint_config.monitor,
                    save_best_only=checkpoint_config.save_best_only,
                    mode=checkpoint_config.mode,
                    save_freq=checkpoint_config.save_freq,
                    results_resource=results_resource,
                    workspace_dir=workspace_dir,
                    verbose=verbose,
                )  # type: Optional[DwsModelCheckpoint]
            else:
                self.checkpoint_cb = None

        def compile(
            self,
            optimizer,
            loss=None,
            metrics=None,
            loss_weights=None,
            sample_weight_mode=None,
            weighted_metrics=None,
            target_tensors=None,
            distribute=None,
            **kwargs
        ):
            if isinstance(optimizer, str):
                self._dws_state.lineage.add_param("optimizer", optimizer)
            elif isinstance(optimizer, optimizers.Optimizer):
                self._dws_state.lineage.add_param("optimizer", optimizer.__class__.__name__)
            if isinstance(loss, str):
                self._dws_state.lineage.add_param("loss_function", loss)
            elif isinstance(loss, losses.Loss):
                self._dws_state.lineage.add_param("loss_function", loss.__class__.__name__)
            return super().compile(
                optimizer,
                loss,
                metrics,
                loss_weights,
                sample_weight_mode,
                weighted_metrics,
                target_tensors,
                distribute,
                **kwargs,
            )

        def fit(self, x, y=None, **kwargs):
            if "epochs" in kwargs:
                self._dws_state.lineage.add_param("fit.epochs", kwargs["epochs"])
            else:
                self._dws_state.lineage.add_param("fit.epochs", 1)
            if "batch_size" in kwargs:
                self._dws_state.lineage.add_param("fit.batch_size", kwargs["batch_size"])
            else:
                self._dws_state.lineage.add_param("fit.batch_size", None)
            api_resource = self._dws_state.find_input_resources_and_return_if_api(x, y)
            if api_resource is not None:
                _verify_eager_if_dataset(x, y, api_resource)
                api_resource.init_hash_state()
                hash_state = api_resource.get_hash_state()
                _add_to_hash(x, hash_state)
                if y is not None:
                    _add_to_hash(y, hash_state)
                api_resource.save_current_hash()  # in case we evaluate in a separate process
            if self.checkpoint_cb:
                if "callbacks" in kwargs:
                    kwargs["callbacks"].append(self.checkpoint_cb)
                else:
                    kwargs["callbacks"] = [
                        self.checkpoint_cb,
                    ]
            return super().fit(x, y, **kwargs)

        def fit_generator(
            self,
            generator,
            steps_per_epoch=None,
            epochs=1,
            verbose=1,
            callbacks=None,
            validation_data=None,
            validation_steps=None,
            validation_freq=1,
            class_weight=None,
            max_queue_size=10,
            workers=1,
            use_multiprocessing=False,
            shuffle=True,
            initial_epoch=0,
        ):
            self._dws_state.lineage.add_param("fit_generator.epochs", epochs)
            self._dws_state.lineage.add_param("fit_generator.steps_per_epoch", steps_per_epoch)
            api_resource = self._dws_state.find_input_resources_and_return_if_api(generator)
            if api_resource is not None:
                # wrap the generator to capture each entry as it is returned
                api_resource.init_hash_state()
                hash_state = api_resource.get_hash_state()
                if isinstance(generator, kerasutils.Sequence):
                    generator = _TfKerasSequenceWrapper(generator, hash_state)
                else:
                    generator = _wrap_generator(generator, hash_state)
            if self.checkpoint_cb:
                if callbacks is not None:
                    callbacks.append(self.checkpoint_cb)
                else:
                    callbacks = [
                        self.checkpoint_cb,
                    ]
            results = super().fit_generator(
                generator,
                steps_per_epoch,
                epochs,
                verbose,
                callbacks,
                validation_data,
                validation_steps,
                validation_freq,
                class_weight,
                max_queue_size,
                workers,
                use_multiprocessing,
                shuffle,
                initial_epoch,
            )
            if api_resource is not None:
                api_resource.save_current_hash()
            return results

        def evaluate(self, x, y=None, **kwargs):
            if "batch_size" in kwargs:
                self._dws_state.lineage.add_param("evaluate.batch_size", kwargs["batch_size"])
            else:
                self._dws_state.lineage.add_param("evaluate.batch_size", None)
            api_resource = self._dws_state.find_input_resources_and_return_if_api(x, y)
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
            assert len(results) == len(self.metrics_names)
            self._dws_state.write_metrics_and_complete(
                {n: v for (n, v) in zip(self.metrics_names, results)}
            )
            return results

        def evaluate_generator(
            self,
            generator,
            steps=None,
            callbacks=None,
            max_queue_size=10,
            workers=1,
            use_multiprocessing=False,
            verbose=0,
        ):
            self._dws_state.lineage.add_param("evaluate_generator.steps", steps)
            api_resource = self._dws_state.find_input_resources_and_return_if_api(generator)
            if api_resource is not None:
                # wrap the generator to capture each entry as it is returned
                api_resource.dup_hash_state()
                hash_state = api_resource.get_hash_state()
                if isinstance(generator, kerasutils.Sequence):
                    generator = _TfKerasSequenceWrapper(generator, hash_state)
                else:
                    generator = _wrap_generator(generator, hash_state)
            results = super().evaluate_generator(
                generator, steps, callbacks, max_queue_size, workers, use_multiprocessing, verbose
            )
            if api_resource is not None:
                api_resource.save_current_hash()
                api_resource.pop_hash_state()
            assert len(results) == len(self.metrics_names)
            self._dws_state.write_metrics_and_complete(
                {n: v for (n, v) in zip(self.metrics_names, results)}
            )
            return results

    WrappedModel.__name__ = Cls.__name__  # this is to fake things out for the reporting
    if workspace.verbose:
        print("dws>> Wrapped model class %s" % Cls.__name__)
    return WrappedModel
