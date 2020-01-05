import unittest
import sys
import os.path
from os.path import exists, join
import json
import functools
import inspect

from utils_for_tests import SimpleCase, WS_DIR

try:
    import tensorflow
    TF_INSTALLED=True
    if tensorflow.__version__.startswith('2.'):
        TF_VERSION=2
    else:
        TF_VERSION=1
except ImportError:
    TF_INSTALLED=False

try:
    import numpy
    NUMPY_INSTALLED=True
except ImportError:
    NUMPY_INSTALLED=False

try:
    import pandas
    PANDAS_INSTALLED=True
except ImportError:
    PANDAS_INSTALLED=False

from dataworkspaces.kits.wrapper_utils import NotSupportedError

def generator_from_arrays(x, y):
    assert len(x)==len(y)
    # keras expects the same number of dimensions, so, we reshape to add one more
    old_shape = x[0].shape
    new_shape = (1, old_shape[0], old_shape[1])
    for i in range(len(y)):
        yield(x[i].reshape(new_shape), y[i].reshape((1,1)))

class TestTensorflowKit(SimpleCase):

    def setUp(self):
        super().setUp()
        if TF_INSTALLED:
            import tensorflow as tf
            self.sequential = tf.keras.Sequential

    def tearDown(self):
        super().tearDown()
        if TF_INSTALLED:
            import tensorflow as tf
            tf.keras.Sequential = self.sequential

    def _take_snapshot(self):
        self._run_dws(['snapshot', 'S1'], cwd=WS_DIR)

    @unittest.skipUnless(TF_INSTALLED, "Tensorflow not available")
    def test_wrapper_for_numpy(self):
        """This test follows the basic classification tutorial.
        """
        import tensorflow as tf
        import tensorflow.keras as keras
        self._setup_initial_repo(git_resources='results', api_resources='fashion-mnist-data')
        from dataworkspaces.kits.tensorflow import add_lineage_to_keras_model_class, CheckpointConfig
        keras.Sequential = add_lineage_to_keras_model_class(keras.Sequential,
                                                            input_resource='fashion-mnist-data',
                                                            verbose=True,
                                                            workspace_dir=WS_DIR,
                                                            checkpoint_config=CheckpointConfig('fashion',
                                                                                               monitor='loss',
                                                                                               save_best_only=True))
        fashion_mnist = keras.datasets.fashion_mnist
        (train_images, train_labels), (test_images, test_labels) = fashion_mnist.load_data()
        train_images = train_images / 255.0
        test_images = test_images / 255.0
        model = keras.Sequential([
            keras.layers.Flatten(input_shape=(28, 28)),
            keras.layers.Dense(128, activation='relu'),
            keras.layers.Dense(10, activation='softmax')
        ])
        model.compile(optimizer='adam',
                      loss='sparse_categorical_crossentropy',
                      metrics=['accuracy'])
        model.fit(train_images, train_labels, epochs=5)
        test_loss, test_acc = model.evaluate(test_images,  test_labels, verbose=2)
        print("test accuracy: %s" % test_acc)
        results_file = join(WS_DIR, 'results/results.json')
        self.assertTrue(exists(results_file), "missing file %s" % results_file)
        with open(results_file, 'r') as f:
            data = json.load(f)
        self.assertAlmostEqual(test_acc, data['metrics']['accuracy' if TF_VERSION==2 else 'acc'])
        self.assertAlmostEqual(test_loss, data['metrics']['loss'])
        self._take_snapshot()

    @unittest.skipUnless(TF_INSTALLED, "Tensorflow not available")
    @unittest.skipUnless(NUMPY_INSTALLED, "numpy not installed")
    @unittest.skipUnless(PANDAS_INSTALLED, 'pandas not available')
    def test_wrapper_for_dataset(self):
        """This follows the csv tutorial (titanic data set)
        """
        import tensorflow as tf
        import pandas as pd
        import numpy as np
        self._setup_initial_repo(git_resources='results', api_resources='titanic-data')
        TRAIN_DATA_URL = "https://storage.googleapis.com/tf-daxtasets/titanic/train.csv"
        TEST_DATA_URL = "https://storage.googleapis.com/tf-datasets/titanic/eval.csv"
        train_file_path = tf.keras.utils.get_file("train.csv", TRAIN_DATA_URL)
        test_file_path = tf.keras.utils.get_file("eval.csv", TEST_DATA_URL)
        LABEL_COLUMN = 'survived'
        LABELS = [0, 1]
        def get_dataset(file_path, **kwargs):
            dataset = tf.data.experimental.make_csv_dataset(
                file_path,
                batch_size=5, # Artificially small to make examples easier to show.
                label_name=LABEL_COLUMN,
                na_value="?",
                num_epochs=1,
                ignore_errors=True, 
                **kwargs)
            return dataset

        raw_train_data = get_dataset(train_file_path)
        raw_test_data = get_dataset(test_file_path)
        SELECT_COLUMNS = ['survived', 'age', 'n_siblings_spouses', 'parch', 'fare']
        DEFAULTS = [0, 0.0, 0.0, 0.0, 0.0]
        temp_dataset = get_dataset(train_file_path, 
                                   select_columns=SELECT_COLUMNS,
                                   column_defaults = DEFAULTS)
        def pack(features, label):
            return tf.stack(list(features.values()), axis=-1), label
        packed_dataset = temp_dataset.map(pack)

        class PackNumericFeatures(object):
            def __init__(self, names):
                self.names = names

            def __call__(self, features, labels):
                numeric_freatures = [features.pop(name) for name in self.names]
                numeric_features = [tf.cast(feat, tf.float32) for feat in numeric_freatures]
                numeric_features = tf.stack(numeric_features, axis=-1)
                features['numeric'] = numeric_features
                #print('features type: %s, labels type: %s' % (type(features), type(labels)))
                return features, labels

        NUMERIC_FEATURES = ['age','n_siblings_spouses','parch', 'fare']

        packed_train_data = raw_train_data.map(PackNumericFeatures(NUMERIC_FEATURES))

        packed_test_data = raw_test_data.map(
            PackNumericFeatures(NUMERIC_FEATURES))
        desc = pd.read_csv(train_file_path)[NUMERIC_FEATURES].describe()
        MEAN = np.array(desc.T['mean'])
        STD = np.array(desc.T['std'])
        def normalize_numeric_data(data, mean, std):
            # Center the data
            return (data-mean)/std
        normalizer = functools.partial(normalize_numeric_data, mean=MEAN, std=STD)
        numeric_column = tf.feature_column.numeric_column('numeric', normalizer_fn=normalizer, shape=[len(NUMERIC_FEATURES)])
        numeric_columns = [numeric_column]
        numeric_layer = tf.keras.layers.DenseFeatures(numeric_columns)
        CATEGORIES = {
            'sex': ['male', 'female'],
            'class' : ['First', 'Second', 'Third'],
            'deck' : ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J'],
            'embark_town' : ['Cherbourg', 'Southhampton', 'Queenstown'],
            'alone' : ['y', 'n']
        }
        categorical_columns = []
        for feature, vocab in CATEGORIES.items():
            cat_col = tf.feature_column.categorical_column_with_vocabulary_list(
                key=feature, vocabulary_list=vocab)
            categorical_columns.append(tf.feature_column.indicator_column(cat_col))
        categorical_layer = tf.keras.layers.DenseFeatures(categorical_columns)
        preprocessing_layer = tf.keras.layers.DenseFeatures(categorical_columns+numeric_columns)
        from dataworkspaces.kits.tensorflow import add_lineage_to_keras_model_class, CheckpointConfig
        tf.keras.Sequential = add_lineage_to_keras_model_class(tf.keras.Sequential, input_resource='titanic-data',
                                                               workspace_dir=WS_DIR,
                                                               checkpoint_config=CheckpointConfig('fashion',
                                                                                               monitor='loss',
                                                                                                  save_best_only=True),
                                                               verbose=True)
        model = tf.keras.Sequential([
            preprocessing_layer,
            tf.keras.layers.Dense(128, activation='relu'),
            tf.keras.layers.Dense(128, activation='relu'),
            tf.keras.layers.Dense(1, activation='sigmoid'),
        ])
        model.compile(
            loss='binary_crossentropy',
            optimizer='adam',
            metrics=['accuracy'])
        train_data = packed_train_data.shuffle(500)
        test_data = packed_test_data
        if TF_VERSION==1:
            with self.assertRaises(NotSupportedError):
                model.fit(train_data, epochs=20)
            return # stop early, not supported in 1.x
        else:
            model.fit(train_data, epochs=20)
        test_loss, test_accuracy = model.evaluate(test_data)
        print('\n\nTest Loss {}, Test Accuracy {}'.format(test_loss, test_accuracy))
        self.assertAlmostEqual(test_accuracy, 0.88, delta=0.2)
        self.assertAlmostEqual(test_loss, 0.31, delta=0.3)
        predictions = model.predict(test_data)
        results_file = join(WS_DIR, 'results/results.json')
        self.assertTrue(exists(results_file), "missing file %s" % results_file)
        with open(results_file, 'r') as f:
            data = json.load(f)
        self.assertAlmostEqual(test_accuracy, data['metrics']['accuracy' if TF_VERSION==2 else 'acc'])
        self.assertAlmostEqual(test_loss, data['metrics']['loss'])
        self._take_snapshot()

    @unittest.skipUnless(TF_INSTALLED, "Tensorflow not available")
    def test_wrapper_for_generators(self):
        """This test follows the basic classification tutorial, modified for using
        the fit_generator() and eval_generator() methods.
        """
        import tensorflow as tf
        import tensorflow.keras as keras
        self._setup_initial_repo(git_resources='results', api_resources='fashion-mnist-data')
        from dataworkspaces.kits.tensorflow import add_lineage_to_keras_model_class, CheckpointConfig
        keras.Sequential = add_lineage_to_keras_model_class(keras.Sequential,
                                                            input_resource='fashion-mnist-data',
                                                            verbose=True,
                                                            workspace_dir=WS_DIR,
                                                            checkpoint_config=CheckpointConfig('fashion',
                                                                                               monitor='loss',
                                                                                               save_best_only=True))

        fashion_mnist = keras.datasets.fashion_mnist
        (train_images, train_labels), (test_images, test_labels) = fashion_mnist.load_data()
        train_images = train_images / 255.0
        test_images = test_images / 255.0
        model = keras.Sequential([
            keras.layers.Flatten(input_shape=(28, 28)),
            keras.layers.Dense(128, activation='relu'),
            keras.layers.Dense(10, activation='softmax')
        ])
        model.compile(optimizer='adam',
                      loss='sparse_categorical_crossentropy',
                      metrics=['accuracy'])
        g = generator_from_arrays(train_images, train_labels)
        self.assertTrue(inspect.isgenerator(g))
        model.fit_generator(g, epochs=5, steps_per_epoch=2)
        g2 = generator_from_arrays(test_images, test_labels)
        test_loss, test_acc = model.evaluate_generator(g2, steps=len(test_labels), verbose=2)
        print("test accuracy: %s" % test_acc)
        results_file = join(WS_DIR, 'results/results.json')
        self.assertTrue(exists(results_file), "missing file %s" % results_file)
        with open(results_file, 'r') as f:
            data = json.load(f)
        self.assertAlmostEqual(test_acc, data['metrics']['accuracy' if TF_VERSION==2 else 'acc'])
        self.assertAlmostEqual(test_loss, data['metrics']['loss'])
        self._take_snapshot()

    @unittest.skipUnless(TF_INSTALLED, "Tensorflow not available")
    def test_wrapper_for_keras_sequence(self):
        """This test follows the basic classification tutorial, modified for using
        the fit_generator() and eval_generator() methods.
        """
        import tensorflow as tf
        import tensorflow.keras as keras
        import tensorflow.keras.utils as kerasutils
        class KSequence(kerasutils.Sequence):
            def __init__(self, x, y):
                assert len(x)==len(y)
                self.x = x
                self.y = y
                old_shape = x[0].shape
                self.new_shape = (1, old_shape[0], old_shape[1])

            def __iter__(self):
                return generator_from_arrays(self.x, self.y)

            def __getitem__(self, idx):
                return (self.x[idx].reshape(self.new_shape), self.y[idx].reshape((1,1)))

            def __len__(self):
                return len(self.y)

        self._setup_initial_repo(git_resources='results', api_resources='fashion-mnist-data')
        from dataworkspaces.kits.tensorflow import add_lineage_to_keras_model_class, CheckpointConfig
        keras.Sequential = add_lineage_to_keras_model_class(keras.Sequential,
                                                            input_resource='fashion-mnist-data',
                                                            verbose=True,
                                                            workspace_dir=WS_DIR,
                                                            checkpoint_config=CheckpointConfig('fashion',
                                                                                               monitor='loss',
                                                                                               save_best_only=True))

        fashion_mnist = keras.datasets.fashion_mnist
        (train_images, train_labels), (test_images, test_labels) = fashion_mnist.load_data()
        train_images = train_images / 255.0
        test_images = test_images / 255.0
        model = keras.Sequential([
            keras.layers.Flatten(input_shape=(28, 28)),
            keras.layers.Dense(128, activation='relu'),
            keras.layers.Dense(10, activation='softmax')
        ])
        model.compile(optimizer='adam',
                      loss='sparse_categorical_crossentropy',
                      metrics=['accuracy'])
        g = KSequence(train_images, train_labels)
        model.fit_generator(g, epochs=5, steps_per_epoch=2)
        g2 = KSequence(test_images, test_labels)
        test_loss, test_acc = model.evaluate_generator(g2, steps=len(test_labels), verbose=2)
        print("test accuracy: %s" % test_acc)
        results_file = join(WS_DIR, 'results/results.json')
        self.assertTrue(exists(results_file), "missing file %s" % results_file)
        with open(results_file, 'r') as f:
            data = json.load(f)
        self.assertAlmostEqual(test_acc, data['metrics']['accuracy' if TF_VERSION==2 else 'acc'])
        self.assertAlmostEqual(test_loss, data['metrics']['loss'])
        self._take_snapshot()

if __name__ == '__main__':
    unittest.main()

