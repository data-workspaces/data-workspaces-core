import unittest
import sys
import os.path
from os.path import exists, join
import json

from utils_for_tests import SimpleCase, WS_DIR

try:
    import tensorflow
    TF_INSTALLED=True
except ImportError:
    TF_INSTALLED=False

class TestTensorflowKit(SimpleCase):

    @unittest.skipUnless(TF_INSTALLED, "Tensorflow not available")
    def test_wrapper(self):
        """This test follows the basic classification tutorial.
        """
        import tensorflow as tf
        import tensorflow.keras as keras
        self._setup_initial_repo(git_resources='results', api_resources='fashion-mnist-data')
        from dataworkspaces.kits.tensorflow import add_lineage_to_keras_model_class
        keras.Sequential = add_lineage_to_keras_model_class(keras.Sequential,
                                                            input_resource='fashion-mnist-data',
                                                            verbose=True,
                                                            workspace_dir=WS_DIR)
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
        self.assertAlmostEqual(test_acc, data['metrics']['accuracy'])
        self.assertAlmostEqual(test_loss, data['metrics']['loss'])


if __name__ == '__main__':
    unittest.main()

