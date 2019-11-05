
import unittest
import sys
import os.path
import hashlib

try:
    import dataworkspaces
except ImportError:
    sys.path.append(os.path.abspath(".."))

from dataworkspaces.kits.wrapper_utils import _add_to_hash

try:
    import pandas
except ImportError:
    pandas = None

try:
    import numpy
except ImportError:
    numpy = None

try:
    import tensorflow
    if tensorflow.__version__.startswith('2.'):
        TF_VERSION=2
    else:
        TF_VERSION=1
except ImportError:
    tensorflow = None
    TF_VERSION=-1


class TestAddToHash(unittest.TestCase):
    def setUp(self):
        self.hash_state = hashlib.sha1()

    @unittest.skipUnless(pandas is not None, 'Pandas not available')
    def test_pandas_df(self):
        df = pandas.DataFrame({'x1':[1,2,3,4,5],
                               'x2':[1.5,2.5,3.5,4.5,5.5],
                               'y':[1,0,0,1,1]})
        _add_to_hash(df, self.hash_state)
        print(self.hash_state.hexdigest())

    @unittest.skipUnless(pandas is not None, 'Pandas not available')
    def test_pandas_series(self):
        s = pandas.Series([1,0,0,1,1], name='y')
        _add_to_hash(s, self.hash_state)
        print(self.hash_state.hexdigest())

    @unittest.skipUnless(numpy is not None, "Numpy not available")
    def test_numpy(self):
        a = numpy.arange(45)
        _add_to_hash(a, self.hash_state)
        print(self.hash_state.hexdigest())

    @unittest.skipUnless(tensorflow is not None and TF_VERSION==2, "Rensorflow 2 not availabile")
    @unittest.skipUnless(numpy is not None, "Numpy is not available")
    def test_tensorflow_tensor(self):
        dataset = tensorflow.data.Dataset.from_tensor_slices(numpy.arange(100).reshape((10,10)))
        for i in dataset:
            _add_to_hash(i, self.hash_state)
        print(self.hash_state.hexdigest())


if __name__ == '__main__':
    unittest.main()
