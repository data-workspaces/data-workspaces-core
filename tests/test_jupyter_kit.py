import unittest
import sys
import os
import os.path
from os.path import dirname, abspath, expanduser, exists, join
import shutil
import subprocess

TEST_DIR=abspath(expanduser(dirname(__file__)))

try:
    import dataworkspaces
except ImportError:
    sys.path.append(os.path.abspath(".."))

from dataworkspaces.utils.subprocess_utils import find_exe


TEMPDIR=os.path.abspath(os.path.expanduser(__file__)).replace('.py', '_data')
WS_DIR=join(TEMPDIR, 'workspace')
CODE_DIR=join(WS_DIR, 'code')
NOTEBOOK='test_jupyter_kit.ipynb'

PYTHONPATH=os.path.abspath("..")

try:
    JUPYTER=find_exe('jupyter', 'install Jupyter before running this test')
    ERROR = None
except Exception as e:
    ERROR = e
    JUPYTER=None

try:
    import pandas
except ImportError:
    pandas = None
try:
    import numpy
except ImportError:
    numpy = None

@unittest.skipUnless(JUPYTER is not None, "SKIP: No Jupyter install found: %s"%ERROR)
class TestJupyterKit(unittest.TestCase):
    def setUp(self):
        if exists(TEMPDIR):
            shutil.rmtree(TEMPDIR)
        os.mkdir(TEMPDIR)
        os.mkdir(WS_DIR)
        self.dws=find_exe("dws", "Make sure you have enabled your python virtual environment")
        self._run_dws(['init', '--hostname', 'test-host',
                       '--create-resources=code,source-data,intermediate-data,results'],
                      verbose=False)
        shutil.copy(NOTEBOOK, join(CODE_DIR, NOTEBOOK))

    def tearDown(self):
        if exists(TEMPDIR):
            shutil.rmtree(TEMPDIR)


    def _run_dws(self, dws_args, cwd=WS_DIR, env=None, verbose=True):
        if verbose:
            command = self.dws + ' --verbose --batch '+ ' '.join(dws_args)
        else:
            command = self.dws + ' --batch '+ ' '.join(dws_args)
        print(command + (' [%s]' % cwd))
        r = subprocess.run(command, cwd=cwd, shell=True, env=env)
        r.check_returncode()

    def test_jupyter(self):
        command = "%s nbconvert --to notebook --execute %s" % (JUPYTER, NOTEBOOK)
        print(command)
        import copy
        env=copy.copy(os.environ)
        env['PYTHONPATH']=PYTHONPATH
        print("set pythonpath to %s" % PYTHONPATH)
        r = subprocess.run(command, cwd=CODE_DIR, shell=True, env=env)
        r.check_returncode()
        self._run_dws(['snapshot', '-m', "'snapshot of notebook run'", 'S1'],
                      verbose=False)

@unittest.skipUnless(JUPYTER is not None, "SKIP: No Jupyter install found: %s"%ERROR)
@unittest.skipUnless(pandas is not None, "SKIP: pandas is not installed")
@unittest.skipUnless(numpy is not None, "SKIP: numpy is not installed")
class TestHeatmapBinning(unittest.TestCase):
    def test_no_unique(self):
        from dataworkspaces.kits.jupyter import _metric_col_to_colormap
        from pandas.testing import assert_series_equal
        bins = _metric_col_to_colormap(pandas.Series([numpy.nan, numpy.nan]))
        assert_series_equal(pandas.Series([-1,-1]), bins)
    def test_one_unique(self):
        from dataworkspaces.kits.jupyter import _metric_col_to_colormap
        from pandas.testing import assert_series_equal
        bins = _metric_col_to_colormap(pandas.Series([1.2, numpy.nan, 1.2]))
        assert_series_equal(pandas.Series([3,-1,3]), bins)
    def test_two_unique(self):
        from dataworkspaces.kits.jupyter import _metric_col_to_colormap
        from pandas.testing import assert_series_equal
        bins = _metric_col_to_colormap(pandas.Series([1.4, numpy.nan, 1.2]))
        assert_series_equal(pandas.Series([4,-1,2]), bins)
    def test_three_unique(self):
        from dataworkspaces.kits.jupyter import _metric_col_to_colormap
        from pandas.testing import assert_series_equal
        bins = _metric_col_to_colormap(pandas.Series([1.4, numpy.nan, 1.2, 1.0]))
        assert_series_equal(pandas.Series([4,-1,3, 2]), bins, check_dtype=False)
    def test_four_unique(self):
        from dataworkspaces.kits.jupyter import _metric_col_to_colormap
        from pandas.testing import assert_series_equal
        bins = _metric_col_to_colormap(pandas.Series([1.4, numpy.nan, 1.2, 1.0, 1.0, 0.8]))
        assert_series_equal(pandas.Series([4,-1,3, 2,2,2]), bins, check_dtype=False)
    def test_five_unique(self):
        from dataworkspaces.kits.jupyter import _metric_col_to_colormap
        from pandas.testing import assert_series_equal
        bins = _metric_col_to_colormap(pandas.Series([1.4, numpy.nan, 1.2, 1.0, 1.0, 0.8, 0.4]))
        assert_series_equal(pandas.Series([5,-1,4, 2, 2, 1, 1]), bins, check_dtype=False)
    def test_six_unique(self):
        from dataworkspaces.kits.jupyter import _metric_col_to_colormap
        from pandas.testing import assert_series_equal
        bins = _metric_col_to_colormap(pandas.Series([1.4, numpy.nan, 1.2, 1.0, 1.0, 0.8, 0.4, 1.5]))
        assert_series_equal(pandas.Series([4,-1,3, 2, 2, 1, 1, 5]), bins, check_dtype=False)
    def test_seven_unique(self):
        from dataworkspaces.kits.jupyter import _metric_col_to_colormap
        from pandas.testing import assert_series_equal
        bins = _metric_col_to_colormap(pandas.Series([0.2, 1.4, numpy.nan, 1.2, 1.0, 1.0, 0.8, 0.4, 1.5]))
        assert_series_equal(pandas.Series([0, 5,-1,5, 2, 2, 1, 0, 6]), bins, check_dtype=False)
    def test_eight_unique(self):
        from dataworkspaces.kits.jupyter import _metric_col_to_colormap
        from pandas.testing import assert_series_equal
        bins = _metric_col_to_colormap(pandas.Series([0.1, 0.2, 1.4, numpy.nan, 1.2, 1.0, 1.0, 0.8, 0.4, 1.5]))
        assert_series_equal(pandas.Series([0, 0, 6,-1,5, 3, 3, 2, 1, 6]), bins, check_dtype=False)
    def test_random(self):
        from dataworkspaces.kits.jupyter import _metric_col_to_colormap
        import random
        random.seed(1)
        data = pandas.Series([random.gauss(5, 1) for i in range(100)])
        bins = _metric_col_to_colormap(data)
    def test_combined_bins(self):
        """"Test case from real bug where qcut() returns fewer bins than we asked"""
        # there are 5 unique values, but qcut() will put it into 4 bins.
        from dataworkspaces.kits.jupyter import _metric_col_to_colormap
        from pandas.testing import assert_series_equal
        data = pandas.Series([numpy.nan, 0.729885, 0.655172, 0.729885, numpy.nan, 0.729885, 0.747126, 0.729885, 0.729885, 0.701149, 0.729885, 0.758621])
        bins = _metric_col_to_colormap(data)
        expected=pandas.Series([-1,2,1,2,-1,2,5,2,2,1,2,5])
        assert_series_equal(expected, bins, check_dtype=False)

if __name__ == '__main__':
    unittest.main()

