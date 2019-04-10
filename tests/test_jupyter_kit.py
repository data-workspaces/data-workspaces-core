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

try:
    JUPYTER=find_exe('jupyter', 'install Jupyter before running this test')
    ERROR = None
except Exception as e:
    ERROR = e
    JUPYTER=None

@unittest.skipUnless(JUPYTER is not None, "No Jupyter install found: %s"%ERROR)
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
        r = subprocess.run(command, cwd=CODE_DIR, shell=True)
        r.check_returncode()
        self._run_dws(['snapshot', '-m', "'snapshot of notebook run'", 'S1'],
                      verbose=False)


if __name__ == '__main__':
    unittest.main()

