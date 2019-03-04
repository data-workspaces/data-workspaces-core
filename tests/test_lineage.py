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
WSDIR=join(TEMPDIR, 'workspace')

class TestLineage(unittest.TestCase):
    def setUp(self):
        if exists(TEMPDIR):
            shutil.rmtree(TEMPDIR)
        os.mkdir(TEMPDIR)
        os.mkdir(WSDIR)
        self.dws=find_exe("dws", "Make sure you have enabled your python virtual environment")
        self._run_dws(['init', '--use-basic-resource-template'],
                      verbose=False)
        with open(join(WSDIR, 'source-data/data.csv'), 'w') as f:
            f.write('a,b,c\n')
            f.write('1,2,3\n')

    def _run_dws(self, dws_args, cwd=WSDIR, env=None, verbose=True):
        if verbose:
            command = self.dws + ' --verbose --batch '+ ' '.join(dws_args)
        else:
            command = self.dws + ' --batch '+ ' '.join(dws_args)
        print(command + (' [%s]' % cwd))
        r = subprocess.run(command, cwd=cwd, shell=True, env=env)
        r.check_returncode()

    def _run_step(self, script_name, args):
        command = [sys.executable, join(TEST_DIR, script_name)]+args
        print(" ".join(command))
        r = subprocess.run(command, cwd=WSDIR, shell=False)
        r.check_returncode()

    def tearDown(self):
        if exists(TEMPDIR):
            shutil.rmtree(TEMPDIR)
        #pass

    def test_lineage(self):
        self._run_step('lineage_step1.py', [])
        self._run_step('lineage_step2.py', [])
        self._run_dws(['snapshot', 'S1'])

if __name__ == '__main__':
    unittest.main()
