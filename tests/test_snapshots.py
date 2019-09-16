"""
Test cases related to snapshot and restore
"""


import unittest
import sys
import os
import os.path
from os.path import join, exists, isdir
import shutil
import subprocess
import json


# If set to True, by --keep-outputs option, leave the output data
KEEP_OUTPUTS=False

TEMPDIR=os.path.abspath(os.path.expanduser(__file__)).replace('.py', '_data')
WS_DIR=join(TEMPDIR,'workspace')
CODE_DIR=join(WS_DIR, 'code')
RESULTS_DIR=join(WS_DIR, 'results')

try:
    import dataworkspaces
except ImportError:
    sys.path.append(os.path.abspath(".."))

from dataworkspaces.utils.git_utils import GIT_EXE_PATH
from dataworkspaces.utils.subprocess_utils import find_exe

class BaseCase(unittest.TestCase):
    def setUp(self):
        if os.path.exists(TEMPDIR):
            shutil.rmtree(TEMPDIR)
        os.mkdir(TEMPDIR)
        os.mkdir(WS_DIR)
        self.dws=find_exe("dws", "Make sure you have enabled your python virtual environment")

    def tearDown(self):
        if os.path.exists(TEMPDIR) and not KEEP_OUTPUTS:
            shutil.rmtree(TEMPDIR)

    def _run_dws(self, dws_args, cwd=WS_DIR, env=None):
        command = self.dws + ' --verbose --batch '+ ' '.join(dws_args)
        print(command + (' [%s]' % cwd))
        r = subprocess.run(command, cwd=cwd, shell=True, env=env)
        r.check_returncode()

    def _run_git(self, git_args, cwd=WS_DIR):
        args = [GIT_EXE_PATH]+git_args
        print(' '.join(args) + (' [%s]' % cwd))
        r = subprocess.run(args, cwd=cwd)
        r.check_returncode()

    def _assert_files_same(self, f1, f2):
        self.assertTrue(exists(f1), "Missing file %s" % f1)
        self.assertTrue(exists(f2), "Missing file %s" % f2)
        self.assertTrue(filecmp.cmp(f1, f2, shallow=False),
                        "Files %s and %s are different" % (f1, f2))

    def _assert_file_contents(self, filepath, expected_contents):
        with open(filepath, 'r') as f:
            data = f.read()
        self.assertEqual(expected_contents, data, "File %s does not contain expected data"%filepath)

    def _write_results(self, metrics):
        with open(join(RESULTS_DIR, 'results.json'), 'w') as f:
            json.dump({'metrics':metrics}, f)

    def _assert_results(self, snapshot_reldir, metrics):
        results_file = join(join(RESULTS_DIR, 'snapshots/'+snapshot_reldir),
                            'results.json')
        self.assertTrue(exists(results_file))
        with open(results_file, 'r') as f:
            data = json.load(f)
            self.assertEqual(metrics, data['metrics'])

class TestSnapshots(BaseCase):
    def test_snapshot_no_tag(self):
        """Just a minimal test case without any tag. It turns out we
        were not testing this case!
        """
        self._run_dws(['init', '--hostname=test', '--create-resources=code,results'])
        with open(join(CODE_DIR, 'test.py'), 'w') as f:
            f.write("print('this is a test')\n")
        metrics={'accuracy':0.95}
        self._write_results(metrics)
        self._run_dws(['snapshot', '-m', "'test of snapshot with no tag'"])
        self._assert_results('test-001', metrics)

    def test_restore_short_hash(self):
        HASH='cdce6a5'
        self._run_dws(['init', '--create-resources=code,results'])
        with open(join(CODE_DIR, 'test.py'), 'w') as f:
            f.write("print('this is a test')\n")
        self._run_dws(['snapshot', '-m', "'test of restore short hash'", 'TAG1'])
        with open(join(CODE_DIR, 'test.py'), 'w') as f:
            f.write("print('this is a test for the second snapshot')\n")
        self._run_dws(['snapshot', '-m', "'second snapshot'", 'TAG2'])
        self._run_dws(['restore', HASH])
        self._assert_file_contents(join(CODE_DIR, 'test.py'),
                                   "print('this is a test')\n")


    def test_snapshot_with_duplicate_tag(self):
        """Test the case where we try to take a snapshot with a tag
        that already exists. This should be an error in batch mode.
        """
        self._run_dws(['init', '--create-resources=code,results'])
        with open(join(CODE_DIR, 'test.py'), 'w') as f:
            f.write("print('this is a test')\n")
        self._run_dws(['snapshot', '-m', "'first tag'", 'S1'])
        got_error = False
        try:
            print(">>> Re-use of snapshot tag in batch mode should fail:")
            self._run_dws(['snapshot', '-m', "'second tag'", 'S1'])
        except subprocess.CalledProcessError:
            got_error = True
        if not got_error:
            self.fail("Did not get an error when calling snapshot for tag S1 a second time")

class TestDeleteSnapshot(BaseCase):
    def test_delete_snapshot(self):
        self._run_dws(['init', '--hostname=test', '--create-resources=code,results'])
        with open(join(CODE_DIR, 'test.py'), 'w') as f:
            f.write("print('this is a test')\n")
        metrics={'accuracy':0.95}
        self._write_results(metrics)
        self._run_dws(['snapshot', '-m', "'test of snapshot to be deleted'",
                       'snapshot-tag'])
        self._assert_results('test-snapshot-tag', metrics)
        snapshot_dir = join(join(RESULTS_DIR, 'snapshots'), 'test-snapshot-tag')
        self.assertTrue(isdir(snapshot_dir))
        self._run_dws(['delete-snapshot', 'snapshot-tag'])
        self.assertFalse(isdir(snapshot_dir))

if __name__ == '__main__':
    if len(sys.argv)>1 and sys.argv[1]=='--keep-outputs':
        KEEP_OUTPUTS=True
        del sys.argv[1]
        print("--keep-outputs specified, will skip cleanup steps")
    unittest.main()
