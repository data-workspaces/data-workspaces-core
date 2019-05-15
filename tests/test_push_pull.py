"""
Test cases related to push and pull
"""

import unittest
import sys
import os
import os.path
from os.path import join, exists
import shutil
import subprocess
import filecmp
import json

TEMPDIR=os.path.abspath(os.path.expanduser(__file__)).replace('.py', '_data')
WS_DIR=join(TEMPDIR,'workspace')
WS_ORIGIN=join(TEMPDIR, 'workspace_origin.git')
CODE2_DIR=join(WS_DIR, 'code2')
OTHER_WS_PARENT=join(TEMPDIR, 'workspace2-parent')
OTHER_WS=join(OTHER_WS_PARENT, 'workspace')
OTHER_CODE2_DIR=join(OTHER_WS, 'code2')


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
        os.mkdir(OTHER_WS_PARENT)
        self.dws=find_exe("dws", "Make sure you have enabled your python virtual environment")

    def tearDown(self):
        if os.path.exists(TEMPDIR):
            shutil.rmtree(TEMPDIR)
        #pass

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

    def _get_resource_set(self, workspace_dir):
        resource_file = join(workspace_dir, '.dataworkspace/resources.json')
        with open(resource_file, 'r') as f:
            data = json.load(f)
        names = set()
        for obj in data:
            names.add(obj['name'])
        return names


class TestPushPull(BaseCase):
    def test_adding_git_subdirectory(self):
        # create a primary ws and the origin
        self._run_dws(['init',
                       '--create-resources=code'])
        self._run_git(['init', '--bare', 'workspace_origin.git'],
                      cwd=TEMPDIR)
        self._run_git(['remote', 'add', 'origin', WS_ORIGIN], cwd=WS_DIR)
        self._run_dws(['push'])
        # clone a copy
        self._run_dws(['clone', WS_ORIGIN], cwd=OTHER_WS_PARENT)
        # add a resource to the copy, create a file in the resource, and push
        os.mkdir(OTHER_CODE2_DIR)
        self._run_dws(['add', 'git', '--role=code', './code2'], cwd=OTHER_WS)
        other_ws_file = join(OTHER_CODE2_DIR, 'test.txt')
        with open(other_ws_file, 'w') as f:
            f.write("this is a test.\n")
        self._run_dws(['snapshot', 'S1'], cwd=OTHER_WS)
        self._run_dws(['push'], cwd=OTHER_WS)
        # pull back to the original workspace
        self._run_dws(['pull'], cwd=WS_DIR)
        # make sure the file was created with the same content
        self._assert_files_same(other_ws_file, join(CODE2_DIR, 'test.txt'))
        resources = self._get_resource_set(WS_DIR)
        self.assertEqual(resources, set(['code', 'code2']))

if __name__ == '__main__':
    unittest.main()

