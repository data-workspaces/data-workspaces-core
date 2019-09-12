"""Utilities for running tests of the dws command line
"""

import shutil
import subprocess
import filecmp
import json
from os.path import join, abspath, expanduser, exists
import os
import unittest

TEMPDIR=abspath(expanduser(__file__)).replace('.py', '_data')
WS_DIR=join(TEMPDIR,'workspace')
WS_ORIGIN=join(TEMPDIR, 'workspace_origin.git')
OTHER_WS=join(TEMPDIR, 'workspace2')

try:
    import dataworkspaces
except ImportError:
    sys.path.append(os.path.abspath(".."))

from dataworkspaces.utils.git_utils import GIT_EXE_PATH
from dataworkspaces.utils.subprocess_utils import find_exe

class BaseCase(unittest.TestCase):
    """utilities to set up an environment that can has two copies of a workspace
    and a central bare git repo as the origin.
    """
    def setUp(self):
        if os.path.exists(TEMPDIR):
            shutil.rmtree(TEMPDIR)
        os.mkdir(TEMPDIR)
        os.mkdir(WS_DIR)
        self.dws=find_exe("dws", "Make sure you have enabled your python virtual environment")

    def tearDown(self):
        if os.path.exists(TEMPDIR):
            shutil.rmtree(TEMPDIR)

    def _run_dws(self, dws_args, cwd=WS_DIR, env=None):
        command = self.dws + ' --verbose --batch '+ ' '.join(dws_args)
        print(command + (' [%s]' % cwd))
        r = subprocess.run(command, cwd=cwd, shell=True, env=env)
        r.check_returncode()

    def _run_dws_with_input(self, dws_args, dws_input, cwd=WS_DIR, env=None):
        command = self.dws + ' --verbose '+ ' '.join(dws_args)
        print(command + (' [%s]' % cwd))
        print(" Input will be %s" % repr(dws_input))
        r = subprocess.run(command, cwd=cwd, input=dws_input, shell=True, env=env,
                           encoding='utf-8')
        r.check_returncode()

    def _run_git(self, git_args, cwd=WS_DIR):
        args = [GIT_EXE_PATH]+git_args
        print(' '.join(args) + (' [%s]' % cwd))
        r = subprocess.run(args, cwd=cwd)
        r.check_returncode()

    def _setup_initial_repo(self, create_resources=None):
        if create_resources is not None:
            self._run_dws(['init', '--create-resources='+create_resources], cwd=WS_DIR)
        else:
            self._run_dws(['init'], cwd=WS_DIR)
        self._run_git(['init', '--bare', 'workspace_origin.git'],
                      cwd=TEMPDIR)
        self._run_git(['remote', 'add', 'origin', WS_ORIGIN], cwd=WS_DIR)
        self._run_dws(['push'], cwd=WS_DIR)

    def _clone_second_repo(self):
        self._run_dws(['clone', WS_ORIGIN, 'workspace2'], cwd=TEMPDIR)

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

