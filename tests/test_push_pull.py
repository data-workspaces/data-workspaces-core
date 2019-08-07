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
from os.path import join

from utils_for_tests import BaseCase, TEMPDIR, WS_DIR, WS_ORIGIN, OTHER_WS

CODE2_DIR=join(WS_DIR, 'code2')
OTHER_CODE2_DIR=join(OTHER_WS, 'code2')



class TestPushPull(BaseCase):
    def test_adding_git_subdirectory(self):
        # create a primary ws and the origin
        self._setup_initial_repo(create_resources='code')
        # clone a copy
        self._clone_second_repo()
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

