"""
Test cases related to the local files resource
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


from utils_for_tests import BaseCase, TEMPDIR, WS_DIR, WS_ORIGIN, OTHER_WS

LOCAL_RESOURCE=join(WS_DIR, 'local-data')
DATA=join(LOCAL_RESOURCE, 'data.txt')

class TestLocalFiles(BaseCase):
    def test_local_files_resource(self):
        # create a primary ws, the origin, and the second ws
        self._setup_initial_repo(create_resources=None)
        self._clone_second_repo()
        os.makedirs(LOCAL_RESOURCE)
        with open(DATA, 'w') as f:
            f.write("testing\n")
        self._run_dws(['add', 'local-files', '--role', 'source-data', LOCAL_RESOURCE])
        self._run_dws(['snapshot', 'S1'], cwd=WS_DIR)
        # push and pull
        self._run_dws(['push'], cwd=WS_DIR)
        self._run_dws(['pull'], cwd=OTHER_WS)

    def test_local_path_override(self):
        # create a primary ws, the origin, and the second ws
        self._setup_initial_repo(create_resources=None)
        os.makedirs(LOCAL_RESOURCE)
        with open(DATA, 'w') as f:
            f.write("testing\n")
        # make a copy of the data resource
        LOCAL_RESOURCE_COPY=join(TEMPDIR, 'data-copy')
        shutil.copytree(LOCAL_RESOURCE, LOCAL_RESOURCE_COPY)
        self._run_dws(['add', 'local-files', '--role', 'source-data', LOCAL_RESOURCE])
        self._run_dws(['snapshot', 'S1'], cwd=WS_DIR)
        self._run_dws(['push'], cwd=WS_DIR)
        shutil.rmtree(WS_DIR) # remove the original to simulate a remote clone
        self._run_dws_with_input(['clone', WS_ORIGIN, 'workspace2'],
                                 dws_input='localhost\n%s\n'%LOCAL_RESOURCE_COPY,
                                 cwd=TEMPDIR)

if __name__ == '__main__':
    unittest.main()

