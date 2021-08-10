"""
Test cases for when non-default branches are used for the workspace
and for git resources. This also covers the case for GitHub repos that
use 'main' instead of 'master'.
"""
import os
from os.path import join, exists
import shutil
import unittest

from dataworkspaces.utils.subprocess_utils import find_exe
from dataworkspaces.utils.git_utils import get_branch_info

from utils_for_tests import HelperMethods, TEMPDIR, WS_DIR, WS_ORIGIN, OTHER_WS

DWS_BRANCH_NAME = 'dws'

RESOURCE_BRANCH_NAME = 'resource'
RESOURCE_PATH=join(TEMPDIR, 'resource')
RESOURCE_ORIGIN=join(TEMPDIR, 'resource_origin.git')

class TestAlternativeBranchCases(HelperMethods, unittest.TestCase):
    """We change the branch 
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

    def _create_repo_with_branch(self, path, branch):
        if not exists(path):
            os.mkdir(path)
        self._run_git(['init'], cwd=path)
        with open(join(path, 'dummy_file.txt'), 'w') as f:
            f.write("create a file in repo to have something to commit\n")
        self._run_git(['add', 'dummy_file.txt'], cwd=path)
        self._run_git(['commit', '-m', 'initial version'], cwd=path)
        self._run_git(['branch', '-m', 'master', branch], cwd=path)
        (actual_branch, _) = get_branch_info(path)
        self.assertEqual(branch, actual_branch)
    
    def _setup_initial_repo(self, create_resources=None, scratch_dir=None, hostname=None):
        self._create_repo_with_branch(WS_DIR, DWS_BRANCH_NAME)
        self._run_dws(['init', '--create-resources', 'source-data'], cwd=WS_DIR)
        self._run_git(['init', '--bare', 'workspace_origin.git'],
                      cwd=TEMPDIR)
        self._run_git(['remote', 'add', 'origin', WS_ORIGIN], cwd=WS_DIR)
        self._run_dws(['push'], cwd=WS_DIR)
        # on the origin, set the default branch to 'dws' instead of 'master'
        self._run_git(['symbolic-ref', 'HEAD', 'refs/heads/'+DWS_BRANCH_NAME],
                      cwd=WS_ORIGIN)

    def _setup_git_resource(self):
        """Create a repo on the branch 'resource' that has an origin configured.
        We then add this to the dws workspaces as a resource
        """
        self._create_repo_with_branch(RESOURCE_PATH, RESOURCE_BRANCH_NAME)
        os.mkdir(RESOURCE_ORIGIN)
        self._run_git(['init', '--bare', 'resource_origin.git'],
                      cwd=TEMPDIR)
        self._run_git(['remote', 'add', 'origin', RESOURCE_ORIGIN], cwd=RESOURCE_PATH)
        self._run_git(['push', 'origin', RESOURCE_BRANCH_NAME], cwd=RESOURCE_PATH)
        # on the origin, set the default branch to RESOURCE_BRNACH_NAME instead of 'master'
        self._run_git(['symbolic-ref', 'HEAD', 'refs/heads/'+RESOURCE_BRANCH_NAME],
                      cwd=RESOURCE_ORIGIN)
        self._run_dws(['add', 'git', '--role', 'source-data', RESOURCE_PATH])


    def _clone_second_repo(self):
        self._run_dws(['clone', WS_ORIGIN, 'workspace2'], cwd=TEMPDIR)

    def test_branch_for_dws_repo(self):
        self._setup_initial_repo(create_resources='source-data')
        DATA_FILE=join(WS_DIR, 'source-data/data.txt')
        with open(DATA_FILE, 'w') as f:
            f.write("1,2,3,4\n")
        self._setup_git_resource()
        self._run_dws(['snapshot', 'TAG1'])
        self._run_dws(['push'])
        self._clone_second_repo()
        # validate that the cloned workspace has the correct branch and that the
        # git resource in the cloned workspace also has its correct branch
        (branch, _) = get_branch_info(OTHER_WS)
        self.assertEqual(DWS_BRANCH_NAME, branch)
        (resource_branch, _) = get_branch_info(join(OTHER_WS, 'resource'))
        self.assertEqual(RESOURCE_BRANCH_NAME, resource_branch)

if __name__ == '__main__':
    unittest.main()
