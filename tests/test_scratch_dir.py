"""
Test cases for the scratch directory functionality
"""
import os
from os.path import isdir, join
import sys
import unittest
from subprocess import CalledProcessError

from utils_for_tests import BaseCase, WS_DIR, OTHER_WS, TEMPDIR


# If set to True, by --keep-outputs option, leave the output data
KEEP_OUTPUTS=False

class TestScratchDir(BaseCase):
    def test_default_scratch(self):
        self._setup_initial_repo()
        scratch_dir=join(WS_DIR, 'scratch')
        self.assertTrue(isdir(scratch_dir), "Scratch directory %s does not exist" % scratch_dir)
        self._clone_second_repo()
        other_scratch_dir = join(OTHER_WS, 'scratch')
        self.assertTrue(isdir(other_scratch_dir), "Cloned scratch directory %s does not exist" % other_scratch_dir)

    def test_scratch_files_not_in_snapshot(self):
        """Make sure that files in the scratch directory are properly
        gitignore'd
        """
        self._setup_initial_repo(create_resources='code')
        scratch_dir=join(WS_DIR, 'scratch')
        self.assertTrue(isdir(scratch_dir), "Scratch directory %s does not exist" % scratch_dir)
        f1path = join(scratch_dir, 'f1.txt')
        with open(f1path, 'w') as f:
            f.write("this file should not be in git\n")
        cpdir = join(scratch_dir, 'checkpoints')
        os.mkdir(cpdir)
        f2path = join(cpdir, 'f2.index')
        with open(f2path, 'w') as f:
            f.write("this file should also not be in git\n")
        codefilepath = join(join(WS_DIR, 'code'), 'foo.py')
        with open(codefilepath, 'w') as f:
            f.write("print('this is a test')\n")
        self._run_dws(['snapshot', 's1'])
        self._assert_file_git_tracked(codefilepath)
        self._assert_file_not_git_tracked(f1path)
        self._assert_file_not_git_tracked(f2path)

    def test_scratch_in_subdirectory(self):
        scratch_dir = join(WS_DIR, 'scratch_parent/scratch')
        self._setup_initial_repo(scratch_dir=scratch_dir)
        self.assertTrue(isdir(scratch_dir), "Scratch directory %s does not exist" % scratch_dir)
        self._clone_second_repo()
        other_scratch_dir = join(OTHER_WS, 'scratch_parent/scratch')
        self.assertTrue(isdir(other_scratch_dir), "Cloned scratch directory %s does not exist" % other_scratch_dir)

    def test_scratch_outside_of_workspace(self):
        scratch_dir = join(TEMPDIR, 'abs_scratch_parent/scratch')
        self._setup_initial_repo(scratch_dir=scratch_dir)
        self.assertTrue(isdir(scratch_dir), "Scratch directory %s does not exist" % scratch_dir)
        # TODO: We cannot test the absolute file case in the clone, as it requires interactive
        # user input. For now, we just verify that the clone fails.
        with self.assertRaises(CalledProcessError) as context:
            self._clone_second_repo()


if __name__ == '__main__':
    if len(sys.argv)>1 and sys.argv[1]=='--keep-outputs':
        KEEP_OUTPUTS=True
        del sys.argv[1]
        print("--keep-outputs specified, will skip cleanup steps")
    unittest.main()
