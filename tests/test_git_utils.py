#!/usr/bin/env python3
# Copyright 2018,2019 by MPI-SWS and Data-ken Research. Licensed under Apache 2.0. See LICENSE.txt.
"""Test git utilities like autocommit.
"""

import unittest
import sys
import os
import os.path
from os.path import join, exists
import shutil
import subprocess

TEMPDIR=os.path.abspath(os.path.expanduser(__file__)).replace('.py', '_data')

try:
    import dataworkspaces
except ImportError:
    sys.path.append(os.path.abspath(".."))

from dataworkspaces.utils.git_utils import \
    is_git_dirty, is_git_subdir_dirty, is_git_staging_dirty,\
    commit_changes_in_repo, checkout_and_apply_commit,\
    get_local_head_hash, commit_changes_in_repo_subdir,\
    checkout_subdir_and_apply_commit, GIT_EXE_PATH


def makefile(relpath, contents):
    with open(join(TEMPDIR, relpath), 'w') as f:
        f.write(contents)


class BaseCase(unittest.TestCase):
    def setUp(self):
        if os.path.exists(TEMPDIR):
            shutil.rmtree(TEMPDIR)
        os.mkdir(TEMPDIR)
        self._run(['init'])

    def tearDown(self):
        if os.path.exists(TEMPDIR):
            shutil.rmtree(TEMPDIR)

    def _run(self, git_args, cwd=TEMPDIR):
        args = [GIT_EXE_PATH]+git_args
        print(' '.join(args) + (' [%s]' % cwd))
        r = subprocess.run(args, cwd=cwd)
        r.check_returncode()

    def _git_add(self, files):
        for fname in files:
            self._run(['add', fname])

    def assert_file_exists(self, relpath):
        self.assertTrue(exists(join(TEMPDIR, relpath)),
                        "Missing file %s" % join(TEMPDIR, relpath))

    def assert_file_not_exists(self, relpath):
        self.assertFalse(exists(join(TEMPDIR, relpath)),
                         "File %s exists, but nsould not" % join(TEMPDIR, relpath))

    def assert_file_contents_equal(self, relpath, contents):
        with open(join(TEMPDIR, relpath), 'r') as f:
            data = f.readlines()
        exp_lines = contents.split('\n')
        self.assertEqual(len(exp_lines), len(data),
                         "Incorrect length for file %s, contents were: %s, expected was %s" %
                         (relpath, repr(data), repr(exp_lines)))
        for i in range(len(exp_lines)):
            exp = exp_lines[i].strip()
            got = data[i].strip()
            self.assertEqual(exp, got, "File %s has a different on line %s: %s"%
                             (relpath, i, got))

class TestIsDirty(BaseCase):
    """Tests for is_git_dirty() and is_git_subdir_dirty(). We consider the repo dirty
    if there are untracked files, modified/added/deleted files, or files in staging.
    """
    def setUp(self):
        super().setUp()
        os.mkdir(join(TEMPDIR, 'subdir'))
        makefile("subdir/to_be_deleted.txt", "this file will be deleted")
        makefile("subdir/to_be_kept.txt", "This file will be kept")
        makefile("ignored_in_root.txt", "This is left in the root and ignored")
        self._git_add(['subdir/to_be_deleted.txt', 'subdir/to_be_kept.txt',
                       'ignored_in_root.txt'])
        self._run(['commit', '-m', 'initial version'])

    def test_git_is_dirty_clean(self):
        self.assertFalse(is_git_dirty(TEMPDIR))
        self.assertFalse(is_git_subdir_dirty(TEMPDIR, 'subdir'))
        self.assertFalse(is_git_staging_dirty(TEMPDIR))
        self.assertFalse(is_git_staging_dirty(TEMPDIR, 'subdir'))

    def test_git_is_dirty_untracked(self):
        makefile('subdir/untracked.txt', 'this is untracked')
        self.assertTrue(is_git_dirty(TEMPDIR))
        self.assertTrue(is_git_subdir_dirty(TEMPDIR, 'subdir'))
        self.assertFalse(is_git_staging_dirty(TEMPDIR))
        self.assertFalse(is_git_staging_dirty(TEMPDIR, 'subdir'))

    def test_git_is_dirty_added(self):
        makefile('subdir/added.txt', 'this will be added')
        self._git_add(['subdir/added.txt'])
        self.assertTrue(is_git_dirty(TEMPDIR))
        self.assertTrue(is_git_subdir_dirty(TEMPDIR, 'subdir'))
        self.assertTrue(is_git_staging_dirty(TEMPDIR))
        self.assertTrue(is_git_staging_dirty(TEMPDIR, 'subdir'))

    def test_git_is_dirty_modified(self):
        with open(join(TEMPDIR, 'subdir/to_be_kept.txt'), 'a') as f:
            f.write("\nmore content\n")
        self.assertTrue(is_git_dirty(TEMPDIR))
        self.assertTrue(is_git_subdir_dirty(TEMPDIR, 'subdir'))
        self.assertFalse(is_git_staging_dirty(TEMPDIR))
        self.assertFalse(is_git_staging_dirty(TEMPDIR, 'subdir'))

    def test_git_is_dirty_modified_and_added(self):
        with open(join(TEMPDIR, 'subdir/to_be_kept.txt'), 'a') as f:
            f.write("\nmore content\n")
        self._git_add(['subdir/to_be_kept.txt'])
        self.assertTrue(is_git_dirty(TEMPDIR))
        self.assertTrue(is_git_subdir_dirty(TEMPDIR, 'subdir'))
        self.assertTrue(is_git_staging_dirty(TEMPDIR))
        self.assertTrue(is_git_staging_dirty(TEMPDIR, 'subdir'))

    def test_git_is_dirty_deleted(self):
        os.remove(join(TEMPDIR, 'subdir/to_be_deleted.txt'))
        self.assertTrue(is_git_dirty(TEMPDIR))
        self.assertTrue(is_git_subdir_dirty(TEMPDIR, 'subdir'))
        self.assertFalse(is_git_staging_dirty(TEMPDIR))
        self.assertFalse(is_git_staging_dirty(TEMPDIR, 'subdir'))

    def test_git_is_dirty_deleted_in_staging(self):
        self._run(['rm', 'subdir/to_be_deleted.txt'])
        self.assertTrue(is_git_dirty(TEMPDIR))
        self.assertTrue(is_git_subdir_dirty(TEMPDIR, 'subdir'))
        self.assertTrue(is_git_staging_dirty(TEMPDIR))
        self.assertTrue(is_git_staging_dirty(TEMPDIR, 'subdir'))

    def test_git_subdir_is_dirty_untracked_outside(self):
        makefile('untracked.txt', 'this is untracked')
        self.assertFalse(is_git_subdir_dirty(TEMPDIR, 'subdir'))
        self.assertTrue(is_git_dirty(TEMPDIR))
        self.assertFalse(is_git_staging_dirty(TEMPDIR))
        self.assertFalse(is_git_staging_dirty(TEMPDIR, 'subdir'))

    def test_git_subdir_is_dirty_added_outside(self):
        makefile('added.txt', 'this will be added')
        self._git_add(['added.txt'])
        self.assertFalse(is_git_subdir_dirty(TEMPDIR, 'subdir'))
        self.assertTrue(is_git_dirty(TEMPDIR))
        self.assertTrue(is_git_staging_dirty(TEMPDIR))
        self.assertFalse(is_git_staging_dirty(TEMPDIR, 'subdir'))

    def test_git_subdir_is_dirty_modified_outside(self):
        with open(join(TEMPDIR, 'ignored_in_root.txt'), 'a') as f:
            f.write("\nmore content\n")
        self.assertFalse(is_git_subdir_dirty(TEMPDIR, 'subdir'))
        self.assertTrue(is_git_dirty(TEMPDIR))
        self.assertFalse(is_git_staging_dirty(TEMPDIR))
        self.assertFalse(is_git_staging_dirty(TEMPDIR, 'subdir'))

    def test_git_subdir_is_dirty_modified_and_added_outside(self):
        with open(join(TEMPDIR, 'ignored_in_root.txt'), 'a') as f:
            f.write("\nmore content\n")
        self._git_add(['ignored_in_root.txt'])
        self.assertFalse(is_git_subdir_dirty(TEMPDIR, 'subdir'))
        self.assertTrue(is_git_dirty(TEMPDIR))
        self.assertTrue(is_git_staging_dirty(TEMPDIR))
        self.assertFalse(is_git_staging_dirty(TEMPDIR, 'subdir'))

    def test_git_subdir_is_dirty_deleted_outside(self):
        os.remove(join(TEMPDIR, 'ignored_in_root.txt'))
        self.assertFalse(is_git_subdir_dirty(TEMPDIR, 'subdir'))
        self.assertTrue(is_git_dirty(TEMPDIR))
        self.assertFalse(is_git_staging_dirty(TEMPDIR))
        self.assertFalse(is_git_staging_dirty(TEMPDIR, 'subdir'))

    def test_git_subdir_is_dirty_deleted_in_staging_outside(self):
        self._run(['rm', 'ignored_in_root.txt'])
        self.assertFalse(is_git_subdir_dirty(TEMPDIR, 'subdir'))
        self.assertTrue(is_git_dirty(TEMPDIR))
        self.assertTrue(is_git_staging_dirty(TEMPDIR))
        self.assertFalse(is_git_staging_dirty(TEMPDIR, 'subdir'))


class TestCommit(BaseCase):
    def test_commit(self):
        makefile('to_be_deleted.txt', 'this file will be deleted')
        makefile('to_be_left_alone.txt', 'this file to be left alone')
        makefile('to_be_modified.txt', 'this file to be modified')
        self._git_add(['to_be_deleted.txt', 'to_be_left_alone.txt',
                       'to_be_modified.txt'])
        self._run(['commit', '-m', 'initial version'])
        os.remove(join(TEMPDIR, 'to_be_deleted.txt'))
        with open(join(TEMPDIR, 'to_be_modified.txt'), 'a') as f:
            f.write("Adding another line to file!\n")
        makefile('to_be_added.txt', 'this file was added')
        commit_changes_in_repo(TEMPDIR, 'testing applied changes',
                               verbose=True)
        self.assertFalse(is_git_dirty(TEMPDIR), "Git still dirty after commit!")
        self.assert_file_exists('to_be_left_alone.txt')
        self.assert_file_exists('to_be_modified.txt')
        self.assert_file_exists('to_be_added.txt')
        self.assert_file_not_exists('to_be_deleted.txt')


class TestCheckoutAndApplyCommit(BaseCase):
    def test_checkout_and_apply_commit(self):
        # First, do all the setup
        makefile('to_be_deleted.txt', 'this file will be deleted')
        makefile('to_be_left_alone.txt', 'this file to be left alone')
        makefile('to_be_modified.txt', 'this file to be modified\n')
        self._git_add(['to_be_deleted.txt', 'to_be_left_alone.txt',
                       'to_be_modified.txt'])
        self._run(['commit', '-m', 'initial version'])
        initial_hash = get_local_head_hash(TEMPDIR, True)
        self._run(['rm', 'to_be_deleted.txt'])
        with open(join(TEMPDIR, 'to_be_modified.txt'), 'a') as f:
            f.write("Adding another line to file!\n")
        makefile('to_be_added.txt', 'this file was added')
        self._git_add(['to_be_modified.txt', 'to_be_added.txt'])
        self._run(['commit', '-m', 'second version'])
        second_hash = get_local_head_hash(TEMPDIR, True)
        makefile('added_in_third_commit.txt', 'added in third commit')
        with open(join(TEMPDIR, 'to_be_modified.txt'), 'a') as f:
            f.write("Adding a third to file!\n")
        self._git_add(['added_in_third_commit.txt', 'to_be_modified.txt'])
        self._run(['commit', '-m', 'third version'])
        third_hash = get_local_head_hash(TEMPDIR, True)

        # now, revert back to the first commit
        checkout_and_apply_commit(TEMPDIR, initial_hash, True)
        self.assertFalse(is_git_dirty(TEMPDIR), "Git still dirty after commit!")
        self.assert_file_exists("to_be_deleted.txt")
        self.assert_file_not_exists("to_be_added.txt")
        self.assert_file_not_exists("added_in_third_commit.txt")
        self.assert_file_contents_equal("to_be_modified.txt",
                                        "this file to be modified")
        restored_hash = get_local_head_hash(TEMPDIR, True)
        self.assertNotEqual(initial_hash, restored_hash) # should be differemt

        # add a commit at this point
        with open(join(TEMPDIR, 'to_be_modified.txt'), 'a') as f:
            f.write("Overwritten after restoring first commit.")
        self._git_add(['to_be_modified.txt'])
        self._run(['commit', '-m', 'branch off first version'])

        # restore to third version
        checkout_and_apply_commit(TEMPDIR, third_hash, True)
        self.assertFalse(is_git_dirty(TEMPDIR), "Git still dirty after commit!")
        self.assert_file_exists("added_in_third_commit.txt")
        self.assert_file_contents_equal("to_be_modified.txt",
                                        'this file to be modified\n'+
                                        "Adding another line to file!\n"+
                                        "Adding a third to file!")
        self.assert_file_not_exists("to_be_deleted.txt")

        # revert to restored hash and verify changes
        checkout_and_apply_commit(TEMPDIR, restored_hash, True)
        self.assertFalse(is_git_dirty(TEMPDIR), "Git still dirty after commit!")
        self.assert_file_exists("to_be_deleted.txt")
        self.assert_file_not_exists("to_be_added.txt")
        self.assert_file_not_exists("added_in_third_commit.txt")
        self.assert_file_contents_equal("to_be_modified.txt",
                                        "this file to be modified")

        # run again with the same hash. It should do nothing, as there
        # are no changes
        checkout_and_apply_commit(TEMPDIR, restored_hash, True)


class TestSubdirCommit(BaseCase):
    def test_commit(self):
        os.mkdir(join(TEMPDIR, 'subdir'))
        makefile('subdir/to_be_deleted.txt', 'this file will be deleted')
        makefile('subdir/to_be_left_alone.txt', 'this file to be left alone')
        makefile('subdir/to_be_modified.txt', 'this file to be modified')
        makefile('root_file1.txt', 'this should not be changed')
        self._git_add(['subdir/to_be_deleted.txt', 'subdir/to_be_left_alone.txt',
                       'subdir/to_be_modified.txt',
                       'root_file1.txt'])
        self._run(['commit', '-m', 'initial version'])
        os.remove(join(TEMPDIR, 'subdir/to_be_deleted.txt'))
        with open(join(TEMPDIR, 'subdir/to_be_modified.txt'), 'a') as f:
            f.write("Adding another line to file!\n")
        makefile('subdir/to_be_added.txt', 'this file was added')
        commit_changes_in_repo_subdir(TEMPDIR, 'subdir', 'testing applied changes',
                                      verbose=True)
        self.assertFalse(is_git_dirty(TEMPDIR), "Git still dirty after commit!")
        self.assert_file_exists('subdir/to_be_left_alone.txt')
        self.assert_file_exists('subdir/to_be_modified.txt')
        self.assert_file_exists('subdir/to_be_added.txt')
        self.assert_file_not_exists('subdir/to_be_deleted.txt')
        self.assert_file_exists('root_file1.txt')
        # verify that staged files outside of the subdir are not changed
        makefile('staged_but_not_committed.txt', 'should be staged but not committed')
        self._git_add(['staged_but_not_committed.txt'])
        commit_changes_in_repo_subdir(TEMPDIR, 'subdir', 'testing not committing',
                                      verbose=True)
        self.assertFalse(is_git_subdir_dirty(TEMPDIR, 'subdir'))
        self.assertTrue(is_git_dirty(TEMPDIR))
        self.assertTrue(is_git_staging_dirty(TEMPDIR))
        self.assertFalse(is_git_staging_dirty(TEMPDIR, 'subdir'))


class TestCheckoutSubdirAndApplyCommit(BaseCase):
    def test_checkout_and_apply_commit(self):
        # First, do all the setup
        os.mkdir(join(TEMPDIR, 'subdir'))
        makefile('subdir/to_be_deleted.txt', 'this file will be deleted')
        makefile('subdir/to_be_left_alone.txt', 'this file to be left alone')
        makefile('subdir/to_be_modified.txt', 'this file to be modified\n')
        makefile('root_file1.txt', 'root file v1\n')
        self._git_add(['subdir/to_be_deleted.txt', 'subdir/to_be_left_alone.txt',
                       'subdir/to_be_modified.txt', 'root_file1.txt'])
        self._run(['commit', '-m', 'initial version'])
        initial_hash = get_local_head_hash(TEMPDIR, True)

        self._run(['rm', 'subdir/to_be_deleted.txt'])
        with open(join(TEMPDIR, 'subdir/to_be_modified.txt'), 'a') as f:
            f.write("Adding another line to file!\n")
        makefile('subdir/to_be_added.txt', 'this file was added')
        with open(join(TEMPDIR, 'root_file1.txt'), 'a') as f:
            f.write("root file v2")
        self._git_add(['subdir/to_be_modified.txt', 'subdir/to_be_added.txt',
                       'root_file1.txt'])
        self._run(['commit', '-m', 'second version'])
        second_hash = get_local_head_hash(TEMPDIR, True)

        makefile('subdir/added_in_third_commit.txt', 'added in third commit')
        with open(join(TEMPDIR, 'subdir/to_be_modified.txt'), 'a') as f:
            f.write("Adding a third to file!\n")
        self._git_add(['subdir/added_in_third_commit.txt', 'subdir/to_be_modified.txt'])
        self._run(['commit', '-m', 'third version'])
        third_hash = get_local_head_hash(TEMPDIR, True)

        # now, revert back to the first commit
        checkout_subdir_and_apply_commit(TEMPDIR, 'subdir', initial_hash, True)
        self.assertFalse(is_git_dirty(TEMPDIR), "Git still dirty after commit!")
        self.assert_file_exists("subdir/to_be_deleted.txt")
        self.assert_file_not_exists("subdir/to_be_added.txt")
        self.assert_file_not_exists("subdir/added_in_third_commit.txt")
        self.assert_file_contents_equal("subdir/to_be_modified.txt",
                                        "this file to be modified")
        self.assert_file_contents_equal("root_file1.txt",
                                        "root file v1\nroot file v2")
        restored_hash = get_local_head_hash(TEMPDIR, True)
        self.assertNotEqual(initial_hash, restored_hash) # should be differemt

        # add a commit at this point
        with open(join(TEMPDIR, 'subdir/to_be_modified.txt'), 'a') as f:
            f.write("Overwritten after restoring first commit.")
        self._git_add(['subdir/to_be_modified.txt'])
        self._run(['commit', '-m', 'branch off first version'])

        # restore to third version
        checkout_subdir_and_apply_commit(TEMPDIR, 'subdir', third_hash, True)
        self.assertFalse(is_git_dirty(TEMPDIR), "Git still dirty after commit!")
        self.assert_file_exists("subdir/added_in_third_commit.txt")
        self.assert_file_contents_equal("subdir/to_be_modified.txt",
                                        'this file to be modified\n'+
                                        "Adding another line to file!\n"+
                                        "Adding a third to file!")
        self.assert_file_not_exists("subdir/to_be_deleted.txt")
        self.assert_file_contents_equal("root_file1.txt",
                                        "root file v1\nroot file v2")

        # revert to restored hash and verify changes
        checkout_subdir_and_apply_commit(TEMPDIR, 'subdir', restored_hash, True)
        self.assertFalse(is_git_dirty(TEMPDIR), "Git still dirty after commit!")
        self.assert_file_exists("subdir/to_be_deleted.txt")
        self.assert_file_not_exists("subdir/to_be_added.txt")
        self.assert_file_not_exists("subdir/added_in_third_commit.txt")
        self.assert_file_contents_equal("subdir/to_be_modified.txt",
                                        "this file to be modified")
        self.assert_file_contents_equal("root_file1.txt",
                                        "root file v1\nroot file v2")

        # run again with the same hash. It should do nothing, as there
        # are no changes
        checkout_subdir_and_apply_commit(TEMPDIR, 'subdir', restored_hash, True)


if __name__ == '__main__':
    unittest.main()

