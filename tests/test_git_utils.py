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
REPODIR=join(TEMPDIR, 'repo')

try:
    import dataworkspaces
except ImportError:
    sys.path.append(os.path.abspath(".."))

from dataworkspaces.utils.file_utils import get_subpath_from_absolute
from dataworkspaces.utils.git_utils import \
    is_git_dirty, is_git_subdir_dirty, is_git_staging_dirty,\
    commit_changes_in_repo, checkout_and_apply_commit,\
    get_local_head_hash, commit_changes_in_repo_subdir,\
    checkout_subdir_and_apply_commit, GIT_EXE_PATH,\
    get_subdirectory_hash, get_json_file_from_remote,\
    git_remove_subtree, git_remove_file


def makefile(relpath, contents):
    with open(join(REPODIR, relpath), 'w') as f:
        f.write(contents)


class BaseCase(unittest.TestCase):
    def setUp(self):
        if os.path.exists(TEMPDIR):
            shutil.rmtree(TEMPDIR)
        os.mkdir(TEMPDIR)
        os.mkdir(REPODIR)
        self._run(['init'])

    def tearDown(self):
        if os.path.exists(TEMPDIR):
            shutil.rmtree(TEMPDIR)

    def _run(self, git_args, cwd=REPODIR):
        args = [GIT_EXE_PATH]+git_args
        print(' '.join(args) + (' [%s]' % cwd))
        r = subprocess.run(args, cwd=cwd)
        r.check_returncode()

    def _git_add(self, files):
        for fname in files:
            self._run(['add', fname])

    def assert_file_exists(self, relpath):
        self.assertTrue(exists(join(REPODIR, relpath)),
                        "Missing file %s" % join(REPODIR, relpath))

    def assert_file_not_exists(self, relpath):
        self.assertFalse(exists(join(REPODIR, relpath)),
                         "File %s exists, but nsould not" % join(REPODIR, relpath))

    def assert_file_contents_equal(self, relpath, contents):
        with open(join(REPODIR, relpath), 'r') as f:
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
        os.mkdir(join(REPODIR, 'subdir'))
        makefile("subdir/to_be_deleted.txt", "this file will be deleted")
        makefile("subdir/to_be_kept.txt", "This file will be kept")
        makefile("ignored_in_root.txt", "This is left in the root and ignored")
        self._git_add(['subdir/to_be_deleted.txt', 'subdir/to_be_kept.txt',
                       'ignored_in_root.txt'])
        self._run(['commit', '-m', 'initial version'])

    def test_git_is_dirty_clean(self):
        self.assertFalse(is_git_dirty(REPODIR))
        self.assertFalse(is_git_subdir_dirty(REPODIR, 'subdir'))
        self.assertFalse(is_git_staging_dirty(REPODIR))
        self.assertFalse(is_git_staging_dirty(REPODIR, 'subdir'))

    def test_git_is_dirty_untracked(self):
        makefile('subdir/untracked.txt', 'this is untracked')
        self.assertTrue(is_git_dirty(REPODIR))
        self.assertTrue(is_git_subdir_dirty(REPODIR, 'subdir'))
        self.assertFalse(is_git_staging_dirty(REPODIR))
        self.assertFalse(is_git_staging_dirty(REPODIR, 'subdir'))

    def test_git_is_dirty_added(self):
        makefile('subdir/added.txt', 'this will be added')
        self._git_add(['subdir/added.txt'])
        self.assertTrue(is_git_dirty(REPODIR))
        self.assertTrue(is_git_subdir_dirty(REPODIR, 'subdir'))
        self.assertTrue(is_git_staging_dirty(REPODIR))
        self.assertTrue(is_git_staging_dirty(REPODIR, 'subdir'))

    def test_git_is_dirty_modified(self):
        with open(join(REPODIR, 'subdir/to_be_kept.txt'), 'a') as f:
            f.write("\nmore content\n")
        self.assertTrue(is_git_dirty(REPODIR))
        self.assertTrue(is_git_subdir_dirty(REPODIR, 'subdir'))
        self.assertFalse(is_git_staging_dirty(REPODIR))
        self.assertFalse(is_git_staging_dirty(REPODIR, 'subdir'))

    def test_git_is_dirty_modified_and_added(self):
        with open(join(REPODIR, 'subdir/to_be_kept.txt'), 'a') as f:
            f.write("\nmore content\n")
        self._git_add(['subdir/to_be_kept.txt'])
        self.assertTrue(is_git_dirty(REPODIR))
        self.assertTrue(is_git_subdir_dirty(REPODIR, 'subdir'))
        self.assertTrue(is_git_staging_dirty(REPODIR))
        self.assertTrue(is_git_staging_dirty(REPODIR, 'subdir'))

    def test_git_is_dirty_deleted(self):
        os.remove(join(REPODIR, 'subdir/to_be_deleted.txt'))
        self.assertTrue(is_git_dirty(REPODIR))
        self.assertTrue(is_git_subdir_dirty(REPODIR, 'subdir'))
        self.assertFalse(is_git_staging_dirty(REPODIR))
        self.assertFalse(is_git_staging_dirty(REPODIR, 'subdir'))

    def test_git_is_dirty_deleted_in_staging(self):
        self._run(['rm', 'subdir/to_be_deleted.txt'])
        self.assertTrue(is_git_dirty(REPODIR))
        self.assertTrue(is_git_subdir_dirty(REPODIR, 'subdir'))
        self.assertTrue(is_git_staging_dirty(REPODIR))
        self.assertTrue(is_git_staging_dirty(REPODIR, 'subdir'))

    def test_git_subdir_is_dirty_untracked_outside(self):
        makefile('untracked.txt', 'this is untracked')
        self.assertFalse(is_git_subdir_dirty(REPODIR, 'subdir'))
        self.assertTrue(is_git_dirty(REPODIR))
        self.assertFalse(is_git_staging_dirty(REPODIR))
        self.assertFalse(is_git_staging_dirty(REPODIR, 'subdir'))

    def test_git_subdir_is_dirty_added_outside(self):
        makefile('added.txt', 'this will be added')
        self._git_add(['added.txt'])
        self.assertFalse(is_git_subdir_dirty(REPODIR, 'subdir'))
        self.assertTrue(is_git_dirty(REPODIR))
        self.assertTrue(is_git_staging_dirty(REPODIR))
        self.assertFalse(is_git_staging_dirty(REPODIR, 'subdir'))

    def test_git_subdir_is_dirty_modified_outside(self):
        with open(join(REPODIR, 'ignored_in_root.txt'), 'a') as f:
            f.write("\nmore content\n")
        self.assertFalse(is_git_subdir_dirty(REPODIR, 'subdir'))
        self.assertTrue(is_git_dirty(REPODIR))
        self.assertFalse(is_git_staging_dirty(REPODIR))
        self.assertFalse(is_git_staging_dirty(REPODIR, 'subdir'))

    def test_git_subdir_is_dirty_modified_and_added_outside(self):
        with open(join(REPODIR, 'ignored_in_root.txt'), 'a') as f:
            f.write("\nmore content\n")
        self._git_add(['ignored_in_root.txt'])
        self.assertFalse(is_git_subdir_dirty(REPODIR, 'subdir'))
        self.assertTrue(is_git_dirty(REPODIR))
        self.assertTrue(is_git_staging_dirty(REPODIR))
        self.assertFalse(is_git_staging_dirty(REPODIR, 'subdir'))

    def test_git_subdir_is_dirty_deleted_outside(self):
        os.remove(join(REPODIR, 'ignored_in_root.txt'))
        self.assertFalse(is_git_subdir_dirty(REPODIR, 'subdir'))
        self.assertTrue(is_git_dirty(REPODIR))
        self.assertFalse(is_git_staging_dirty(REPODIR))
        self.assertFalse(is_git_staging_dirty(REPODIR, 'subdir'))

    def test_git_subdir_is_dirty_deleted_in_staging_outside(self):
        self._run(['rm', 'ignored_in_root.txt'])
        self.assertFalse(is_git_subdir_dirty(REPODIR, 'subdir'))
        self.assertTrue(is_git_dirty(REPODIR))
        self.assertTrue(is_git_staging_dirty(REPODIR))
        self.assertFalse(is_git_staging_dirty(REPODIR, 'subdir'))


class TestCommit(BaseCase):
    def test_commit(self):
        makefile('to_be_deleted.txt', 'this file will be deleted')
        makefile('to_be_left_alone.txt', 'this file to be left alone')
        makefile('to_be_modified.txt', 'this file to be modified')
        self._git_add(['to_be_deleted.txt', 'to_be_left_alone.txt',
                       'to_be_modified.txt'])
        self._run(['commit', '-m', 'initial version'])
        os.remove(join(REPODIR, 'to_be_deleted.txt'))
        with open(join(REPODIR, 'to_be_modified.txt'), 'a') as f:
            f.write("Adding another line to file!\n")
        makefile('to_be_added.txt', 'this file was added')
        commit_changes_in_repo(REPODIR, 'testing applied changes',
                               verbose=True)
        self.assertFalse(is_git_dirty(REPODIR), "Git still dirty after commit!")
        self.assert_file_exists('to_be_left_alone.txt')
        self.assert_file_exists('to_be_modified.txt')
        self.assert_file_exists('to_be_added.txt')
        self.assert_file_not_exists('to_be_deleted.txt')

    def test_commit_filename_with_spaces(self):
        """See issue #79 (https://github.com/data-workspaces/data-workspaces-core/issues/79)
        Files with spaces returned by git status --porcelain are in quotes!
        """
        makefile('to_be_deleted.txt', 'this file will be deleted')
        makefile('to_be_left_alone.txt', 'this file to be left alone')
        makefile('to be modified.txt', 'this file to be modified')
        self._git_add(['to_be_deleted.txt', 'to_be_left_alone.txt',
                       'to be modified.txt'])
        self._run(['commit', '-m', 'initial version'])
        os.remove(join(REPODIR, 'to_be_deleted.txt'))
        with open(join(REPODIR, 'to be modified.txt'), 'a') as f:
            f.write("Adding another line to file!\n")
        makefile('to_be_added.txt', 'this file was added')
        commit_changes_in_repo(REPODIR, 'testing applied changes',
                               verbose=True)
        self.assertFalse(is_git_dirty(REPODIR), "Git still dirty after commit!")
        self.assert_file_exists('to_be_left_alone.txt')
        self.assert_file_exists('to be modified.txt')
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
        initial_hash = get_local_head_hash(REPODIR, True)
        self._run(['rm', 'to_be_deleted.txt'])
        with open(join(REPODIR, 'to_be_modified.txt'), 'a') as f:
            f.write("Adding another line to file!\n")
        makefile('to_be_added.txt', 'this file was added')
        self._git_add(['to_be_modified.txt', 'to_be_added.txt'])
        self._run(['commit', '-m', 'second version'])
        second_hash = get_local_head_hash(REPODIR, True)
        makefile('added_in_third_commit.txt', 'added in third commit')
        with open(join(REPODIR, 'to_be_modified.txt'), 'a') as f:
            f.write("Adding a third to file!\n")
        self._git_add(['added_in_third_commit.txt', 'to_be_modified.txt'])
        self._run(['commit', '-m', 'third version'])
        third_hash = get_local_head_hash(REPODIR, True)

        # now, revert back to the first commit
        checkout_and_apply_commit(REPODIR, initial_hash, True)
        self.assertFalse(is_git_dirty(REPODIR), "Git still dirty after commit!")
        self.assert_file_exists("to_be_deleted.txt")
        self.assert_file_not_exists("to_be_added.txt")
        self.assert_file_not_exists("added_in_third_commit.txt")
        self.assert_file_contents_equal("to_be_modified.txt",
                                        "this file to be modified")
        restored_hash = get_local_head_hash(REPODIR, True)
        self.assertNotEqual(initial_hash, restored_hash) # should be differemt

        # add a commit at this point
        with open(join(REPODIR, 'to_be_modified.txt'), 'a') as f:
            f.write("Overwritten after restoring first commit.")
        self._git_add(['to_be_modified.txt'])
        self._run(['commit', '-m', 'branch off first version'])

        # restore to third version
        checkout_and_apply_commit(REPODIR, third_hash, True)
        self.assertFalse(is_git_dirty(REPODIR), "Git still dirty after commit!")
        self.assert_file_exists("added_in_third_commit.txt")
        self.assert_file_contents_equal("to_be_modified.txt",
                                        'this file to be modified\n'+
                                        "Adding another line to file!\n"+
                                        "Adding a third to file!")
        self.assert_file_not_exists("to_be_deleted.txt")

        # revert to restored hash and verify changes
        checkout_and_apply_commit(REPODIR, restored_hash, True)
        self.assertFalse(is_git_dirty(REPODIR), "Git still dirty after commit!")
        self.assert_file_exists("to_be_deleted.txt")
        self.assert_file_not_exists("to_be_added.txt")
        self.assert_file_not_exists("added_in_third_commit.txt")
        self.assert_file_contents_equal("to_be_modified.txt",
                                        "this file to be modified")

        # run again with the same hash. It should do nothing, as there
        # are no changes
        checkout_and_apply_commit(REPODIR, restored_hash, True)


class TestSubdirCommit(BaseCase):
    def test_commit(self):
        os.mkdir(join(REPODIR, 'subdir'))
        makefile('subdir/to_be_deleted.txt', 'this file will be deleted')
        makefile('subdir/to_be_left_alone.txt', 'this file to be left alone')
        makefile('subdir/to_be_modified.txt', 'this file to be modified')
        makefile('root_file1.txt', 'this should not be changed')
        self._git_add(['subdir/to_be_deleted.txt', 'subdir/to_be_left_alone.txt',
                       'subdir/to_be_modified.txt',
                       'root_file1.txt'])
        self._run(['commit', '-m', 'initial version'])
        os.remove(join(REPODIR, 'subdir/to_be_deleted.txt'))
        with open(join(REPODIR, 'subdir/to_be_modified.txt'), 'a') as f:
            f.write("Adding another line to file!\n")
        makefile('subdir/to_be_added.txt', 'this file was added')
        commit_changes_in_repo_subdir(REPODIR, 'subdir', 'testing applied changes',
                                      verbose=True)
        self.assertFalse(is_git_dirty(REPODIR), "Git still dirty after commit!")
        self.assert_file_exists('subdir/to_be_left_alone.txt')
        self.assert_file_exists('subdir/to_be_modified.txt')
        self.assert_file_exists('subdir/to_be_added.txt')
        self.assert_file_not_exists('subdir/to_be_deleted.txt')
        self.assert_file_exists('root_file1.txt')
        # verify that staged files outside of the subdir are not changed
        makefile('staged_but_not_committed.txt', 'should be staged but not committed')
        self._git_add(['staged_but_not_committed.txt'])
        commit_changes_in_repo_subdir(REPODIR, 'subdir', 'testing not committing',
                                      verbose=True)
        self.assertFalse(is_git_subdir_dirty(REPODIR, 'subdir'))
        self.assertTrue(is_git_dirty(REPODIR))
        self.assertTrue(is_git_staging_dirty(REPODIR))
        self.assertFalse(is_git_staging_dirty(REPODIR, 'subdir'))

    def test_commit_filename_with_spaces(self):
        """See issue #79 (https://github.com/data-workspaces/data-workspaces-core/issues/79)
        Files with spaces returned by git status --porcelain are in quotes!
        """
        os.mkdir(join(REPODIR, 'subdir'))
        makefile('subdir/to_be_deleted.txt', 'this file will be deleted')
        makefile('subdir/to_be_left_alone.txt', 'this file to be left alone')
        makefile('subdir/to be modified.txt', 'this file to be modified')
        makefile('root_file1.txt', 'this should not be changed')
        self._git_add(['subdir/to_be_deleted.txt', 'subdir/to_be_left_alone.txt',
                       'subdir/to be modified.txt',
                       'root_file1.txt'])
        self._run(['commit', '-m', 'initial version'])
        os.remove(join(REPODIR, 'subdir/to_be_deleted.txt'))
        with open(join(REPODIR, 'subdir/to be modified.txt'), 'a') as f:
            f.write("Adding another line to file!\n")
        makefile('subdir/to_be_added.txt', 'this file was added')
        commit_changes_in_repo_subdir(REPODIR, 'subdir', 'testing applied changes',
                                      verbose=True)
        self.assertFalse(is_git_dirty(REPODIR), "Git still dirty after commit!")
        self.assert_file_exists('subdir/to_be_left_alone.txt')
        self.assert_file_exists('subdir/to be modified.txt')
        self.assert_file_exists('subdir/to_be_added.txt')
        self.assert_file_not_exists('subdir/to_be_deleted.txt')
        self.assert_file_exists('root_file1.txt')
        # verify that staged files outside of the subdir are not changed
        makefile('staged_but_not_committed.txt', 'should be staged but not committed')
        self._git_add(['staged_but_not_committed.txt'])
        commit_changes_in_repo_subdir(REPODIR, 'subdir', 'testing not committing',
                                      verbose=True)
        self.assertFalse(is_git_subdir_dirty(REPODIR, 'subdir'))
        self.assertTrue(is_git_dirty(REPODIR))
        self.assertTrue(is_git_staging_dirty(REPODIR))
        self.assertFalse(is_git_staging_dirty(REPODIR, 'subdir'))


class TestCheckoutSubdirAndApplyCommit(BaseCase):
    def test_checkout_and_apply_commit(self):
        # First, do all the setup
        os.mkdir(join(REPODIR, 'subdir'))
        makefile('subdir/to_be_deleted.txt', 'this file will be deleted')
        makefile('subdir/to_be_left_alone.txt', 'this file to be left alone')
        makefile('subdir/to_be_modified.txt', 'this file to be modified\n')
        makefile('root_file1.txt', 'root file v1\n')
        self._git_add(['subdir/to_be_deleted.txt', 'subdir/to_be_left_alone.txt',
                       'subdir/to_be_modified.txt', 'root_file1.txt'])
        self._run(['commit', '-m', 'initial version'])
        initial_hash = get_local_head_hash(REPODIR, True)
        subdir_hash = get_subdirectory_hash(REPODIR, 'subdir', verbose=True)

        self._run(['rm', 'subdir/to_be_deleted.txt'])
        with open(join(REPODIR, 'subdir/to_be_modified.txt'), 'a') as f:
            f.write("Adding another line to file!\n")
        makefile('subdir/to_be_added.txt', 'this file was added')
        with open(join(REPODIR, 'root_file1.txt'), 'a') as f:
            f.write("root file v2")
        self._git_add(['subdir/to_be_modified.txt', 'subdir/to_be_added.txt',
                       'root_file1.txt'])
        self._run(['commit', '-m', 'second version'])
        second_hash = get_local_head_hash(REPODIR, True)

        makefile('subdir/added_in_third_commit.txt', 'added in third commit')
        with open(join(REPODIR, 'subdir/to_be_modified.txt'), 'a') as f:
            f.write("Adding a third to file!\n")
        self._git_add(['subdir/added_in_third_commit.txt', 'subdir/to_be_modified.txt'])
        self._run(['commit', '-m', 'third version'])
        third_hash = get_local_head_hash(REPODIR, True)

        # now, revert back to the first commit
        checkout_subdir_and_apply_commit(REPODIR, 'subdir', initial_hash, True)
        self.assertFalse(is_git_dirty(REPODIR), "Git still dirty after commit!")
        self.assert_file_exists("subdir/to_be_deleted.txt")
        self.assert_file_not_exists("subdir/to_be_added.txt")
        self.assert_file_not_exists("subdir/added_in_third_commit.txt")
        self.assert_file_contents_equal("subdir/to_be_modified.txt",
                                        "this file to be modified")
        self.assert_file_contents_equal("root_file1.txt",
                                        "root file v1\nroot file v2")
        restored_hash = get_local_head_hash(REPODIR, True)
        self.assertNotEqual(initial_hash, restored_hash) # should be differemt
        restored_subdir_hash = get_subdirectory_hash(REPODIR, 'subdir', verbose=True)
        self.assertEqual(subdir_hash, restored_subdir_hash)

        # add a commit at this point
        with open(join(REPODIR, 'subdir/to_be_modified.txt'), 'a') as f:
            f.write("Overwritten after restoring first commit.")
        self._git_add(['subdir/to_be_modified.txt'])
        self._run(['commit', '-m', 'branch off first version'])

        # restore to third version
        checkout_subdir_and_apply_commit(REPODIR, 'subdir', third_hash, True)
        self.assertFalse(is_git_dirty(REPODIR), "Git still dirty after commit!")
        self.assert_file_exists("subdir/added_in_third_commit.txt")
        self.assert_file_contents_equal("subdir/to_be_modified.txt",
                                        'this file to be modified\n'+
                                        "Adding another line to file!\n"+
                                        "Adding a third to file!")
        self.assert_file_not_exists("subdir/to_be_deleted.txt")
        self.assert_file_contents_equal("root_file1.txt",
                                        "root file v1\nroot file v2")

        # revert to restored hash and verify changes
        checkout_subdir_and_apply_commit(REPODIR, 'subdir', restored_hash, True)
        self.assertFalse(is_git_dirty(REPODIR), "Git still dirty after commit!")
        self.assert_file_exists("subdir/to_be_deleted.txt")
        self.assert_file_not_exists("subdir/to_be_added.txt")
        self.assert_file_not_exists("subdir/added_in_third_commit.txt")
        self.assert_file_contents_equal("subdir/to_be_modified.txt",
                                        "this file to be modified")
        self.assert_file_contents_equal("root_file1.txt",
                                        "root file v1\nroot file v2")

        # run again with the same hash. It should do nothing, as there
        # are no changes
        checkout_subdir_and_apply_commit(REPODIR, 'subdir', restored_hash, True)

TESTS_DIR=os.path.dirname(os.path.abspath(os.path.expanduser(__file__)))
THIS_REPO_DIR=os.path.abspath(join(TESTS_DIR, '..'))
class TestGetSubdirectoryHash(unittest.TestCase):
    def test_subdir_hash(self):
        """Test that we can obtain git's hash entry for data-workspaces-core/tests.
        We validate the hash by running a git cat-file on the object and then
        verifying that it contains this file and the makefile
        """
        tests_dir_hash = get_subdirectory_hash(THIS_REPO_DIR, 'tests', verbose=True)
        r = subprocess.run([GIT_EXE_PATH, 'cat-file', '-p', tests_dir_hash],
                           cwd=THIS_REPO_DIR, stdout=subprocess.PIPE, encoding='utf-8')
        r.check_returncode()
        files = set()
        for line in r.stdout.split('\n'):
            if len(line)==0:
                continue
            fields = line.split()
            self.assertEqual(len(fields), 4, "Unexpected cat-file output: %s"%line)
            files.add(fields[3])
        self.assertTrue('test_git_utils.py' in files)
        self.assertTrue('Makefile' in files)


class TestMisc(unittest.TestCase):
    def test_get_json_file_from_remote(self):
        """We get the data.json file in this directory from the origin repo
        and then check its contents.
        """
        this_dir = os.path.dirname(os.path.abspath(os.path.expanduser(__file__)))
        repo_dir = os.path.abspath(join(this_dir, '..'))
        data = get_json_file_from_remote('tests/data.json', repo_dir, verbose=True)
        keys = frozenset(data.keys())
        self.assertEqual(keys, frozenset(['foo', 'bat']))
        self.assertEqual(data['foo'], 'bar')
        self.assertEqual(data['bat'], 3)

ORIGIN_DIR=join(TEMPDIR, 'repo_origin.git')
DELETE_DIR=join(REPODIR, 'to-delete')
KEEP_DIR=join(REPODIR, 'to-keep')
DELETE_FILES_DIR=join(REPODIR, 'to-delete-files')
class TestRemove(BaseCase):
    def _add_file(self, dirpath, filename):
        filepath = join(dirpath, filename)
        with open(filepath, 'w') as f:
            f.write("test file %s\n" % filename)
        rel_path = get_subpath_from_absolute(REPODIR, filepath)
        assert rel_path is not None
        self._git_add([rel_path])

    def setUp(self):
        super().setUp()
        self._run(['init', '--bare', 'repo_origin.git'],
                  cwd=TEMPDIR)
        self._run(['remote', 'add', 'origin', ORIGIN_DIR], cwd=REPODIR)
        os.mkdir(KEEP_DIR)
        self._add_file(KEEP_DIR, 'keep1.txt')
        self._add_file(KEEP_DIR, 'keep2.txt')
        os.mkdir(DELETE_DIR)
        self._add_file(DELETE_DIR, 'delete1.txt')
        self._add_file(DELETE_DIR, 'delete2.txt')
        os.mkdir(DELETE_FILES_DIR)
        self._add_file(DELETE_FILES_DIR, 'delete_files1.txt')
        self._add_file(DELETE_FILES_DIR, 'delete_files2.txt')
        self._add_file(DELETE_FILES_DIR, 'keep_in_delete_files_dir.txt')
        self._run(['commit', '-m', 'initial commit'])
        self._run(['push', 'origin', 'master'])

    def test_delete_tree(self):
        git_remove_subtree(REPODIR, 'to-delete', verbose=True)
        self._run(['commit', '-m', 'test commit'])
        self._run(['push', 'origin', 'master'])

    def test_delete_files(self):
        git_remove_file(REPODIR, 'to-delete-files/delete_files1.txt',
                        verbose=True)
        git_remove_file(REPODIR, 'to-delete-files/delete_files2.txt',
                        verbose=True)
        self._run(['commit', '-m', 'test commit'])
        self._run(['push', 'origin', 'master'])





if __name__ == '__main__':
    unittest.main()

