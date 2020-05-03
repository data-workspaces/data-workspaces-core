"""
Test cases for rclone resource
"""
import sys
import os
from os.path import join, exists
import unittest
import shutil

from utils_for_tests import BaseCase, WS_DIR, TEMPDIR
from dataworkspaces.lineage import LineageBuilder
from dataworkspaces.api import get_snapshot_history
from dataworkspaces.utils.subprocess_utils import find_exe

# If set to True, by --keep-outputs option, leave the output data
KEEP_OUTPUTS = False

MASTER_DIR=join(TEMPDIR, 'rclone-master')
RESOURCE_DIR=join(WS_DIR, 'rclone-resource')

REAL_REMOTE='dws-test:/dws-test.benedat'

def rclone_found():
    try:
        find_exe('rclone', "need to install rclone")
        return True
    except:
        return False

def remote_exists(remote_name):
    from dataworkspaces.third_party.rclone import RClone
    rc = RClone()
    return remote_name in rc.listremotes()

@unittest.skipUnless(rclone_found(), "SKIP: rclone executable not found")
class TestRclone(BaseCase):
    def tearDown(self):
        if not KEEP_OUTPUTS:
            super().tearDown()

    def _get_rclone(self):
        from dataworkspaces.third_party.rclone import RClone
        return RClone()

    def _assert_file(self, base_path, rel_path, filesize):
        path = join(base_path, rel_path)
        self.assertTrue(exists(path), "Local file %s is missing"%path)
        fstat = os.stat(path)
        with open(path, 'r') as f:
            data = f.read()
        self.assertEqual(filesize, fstat.st_size,
                         "Filesize missmatch for %s, contents were: '%s'" %
                         (rel_path, repr(data)))

    def _assert_file_removed(self, base_path, rel_path):
        path = join(base_path, rel_path)
        self.assertTrue(not exists(path), "Local file %s present, should have been removed"%path)

    def _assert_initial_state(self, basedir):
        self._assert_file(basedir, 'file1.txt', 15)
        self._assert_file(basedir, 'subdir/file2.txt', 20)
        self._assert_file(basedir, 'subdir/file3.txt', 16)
        self._assert_file_removed(basedir, 'subdir/file4.txt')

    def _assert_final_state_copy(self, basedir):
        self._assert_file(basedir, 'file1.txt', 30)
        self._assert_file(basedir, 'subdir/file2.txt', 20)
        self._assert_file(basedir, 'subdir/file3.txt', 16)
        self._assert_file(basedir, 'subdir/file4.txt', 16)

    def _assert_final_state_sync(self, basedir):
        self._assert_file(basedir, 'file1.txt', 30)
        self._assert_file_removed(basedir, 'subdir/file2.txt')
        self._assert_file(basedir, 'subdir/file3.txt', 16)
        self._assert_file(basedir, 'subdir/file4.txt', 16)

    def _init_files(self, basedir=MASTER_DIR):
        with open(join(basedir, 'file1.txt'), 'w') as f:
            f.write("this is a test\n")
        subdir = join(basedir, 'subdir')
        os.mkdir(subdir)
        with open(join(subdir, 'file2.txt'), 'w') as f:
            f.write("this is also a test\n")
        with open(join(subdir, 'file3.txt'), 'w') as f:
            f.write("this is test #3\n")

    def _update_files(self, basedir=MASTER_DIR):
        """Append to file1.txt, remove file2.txt, leave file3.txt alone,
        and add file4.txt
        """
        with open(join(basedir, 'file1.txt'), 'a') as f:
            f.write("line 2 of file\n")
        subdir = join(basedir, 'subdir')
        os.remove(join(subdir, 'file2.txt'))
        with open(join(subdir, 'file4.txt'), 'w') as f:
            f.write("this is test #4\n")

    def test_copy_remote_is_master(self):
        """Will pull changes down from master in copy mode.
        """
        self._setup_initial_repo()
        os.mkdir(MASTER_DIR)
        self._init_files()
        self._run_dws(['add', 'rclone','--role', 'source-data', '--sync-mode=copy', '--master=remote', 'localfs:'+MASTER_DIR,
                       RESOURCE_DIR])
        self._assert_initial_state(RESOURCE_DIR)
        self._run_dws(['snapshot', 'tag1'])

        self._update_files()
        self._run_dws(['pull'])
        self._assert_final_state_copy(RESOURCE_DIR)
        self._run_dws(['snapshot', 'tag2'])

        # get the snapshot history and check the hashes.
        # these were manually verified. The key thing to
        # check is that the local hash is being computed and included
        # in the overall snapshot.
        history = get_snapshot_history(WS_DIR)
        self.assertEqual(len(history), 2)
        snap1 = history[0]
        self.assertEqual(['tag1'], snap1.tags)
        self.assertEqual('9162a3c2825841ee9426c28089da4901986bb226', snap1.hashval)
        snap2 = history[1]
        self.assertEqual(['tag2'], snap2.tags)
        self.assertEqual('e8ea515f179a08479caafb7c7da05ce18525cbd0', snap2.hashval)

    def test_sync_remote_is_master(self):
        """Will pull changes down from master in sync mode.
        """
        self._setup_initial_repo()
        os.mkdir(MASTER_DIR)
        self._init_files()
        self._run_dws(['add', 'rclone','--role', 'source-data', '--sync-mode=sync', '--master=remote', 'localfs:'+MASTER_DIR,
                       RESOURCE_DIR])
        self._assert_initial_state(RESOURCE_DIR)
        self._run_dws(['snapshot', 'tag1'])

        self._update_files()
        self._run_dws(['pull'])
        self._assert_final_state_sync(RESOURCE_DIR)
        self._run_dws(['snapshot', 'tag2'])

    def test_copy_local_is_master(self):
        """Will push changes up to master in copy mode."""
        self._setup_initial_repo()
        os.mkdir(MASTER_DIR)
        os.mkdir(RESOURCE_DIR)
        self._init_files(RESOURCE_DIR)
        self._run_dws(['add', 'rclone','--role', 'source-data', '--sync-mode=copy', '--master=local', 'localfs:'+MASTER_DIR,
                       RESOURCE_DIR])
        # before we push up
        self._assert_initial_state(RESOURCE_DIR)
        self._run_dws(['snapshot', 'tag1'])
        self._run_dws(['push'])
        self._assert_initial_state(MASTER_DIR)

        self._update_files(RESOURCE_DIR)
        self._run_dws(['snapshot', 'tag2'])
        self._run_dws(['push'])
        self._assert_final_state_copy(MASTER_DIR)

    def test_copy_local_is_master(self):
        """Will push changes up to master in copy mode."""
        self._setup_initial_repo()
        os.mkdir(MASTER_DIR)
        os.mkdir(RESOURCE_DIR)
        self._init_files(RESOURCE_DIR)
        self._run_dws(['add', 'rclone','--role', 'source-data', '--sync-mode=copy', '--master=local', 'localfs:'+MASTER_DIR,
                       RESOURCE_DIR])
        # before we push up
        self._assert_initial_state(RESOURCE_DIR)
        self._run_dws(['snapshot', 'tag1'])
        self._run_dws(['push'])
        self._assert_initial_state(MASTER_DIR)

        self._update_files(RESOURCE_DIR)
        self._run_dws(['snapshot', 'tag2'])
        self._run_dws(['push'])
        self._assert_final_state_copy(MASTER_DIR)

    def test_sync_local_is_master(self):
        """Will push changes up to master in sync mode."""
        self._setup_initial_repo()
        os.mkdir(MASTER_DIR)
        os.mkdir(RESOURCE_DIR)
        self._init_files(RESOURCE_DIR)
        self._run_dws(['add', 'rclone','--role', 'source-data', '--sync-mode=sync', '--master=local', 'localfs:'+MASTER_DIR,
                       RESOURCE_DIR])
        # before we push up
        self._assert_initial_state(RESOURCE_DIR)
        self._run_dws(['snapshot', 'tag1'])
        self._run_dws(['push'])
        self._assert_initial_state(MASTER_DIR)

        self._update_files(RESOURCE_DIR)
        self._run_dws(['snapshot', 'tag2'])
        self._run_dws(['push'])
        self._assert_final_state_sync(MASTER_DIR)

    def test_no_master_initial_copy(self):
        """Master set to none, there is no initial directory, so we
        copy down the files from master.
        """
        self._setup_initial_repo()
        os.mkdir(MASTER_DIR)
        self._init_files()
        self._run_dws(['add', 'rclone','--role', 'source-data', '--sync-mode=copy', '--master=none', 'localfs:'+MASTER_DIR,
                       RESOURCE_DIR])
        self._assert_initial_state(RESOURCE_DIR)
        self._run_dws(['snapshot', 'tag1'])

        self._update_files()
        self._run_dws(['pull'])
        self._assert_initial_state(RESOURCE_DIR)
        self._run_dws(['snapshot', 'tag2'])

    @unittest.skipUnless(remote_exists('dws-test'), "SKIP: rclone config file does not contain dws-test remote")
    def test_real_remote(self):
        """Test with a real remote that is configured in the rclone config file as dws-test.
        We use the sync with remote master scenario."""
        self._setup_initial_repo()
        # the master is not tied to our workspace, we sync it to the real master using rclone directly
        os.mkdir(MASTER_DIR)
        self._init_files(MASTER_DIR)
        rclone = self._get_rclone()
        print("Running initial sync...")
        ret = rclone.sync(MASTER_DIR, REAL_REMOTE, flags=['--verbose'])
        self.assertEqual(ret['code'], 0, "Return from rclone sync bad: %s"% repr(ret))
        print("Sync was successful")
        self._run_dws(['add', 'rclone','--role', 'source-data', '--sync-mode=sync', '--master=remote', REAL_REMOTE,
                       RESOURCE_DIR])
        self._assert_initial_state(RESOURCE_DIR)
        self._run_dws(['snapshot', 'tag1'])

        self._update_files(MASTER_DIR)
        ret = rclone.sync(MASTER_DIR, REAL_REMOTE)
        self.assertEqual(ret['code'], 0, "Return from rclone sync bad: %s"% repr(ret))
        self._run_dws(['pull'])
        self._assert_final_state_sync(RESOURCE_DIR)
        self._run_dws(['snapshot', 'tag2'])


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--keep-outputs":
        KEEP_OUTPUTS = True
        del sys.argv[1]
        print("--keep-outputs specified, will skip cleanup steps")
        print("  Outputs in %s" % TEMPDIR)
    unittest.main()
