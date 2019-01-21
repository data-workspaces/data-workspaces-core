#!/usr/bin/env python3
# Copyright 2018,2019 by MPI-SWS and Data-ken Research. Licensed under Apache 2.0. See LICENSE.txt.
"""Test the integration with git-fat for the main workspace as well as individual
repositories
"""

import unittest
import sys
import os
import os.path
from os.path import join, exists, basename, dirname, isdir
import shutil
import subprocess
import getpass
import filecmp

USERNAME=getpass.getuser()
TEMPDIR=os.path.abspath(os.path.expanduser(__file__)).replace('.py', '_data')
WS_DIR=join(TEMPDIR,'workspace')
FAT_FILES=join(TEMPDIR, 'fat-files')
WS_ORIGIN=join(TEMPDIR, 'workspace_origin.git')
CLONED_WS_PARENT=join(TEMPDIR, 'cloned_ws')
CLONED_WS=join(CLONED_WS_PARENT, 'workspace')

DWS_REPO=dirname(dirname(os.path.abspath(os.path.expanduser(__file__))))
BAD_FAT_DIR=join(DWS_REPO, '.git/fat')


try:
    import dataworkspaces
except ImportError:
    sys.path.append(os.path.abspath(".."))

from dataworkspaces.utils.git_utils import GIT_EXE_PATH

# figure out where the command line api lives
import dataworkspaces.dws
COMMAND_LINE_API=os.path.abspath(os.path.expanduser(dataworkspaces.dws.__file__))

def make_compressed_file(path, extra=None):
    if exists(path+'.gz'):
        os.remove(path+'.gz')
    with open(path, 'w') as f:
        f.write("This is a test. This file will be compressed and stored in git-files\n")
        if extra:
            f.write(extra + '\n')
    r = subprocess.run(['/usr/bin/gzip', '-n', basename(path)], cwd=dirname(path))
    r.check_returncode()
    return path + '.gz'


class BaseCase(unittest.TestCase):
    def setUp(self):
        if isdir(BAD_FAT_DIR):
            raise Exception("%s exists -- you seem to have run git fat init against the actual dataworkspaces repo!"%
                            BAD_FAT_DIR)
        if os.path.exists(TEMPDIR):
            shutil.rmtree(TEMPDIR)
        os.mkdir(TEMPDIR)
        os.mkdir(WS_DIR)
        os.mkdir(FAT_FILES)
        os.mkdir(CLONED_WS_PARENT)

    def tearDown(self):
        if os.path.exists(TEMPDIR):
            shutil.rmtree(TEMPDIR)
            #pass

    def _run_dws(self, dws_args, cwd=WS_DIR):
        #args = [sys.executable, COMMAND_LINE_API]+dws_args
        command = 'dws --verbose --batch '+ ' '.join(dws_args)
        print(command + (' [%s]' % cwd))
        r = subprocess.run(command, cwd=cwd, shell=True)
        r.check_returncode()

    def _run_git(self, git_args, cwd=WS_DIR):
        args = [GIT_EXE_PATH]+git_args
        print(' '.join(args) + (' [%s]' % cwd))
        r = subprocess.run(args, cwd=cwd)
        r.check_returncode()

    def _assert_fat_file_exists(self, filehash, base_name):
        fpath = join(FAT_FILES, filehash)
        self.assertTrue(exists(fpath),
                        ("Git-fat file entry for %s does not exist, expecting"+
                        " it at %s") % (base_name, fpath))

    def _assert_files_same(self, f1, f2):
        self.assertTrue(exists(f1), "Missing file %s" % f1)
        self.assertTrue(exists(f2), "Missing file %s" % f2)
        self.assertTrue(filecmp.cmp(f1, f2, shallow=False),
                        "Files %s and %s are different" % (f1, f2))

class TestGitFatInWorkspace(BaseCase):
    def test_init(self):
        self._run_dws(['init', '--use-basic-resource-template',
                       '--git-fat-remote',
                       FAT_FILES,
                       '--git-fat-user='+USERNAME,
                       "--git-fat-attributes='*.gz,*.zip'"])
        self._run_git(['init', '--bare', 'workspace_origin.git'],
                      cwd=TEMPDIR)
        self._run_git(['remote', 'add', 'origin', WS_ORIGIN], cwd=WS_DIR)
        base_file=join(WS_DIR, 'source-data/data.txt')
        compressed_file = make_compressed_file(base_file)
        self._run_dws(['snapshot', 'SNAPSHOT1'])
        self._run_dws(['push'])
        self._assert_fat_file_exists('42c56f6ca605b48763aca8e87de977b5708b4d3b',
                                     'data.txt.gz')
        self._run_dws(['clone', WS_ORIGIN], cwd=CLONED_WS_PARENT)
        cloned_fat_dir=join(CLONED_WS, '.git/fat')
        self.assertFalse(isdir(BAD_FAT_DIR),
                         "%s exists -- test to have run git fat init against the actual dataworkspaces repo!"%
                         BAD_FAT_DIR)
        self.assertTrue(isdir(cloned_fat_dir),
                        "did not find fat directory at %s" % cloned_fat_dir)
        cloned_data_file = join(CLONED_WS, 'source-data/data.txt.gz')
        self._assert_files_same(compressed_file, cloned_data_file)

        # now, change our file and propagate to clone
        compressed_file = make_compressed_file(base_file, 'this is the second version')
        self._run_dws(['snapshot', 'SNAPSHOT2'])
        self._run_dws(['push'])
        self._assert_fat_file_exists('66698e38d8d72d45bf1a8c443f47540a1918089d',
                                     'data.txt.gz')
        self._run_dws(['pull'], cwd=CLONED_WS)
        self._assert_files_same(compressed_file, cloned_data_file)

        # restore our first snapshot
        self._run_dws(['restore', 'SNAPSHOT1'])
        self._assert_files_same(compressed_file,
                                join(FAT_FILES,
                                     '42c56f6ca605b48763aca8e87de977b5708b4d3b',))


if __name__ == '__main__':
    unittest.main()


