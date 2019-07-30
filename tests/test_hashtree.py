#!/usr/bin/env python3
# Copyright 2018,2019 by MPI-SWS and Data-ken Research. Licensed under Apache 2.0. See LICENSE.txt.
"""Test of file hashing code at dataworkspaces.resources.hashtree
"""

import unittest
import sys
import os
import os.path
from os.path import join, basename
import shutil

CURRENTDIR=os.path.dirname(os.path.abspath(os.path.expanduser(__file__)))
HASHDIR=os.path.abspath(os.path.expanduser(__file__)).replace('.py', '_data')
DATADIR=os.path.abspath(os.path.expanduser(__file__)).replace('.py', '_data2')
DATA_SUBDIR=join(DATADIR, 'subdir')
SKIP_SUBDIR=join(DATA_SUBDIR, 'skip_me')


# If set to True, by --keep-outputs option, leave the output data
KEEP_OUTPUTS=False

try:
    import dataworkspaces
except ImportError:
    sys.path.append(os.path.abspath(".."))

from dataworkspaces.resources.hashtree import generate_hashes, check_hashes,\
      compute_hash, compute_size

IGNORE_DIRS= ['skip_me']# ['test_jupyter_kit.ipynb']


# we will add this file after first computing the hash
EXTRA_FILE=join(DATA_SUBDIR, 'extra_file.txt')
FILE_TO_OVERWRITE=join(DATADIR, 'test_hashtree.py')

VERBOSE=True

class TestHashTree(unittest.TestCase):
    def setUp(self):
        if os.path.exists(HASHDIR):
            shutil.rmtree(HASHDIR)
        if os.path.exists(DATADIR):
            shutil.rmtree(DATADIR)
        os.mkdir(HASHDIR)
        os.mkdir(DATADIR)
        os.mkdir(DATA_SUBDIR)
        os.mkdir(SKIP_SUBDIR)
        for fname in os.listdir(CURRENTDIR):
            if fname.endswith('.py') or fname.endswith('.sh') or fname=='test_jupyter_kit.ipynb':
                shutil.copyfile(join(CURRENTDIR, fname),
                                join(DATADIR, fname))
        with open(join(SKIP_SUBDIR, 'skip.txt'), 'w') as f:
            f.write("Should be skipped!\n")

    def tearDown(self):
        if not KEEP_OUTPUTS:
            if os.path.exists(HASHDIR):
                shutil.rmtree(HASHDIR)
            if os.path.exists(DATADIR):
                shutil.rmtree(DATADIR)

    def _run_hash_and_check(self, hash_fun):
        h = generate_hashes(HASHDIR, DATADIR, ignore=IGNORE_DIRS, hash_fun=hash_fun, add_to_git=False,
                            verbose=VERBOSE)
        print("Hash of %s is %s"% (DATADIR, h))
        #test_walk('.')
        print("Checking hashes")
        b = check_hashes(h, HASHDIR, DATADIR, ignore=IGNORE_DIRS, hash_fun=hash_fun,
                         verbose=VERBOSE)
        self.assertTrue(b, 'initial check failed')
        with open(EXTRA_FILE, 'w') as f:
            f.write("AHA")
        print("\n\nAdded file at %s" % EXTRA_FILE)
        print("Checking hashes again (should fail)")
        b = check_hashes(h, HASHDIR, DATADIR, ignore=IGNORE_DIRS, hash_fun=hash_fun,
                         verbose=VERBOSE)
        self.assertFalse(b, "check after adding file should have failed, but didn't")
        # overwrite a file and validate that it changes the hash
        h = generate_hashes(HASHDIR, DATADIR, ignore=IGNORE_DIRS, hash_fun=hash_fun, add_to_git=False,
                            verbose=VERBOSE)
        print("Hash of %s is %s"% (DATADIR, h))
        with open(FILE_TO_OVERWRITE, 'w') as f:
            f.write("Overwritten!")
        print("Changed contents of %s" % FILE_TO_OVERWRITE)
        print("Checking hashes")
        b = check_hashes(h, HASHDIR, DATADIR, ignore=IGNORE_DIRS, hash_fun=hash_fun,
                         verbose=VERBOSE)
        self.assertFalse(b, "hash should have been difference, but wasn't")


    def test_hashing(self):
        self._run_hash_and_check(compute_hash)

    def test_size_based_hashing(self):
        self._run_hash_and_check(compute_size)


if __name__ == '__main__':
    if len(sys.argv)>1 and sys.argv[1]=='--keep-outputs':
        KEEP_OUTPUTS=True
        del sys.argv[1]
        print("--keep-outputs specified, will skip cleanup steps")
    unittest.main()
