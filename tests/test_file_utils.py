#!/usr/bin/env python3
"""
Test file utilities
"""
import os.path
import unittest
import tempfile
import sys
import shutil

TEMPDIR=os.path.abspath(os.path.expanduser(__file__)).replace('.py', '_data')

OTHER_FS='/tmp'

try:
    import dataworkspaces
except ImportError:
    sys.path.append(os.path.abspath(".."))

from dataworkspaces.utils.file_utils import safe_rename

class TestFileUtils(unittest.TestCase):
    def setUp(self):
        if os.path.exists(TEMPDIR):
            shutil.rmtree(TEMPDIR)
        os.mkdir(TEMPDIR)

    def tearDown(self):
        if os.path.exists(TEMPDIR):
            shutil.rmtree(TEMPDIR)

    def test_safe_rename(self):
        """This test needs to be run in a case where OTHER_FS is on a separate
        filesystem - the underlying issue is that os.rename() does not work
        across filesystems and this version does copying as a fallback.
        """
        data = "this is a test\n"
        with tempfile.NamedTemporaryFile(dir=OTHER_FS, delete=False) as testfile:
            testfile.write(data.encode('utf-8'))
        try:
            dest = os.path.join(TEMPDIR, 'data.txt')
            safe_rename(testfile.name, dest)
            self.assertTrue(os.path.exists(dest))
            with open(dest, 'r') as f:
                data2 = f.read()
            self.assertEqual(data, data2)
        finally:
            if os.path.exists(testfile.name):
                os.remove(testfile.name)



if __name__ == '__main__':
    unittest.main()
