#!/usr/bin/env python3
# Copyright 2018,2019 by MPI-SWS and Data-ken Research. Licensed under Apache 2.0. See LICENSE.txt.
"""Test moving result files during the snapshot command
"""

import unittest
import sys
import os
import os.path
import shutil
import re
import datetime
import subprocess

try:
    import dataworkspaces
except ImportError:
    sys.path.append(os.path.abspath(".."))

from dataworkspaces.utils.snapshot_utils import \
    move_current_files_local_fs, make_re_pattern_for_dir_template,\
    expand_dir_template
from dataworkspaces.resources.git_resource import \
    git_move_and_add, is_file_tracked_by_git

TEMPDIR=os.path.abspath(os.path.expanduser(__file__)).replace('.py', '_data')
EXCLUDE_DIRS_RE=re.compile(r'^.+\-.+\/.+\/.+\-\d\d\:\d\d$')

def makefile(fname):
    fpath = os.path.join(TEMPDIR, fname)
    with open(fpath, 'w') as f:
        f.write(fpath)

class TestDirTemplateRe(unittest.TestCase):
    def _test_pat(self, template, expected):
        expected = expected.replace(r'\/', '/').replace(r'\:', ':')
        p = make_re_pattern_for_dir_template(template)
        print("%s => %s" % (template, p))
        self.assertEqual(p, expected,
                         "pattern result was '%s', expecting '%s'" %
                         (str(p), str(expected)))

    def test1(self):
        self._test_pat('{ISO_TIMESTAMP}/{USERNAME}-{SNAPSHOT_NO}',
                       r'^\d\d\d\d\-\d\d\-\d\dT\d\d:\d\d:\d\d\/\w([\w\-\.])*\-\d\d+$')

    def test2(self):
        self._test_pat('{YEAR}-{MONTH}/{DAY}-{MIN}:{SEC}-{TAG}',
                       r'^\d\d\d\d\-\d\d\/\d\d\-\d\d\:\d\d\-\w([\w\-\.])*$')

    def test3(self):
        self._test_pat('saved-results/{ISO_TIMESTAMP}-{TAG}-{SNAPSHOT_NO}',
                       r'^saved\-results\/\d\d\d\d\-\d\d\-\d\dT\d\d:\d\d:\d\d\-\w([\w\-\.])*\-\d\d+$')

TIMESTAMP=datetime.datetime(2018, 9, 30, 18, 19, 54, 951829)

class TestExpandDirTemplate(unittest.TestCase):
    def _test_template(self, template, username, hostname, timestamp,
                       snapshot_no, snapshot_tag, expected):
        d = expand_dir_template(template, username, hostname, timestamp,
                                snapshot_no, snapshot_tag)
        print("%s [%s,%s,%s,%s,%s] => %s"%
              (template, username, hostname, timestamp.isoformat(), snapshot_no,
               snapshot_tag, d))
        self.assertEqual(d, expected,
                         "Directory '%s' does not match expected directory '%s"%
                         (d, expected))
        p = make_re_pattern_for_dir_template(template)
        r = re.compile(p)
        self.assertTrue(r.match(d) is not None,
                        "Pattern '%s' did not match directory '%s'" %
                        (p, d))

    def test1(self):
        self._test_template('{ISO_TIMESTAMP}/{USERNAME}-{TAG}',
                            'jfischer', 'localhost', TIMESTAMP,
                            22, 'V1', '2018-09-30T18:19:54/jfischer-V1')

    def test2(self):
        self._test_template('results/{YEAR}-{MONTH}/{DAY}.{DAY_OF_WEEK}-{TAG}-{HOSTNAME}',
                            'jfischer', 'localhost', TIMESTAMP,
                            22, None, 'results/2018-09/30.Sunday-022-localhost')

    def test3(self):
        self._test_template('{YEAR}-{MONTH}/{SHORT_MONTH}-{DAY}-{HOUR}:{MIN}-{TAG}',
                            'jfischer', 'localhost', TIMESTAMP,
                            22, 'V1',
                            '2018-09/Sep-30-18:19-V1')


class TestMoveResults(unittest.TestCase):
    def setUp(self):
        if os.path.exists(TEMPDIR):
            shutil.rmtree(TEMPDIR)
        os.mkdir(TEMPDIR)

    def tearDown(self):
        if os.path.exists(TEMPDIR):
            shutil.rmtree(TEMPDIR)

    def _assert_exists(self, relpath):
        abspath = os.path.join(TEMPDIR, relpath)
        self.assertTrue(os.path.exists(abspath),
                        "File %s (relpath %s) does not exist" %
                        (abspath, relpath))
    def test_move(self):
        # first set up some files
        makefile('results.csv')
        makefile('test.log')
        subdir = os.path.join(TEMPDIR, 'subdir')
        os.mkdir(subdir)
        makefile('subdir/output.csv')
        mapping = move_current_files_local_fs('test',
                                              TEMPDIR, '2018-09/19/jfischer-11:45',
                                              set(['results.csv']),
                                              EXCLUDE_DIRS_RE,
                                              verbose=True)
        self.assertEqual([('test.log', '2018-09/19/jfischer-11:45/test.log'),
                          ('subdir/output.csv',
                           '2018-09/19/jfischer-11:45/subdir/output.csv')],
                         mapping)
        self._assert_exists('2018-09/19/jfischer-11:45/test.log')
        self._assert_exists('2018-09/19/jfischer-11:45/subdir/output.csv')
        self._assert_exists('results.csv')

        # now, add more files and move them
        print("Adding second batch of files")
        makefile('test.log')
        makefile('test2.log')
        os.mkdir(subdir)
        makefile('subdir/output.csv')
        mapping = move_current_files_local_fs('test',
                                              TEMPDIR, '2018-09/19/jfischer-11:50',
                                              set(['results.csv']),
                                              EXCLUDE_DIRS_RE,
                                              verbose=True)
        mapping_dict={orig:new for (orig, new) in mapping}
        self.assertEqual({'test.log':'2018-09/19/jfischer-11:50/test.log',
                          'test2.log': '2018-09/19/jfischer-11:50/test2.log',
                          'subdir/output.csv': '2018-09/19/jfischer-11:50/subdir/output.csv'},
                         mapping_dict)
        self._assert_exists('2018-09/19/jfischer-11:50/test.log')
        self._assert_exists('2018-09/19/jfischer-11:50/test2.log')
        self._assert_exists('2018-09/19/jfischer-11:50/subdir/output.csv')
        self._assert_exists('results.csv')


class TestMoveResultsGit(unittest.TestCase):
    def setUp(self):
        if os.path.exists(TEMPDIR):
            shutil.rmtree(TEMPDIR)
        os.mkdir(TEMPDIR)
        subprocess.check_call(['/usr/bin/git', 'init'],
                              cwd=TEMPDIR)

    def tearDown(self):
        if os.path.exists(TEMPDIR):
            shutil.rmtree(TEMPDIR)

    def _assert_exists(self, relpath, should_be_in_git=False):
        abspath = os.path.join(TEMPDIR, relpath)
        self.assertTrue(os.path.exists(abspath),
                        "File %s (relpath %s) does not exist" %
                        (abspath, relpath))
        if should_be_in_git:
            self.assertTrue(is_file_tracked_by_git(relpath, TEMPDIR, True))

    def test_move(self):
        # first set up some files
        makefile('results.csv')
        makefile('test.log')
        subdir = os.path.join(TEMPDIR, 'subdir')
        os.mkdir(subdir)
        makefile('subdir/output.csv')
        mapping = move_current_files_local_fs('test',
                                              TEMPDIR, '2018-09/19/jfischer-11:45',
                                              set(['results.csv']),
                                              EXCLUDE_DIRS_RE,
                                              move_fn=lambda src,dest:
                                                        git_move_and_add(src, dest,
                                                                         TEMPDIR,
                                                                         True),
                                              verbose=True)
        self.assertEqual([('test.log', '2018-09/19/jfischer-11:45/test.log'),
                          ('subdir/output.csv',
                           '2018-09/19/jfischer-11:45/subdir/output.csv')],
                         mapping)
        self._assert_exists('2018-09/19/jfischer-11:45/test.log', True)
        self._assert_exists('2018-09/19/jfischer-11:45/subdir/output.csv', True)
        self._assert_exists('results.csv', False)

        # now, add more files and move them
        print("Adding second batch of files")
        makefile('test.log')
        makefile('test2.log')
        os.mkdir(subdir)
        makefile('subdir/output.csv')
        mapping = move_current_files_local_fs('test',
                                              TEMPDIR, '2018-09/19/jfischer-11:50',
                                              set(['results.csv']),
                                              EXCLUDE_DIRS_RE,
                                              verbose=True)
        mapping_dict={orig:new for (orig, new) in mapping}
        self.assertEqual({'test.log':'2018-09/19/jfischer-11:50/test.log',
                          'test2.log':'2018-09/19/jfischer-11:50/test2.log',
                          'subdir/output.csv':'2018-09/19/jfischer-11:50/subdir/output.csv'},
                         mapping_dict)
        self._assert_exists('2018-09/19/jfischer-11:50/test.log')
        self._assert_exists('2018-09/19/jfischer-11:50/test2.log')
        self._assert_exists('2018-09/19/jfischer-11:50/subdir/output.csv')
        self._assert_exists('results.csv')




if __name__ == '__main__':
    unittest.main()
