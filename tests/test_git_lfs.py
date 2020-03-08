#!/usr/bin/env python3
# Copyright 2018,2019 by MPI-SWS and Data-ken Research. Licensed under Apache 2.0. See LICENSE.txt.
"""Test the integration with git-fat for the main workspace as well as individual
repositories
"""

import unittest
import sys
import os
import os.path
from os.path import join, exists, basename, dirname, isdir, abspath, expanduser
import shutil




# also sets up the path for dataworkspaces
from utils_for_tests import TEMPDIR, WS_DIR, HelperMethods

GZIP_CANDIDATES=['/bin/gzip', '/usr/bin/gzip']
GZIP_EXE=None
for exe in GZIP_CANDIDATES:
    if exists(exe):
        GZIP_EXE=exe
        break
assert GZIP_EXE is not None,\
  "Could not find gzip. Looked for it at: %s" % ', '.join(GZIP_CANDIDATES)


from dataworkspaces.utils.git_utils import GIT_EXE_PATH
from dataworkspaces.utils.subprocess_utils import find_exe
from dataworkspaces.errors import ConfigurationError

from dataworkspaces.utils.git_lfs_utils import \
    _does_attributes_file_reference_lfs, is_a_git_lfs_repo,\
    is_git_lfs_installed_for_user

try:
    lfs_exe = find_exe('git-lfs',
                       'install git-lfs via instructions at https://git-lfs.github.com')
    GIT_LFS_INSTALLED = True
    print("git-lfs installed at %s" % lfs_exe)
except:
    print("Did not find git-lfs")
    GIT_LFS_INSTALLED = False


def make_compressed_file(path, extra=None):
    if exists(path+'.gz'):
        os.remove(path+'.gz')
    with open(path, 'w') as f:
        f.write("This is a test. This file will be compressed and stored in git-files\n")
        if extra:
            f.write(extra + '\n')
    r = subprocess.run([GZIP_EXE, '-n', basename(path)], cwd=dirname(path))
    r.check_returncode()
    return path + '.gz'

SAMPLE_ATTR_FILE1="""
*               text=auto
b_logfile* filter=lfs diff=lfs merge=lfs -text
*.idb filter=lfs diff=lfs merge=lfs -text
"""

SAMPLE_ATTR_FILE2="""
*               text=auto
b_logfile* filter=fat diff=fat merge=fat -text
*.idb filter=fat diff=fat merge=fat -text
"""



class TestGitAttributes(unittest.TestCase):
    def setUp(self):
        os.mkdir(TEMPDIR)
    def tearDown(self):
        if exists(TEMPDIR):
            shutil.rmtree(TEMPDIR)

    def _write_attributes_file(self, content):
        afile = join(TEMPDIR, '.gitattributes')
        with open(afile, 'w') as f:
            f.write(content)
        return afile

    def test_git_attributes_check(self):
        afile = self._write_attributes_file(SAMPLE_ATTR_FILE1)
        self.assertTrue(_does_attributes_file_reference_lfs(afile))

    def test_negative_git_attributes_check(self):
        afile = self._write_attributes_file(SAMPLE_ATTR_FILE2)
        self.assertFalse(_does_attributes_file_reference_lfs(afile))

class TestIsAnLfsRepo(HelperMethods, unittest.TestCase):
    def setUp(self):
        os.mkdir(TEMPDIR)
        # create a subdirectory structure
        os.mkdir(WS_DIR)
        self.subdir = join(WS_DIR, 'subdir')
        os.mkdir(self.subdir)
        self.subsubdir = join(self.subdir, 'subsubdir')
        os.mkdir(self.subsubdir)
        self.subdir2 = join(WS_DIR, 'subdir2')
        os.mkdir(self.subdir2)
        self._run_git(['init'])
        random_file = join(self.subsubdir, 'random.txt')
        with open(random_file, 'w') as f:
            f.write("this is a test\n")
        self._run_git(['add', random_file])
        self._run_git(['commit', '-m', 'initial'])

    def tearDown(self):
        if exists(TEMPDIR):
            shutil.rmtree(TEMPDIR)

    def _write_attributes_file(self, content, subdir=None):
        if subdir:
            afile = join(join(WS_DIR, subdir), '.gitattributes')
            relpath = join(subdir, '.gitattributes')
        else:
            afile = join(WS_DIR, '.gitattributes')
            relpath = '.gitattributes'
        with open(afile, 'w') as f:
            f.write(content)
        self._run_git(['add', relpath])
        self._run_git(['commit', '-m', 'added attributes at %s'%relpath])

    def test_root_attributes(self):
        self._write_attributes_file(SAMPLE_ATTR_FILE1)
        self.assertTrue(is_a_git_lfs_repo(WS_DIR, recursive=False))
        self.assertTrue(is_a_git_lfs_repo(WS_DIR, recursive=True))

    def test_subdir_attributes(self):
        self._write_attributes_file(SAMPLE_ATTR_FILE1, subdir=self.subsubdir)
        self.assertFalse(is_a_git_lfs_repo(WS_DIR, recursive=False))
        self.assertTrue(is_a_git_lfs_repo(WS_DIR, recursive=True))

    def test_no_attributes(self):
        self._write_attributes_file(SAMPLE_ATTR_FILE2)
        self._write_attributes_file(SAMPLE_ATTR_FILE2, subdir=self.subsubdir)
        self.assertFalse(is_a_git_lfs_repo(WS_DIR, recursive=False))
        self.assertFalse(is_a_git_lfs_repo(WS_DIR, recursive=False))

GIT_CONFIG_FILE1="""
[user]
        name = Jeff Fischer
        email = jeff.fischer@benedat.com
[filter "lfs"]
        process = git-lfs filter-process
        required = true
        clean = git-lfs clean -- %f
        smudge = git-lfs smudge -- %f
"""

GIT_CONFIG_FILE2="""
[user]
        name = Jeff Fischer
        email = jeff.fischer@benedat.com
"""


class TestCheckInstalledInUser(unittest.TestCase):
    def setUp(self):
        os.mkdir(TEMPDIR)
    def tearDown(self):
        if exists(TEMPDIR):
            shutil.rmtree(TEMPDIR)

    def _write_config_file(self, content):
        afile = join(TEMPDIR, '.gitconfig')
        with open(afile, 'w') as f:
            f.write(content)

    def test_git_lfs_installed(self):
        self._write_config_file(GIT_CONFIG_FILE1)
        self.assertTrue(is_git_lfs_installed_for_user(TEMPDIR))

    def test_git_lfs_not_installed(self):
        self._write_config_file(GIT_CONFIG_FILE2)
        self.assertFalse(is_git_lfs_installed_for_user(TEMPDIR))

    def test_real_homedir(self):
        homedir = abspath(expanduser('~'))
        self.assertEqual(is_git_lfs_installed_for_user(homedir),
                         is_git_lfs_installed_for_user())


if __name__ == '__main__':
    unittest.main()


