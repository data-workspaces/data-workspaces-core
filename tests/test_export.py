"""
Test cases for exporting a resource
"""
import sys
import os
from os.path import join, exists
import unittest

from utils_for_tests import SimpleCase, WS_DIR, OTHER_WS, TEMPDIR
from dataworkspaces.lineage import LineageBuilder

# If set to True, by --keep-outputs option, leave the output data
KEEP_OUTPUTS=False

CSV_DATA=\
"""X1,X2,Y
1,2,1
2,2,0
3,1,1
"""

IM_DATA=\
"""
PRED
1
0
0
"""

RESULTS_DIR=join(WS_DIR, 'results')
SOURCE_DATA_DIR=join(WS_DIR, 'source-data')
CSV_FILE=join(SOURCE_DATA_DIR, 'data.csv')
CODE_DIR=join(WS_DIR, 'code')
CODE_FILE=join(CODE_DIR, 'code.py')


class TestExport(SimpleCase):
    """For these tests, we create a source CSV file and some output data.
    We call the Lineage api directly to create some lineage, take a snapshot,
    and verify that the lineage file got created. In the case of a results resesource,
    a copy of the various results files should be left at the root of the resource
    (doing a copy to the snapshot directory rather than a move).
    """
    def tearDown(self):
        if not KEEP_OUTPUTS:
            super().tearDown()
    def _assert_exists(self, ws_relpath):
        full_path = join(WS_DIR, ws_relpath)
        self.assertTrue(exists(full_path),
                        "Expecting %s at %s, but did not find it"% (ws_relpath, full_path))


    def test_export_of_results(self):
        self._setup_initial_repo(git_resources='code,source-data', hostname='test-host')
        results_dir = join(WS_DIR, 'results')
        os.mkdir(results_dir)
        self._run_dws(['add', 'git', '--export', '--role', 'results', results_dir])
        with open(CSV_FILE, 'w') as f:
            f.write(CSV_DATA)
        with open(CODE_FILE, 'w') as f:
            f.write("print('hello')\n")
        builder = LineageBuilder(
                      ).with_workspace_directory(WS_DIR
                      ).with_step_name('code.py'
                      ).with_parameters({'a':5}
                      ).with_input_path(CSV_FILE
                      ).as_results_step(RESULTS_DIR
                      )
        with builder.eval() as lineage:
            lineage.write_results({'accuracy':0.95, 'recall':0.8})
        self._run_dws(['snapshot', 'tag1'])
        self._assert_exists('results/lineage.json')
        self._assert_file_git_tracked('results/lineage.json')
        self._assert_exists('results/results.json')
        self._assert_file_git_tracked('results/results.json')
        self._assert_exists('results/snapshots/test-host-tag1')
        self._assert_exists('results/snapshots/test-host-tag1/results.json')
        self._assert_file_git_tracked('results/snapshots/test-host-tag1/results.json')
        self._assert_exists('results/snapshots/test-host-tag1/lineage.json')
        self._assert_file_git_tracked('results/snapshots/test-host-tag1/lineage.json')

        # run a second time to make sure that delete of old lineage was successful
        builder = LineageBuilder(
                      ).with_workspace_directory(WS_DIR
                      ).with_step_name('code.py'
                      ).with_parameters({'a':6}
                      ).with_input_path(CSV_FILE
                      ).as_results_step(RESULTS_DIR
                      )
        with builder.eval() as lineage:
            lineage.write_results({'accuracy':0.96, 'recall':0.85})
        self._run_dws(['snapshot', 'tag2'])
        self._assert_exists('results/lineage.json')
        self._assert_file_git_tracked('results/lineage.json')
        self._assert_exists('results/results.json')
        self._assert_file_git_tracked('results/results.json')
        self._assert_exists('results/snapshots/test-host-tag2')
        self._assert_exists('results/snapshots/test-host-tag2/results.json')
        self._assert_file_git_tracked('results/snapshots/test-host-tag2/results.json')
        self._assert_exists('results/snapshots/test-host-tag2/lineage.json')
        self._assert_file_git_tracked('results/snapshots/test-host-tag2/lineage.json')

    def test_export_of_intermediate_data(self):
        self._setup_initial_repo(git_resources='code,source-data', hostname='test-host')
        im_dir = join(WS_DIR, 'intermediate-data')
        os.mkdir(im_dir)
        self._run_dws(['add', 'git', '--export', '--role', 'intermediate-data', im_dir])
        with open(CSV_FILE, 'w') as f:
            f.write(CSV_DATA)
        with open(CODE_FILE, 'w') as f:
            f.write("print('hello')\n")
        builder = LineageBuilder(
                      ).with_workspace_directory(WS_DIR
                      ).with_step_name('code.py'
                      ).with_parameters({'a':5}
                      ).with_input_path(CSV_FILE
                      )
        with builder.eval() as lineage:
            im_data = join(im_dir, 'im_data.csv')
            lineage.add_output_path(im_data)
            with open(im_data, 'w') as f:
                f.write(IM_DATA)
        self._run_dws(['snapshot', 'tag1'])
        self._assert_exists('intermediate-data/lineage.json')
        self._assert_file_git_tracked('intermediate-data/lineage.json')
        self._assert_exists('intermediate-data/im_data.csv')
        self._assert_file_git_tracked('intermediate-data/im_data.csv')

    def test_export_of_local_file_results(self):
        self._setup_initial_repo(git_resources='code,source-data', hostname='test-host')
        results_dir = join(WS_DIR, 'results')
        os.mkdir(results_dir)
        self._run_dws(['add', 'local-files', '--export', '--role', 'results', results_dir])
        with open(CSV_FILE, 'w') as f:
            f.write(CSV_DATA)
        with open(CODE_FILE, 'w') as f:
            f.write("print('hello')\n")
        builder = LineageBuilder(
                      ).with_workspace_directory(WS_DIR
                      ).with_step_name('code.py'
                      ).with_parameters({'a':5}
                      ).with_input_path(CSV_FILE
                      ).as_results_step(RESULTS_DIR
                      )
        with builder.eval() as lineage:
            lineage.write_results({'accuracy':0.95, 'recall':0.8})
        self._run_dws(['snapshot', 'tag1'])
        self._assert_exists('results/lineage.json')
        self._assert_exists('results/results.json')
        self._assert_exists('results/snapshots/test-host-tag1')
        self._assert_exists('results/snapshots/test-host-tag1/results.json')
        self._assert_exists('results/snapshots/test-host-tag1/lineage.json')

    def test_export_of_local_file_intermediate_data(self):
        self._setup_initial_repo(git_resources='code,source-data', hostname='test-host')
        im_dir = join(WS_DIR, 'intermediate-data')
        os.mkdir(im_dir)
        self._run_dws(['add', 'local-files', '--export', '--role', 'intermediate-data', im_dir])
        with open(CSV_FILE, 'w') as f:
            f.write(CSV_DATA)
        with open(CODE_FILE, 'w') as f:
            f.write("print('hello')\n")
        builder = LineageBuilder(
                      ).with_workspace_directory(WS_DIR
                      ).with_step_name('code.py'
                      ).with_parameters({'a':5}
                      ).with_input_path(CSV_FILE
                      )
        with builder.eval() as lineage:
            im_data = join(im_dir, 'im_data.csv')
            lineage.add_output_path(im_data)
            with open(im_data, 'w') as f:
                f.write(IM_DATA)
        self._run_dws(['snapshot', 'tag1'])
        self._assert_exists('intermediate-data/lineage.json')
        self._assert_exists('intermediate-data/im_data.csv')


if __name__ == '__main__':
    if len(sys.argv)>1 and sys.argv[1]=='--keep-outputs':
        KEEP_OUTPUTS=True
        del sys.argv[1]
        print("--keep-outputs specified, will skip cleanup steps")
        print("  Outputs in %s"%TEMPDIR)
    unittest.main()
