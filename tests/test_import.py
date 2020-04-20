"""
Test cases for importing a resource
"""
import sys
import os
from os.path import join, exists
import unittest
import shutil

from utils_for_tests import SimpleCase, WS_DIR, OTHER_WS, TEMPDIR
from dataworkspaces.lineage import LineageBuilder
from dataworkspaces.api import make_lineage_table, make_lineage_graph

# If set to True, by --keep-outputs option, leave the output data
KEEP_OUTPUTS = False

CSV_DATA = """X1,X2,Y
1,2,1
2,2,0
3,1,1
"""

IM_DATA = """
PRED
1
0
0
"""

RESULTS_DIR = join(WS_DIR, "results")
SOURCE_DATA_DIR = join(WS_DIR, "source-data")
CSV_FILE = join(SOURCE_DATA_DIR, "data.csv")
CODE_DIR = join(WS_DIR, "code")
CODE_FILE = join(CODE_DIR, "code.py")
EXPORTED_RESOURCE_DIR=join(TEMPDIR, 'exported-resource')
RESOURCE_ORIGIN=join(TEMPDIR, 'resource_origin.git')


class TestImport(SimpleCase):
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
        self.assertTrue(
            exists(full_path),
            "Expecting %s at %s, but did not find it" % (ws_relpath, full_path),
        )

    def _setup_exported_resource(self):
        self._setup_initial_repo(git_resources="code,source-data", hostname="test-host")
        im_dir = join(WS_DIR, "intermediate-data")
        os.mkdir(im_dir)
        self._run_git(['init'], cwd=im_dir)
        with open(join(im_dir, 'readme.txt'), 'w') as f:
            f.write("This is an exported git resource\n")
        self._run_git(['add', 'readme.txt'], cwd=im_dir)
        self._run_git(['commit', '-m', 'git initial commit'],
                      cwd=im_dir)
        self._run_git(['init', '--bare', 'resource_origin.git'],
                      cwd=TEMPDIR)
        self._run_git(['remote', 'add', 'origin', RESOURCE_ORIGIN], cwd=im_dir)
        self._run_git(['push', '--set-upstream', 'origin', 'master'], cwd=im_dir)

        self._run_dws(["add", "git", "--export", "--role", "intermediate-data", '--name', 'exported-resource', im_dir])
        with open(CSV_FILE, "w") as f:
            f.write(CSV_DATA)
        with open(CODE_FILE, "w") as f:
            f.write("print('hello')\n")
        builder = (
            LineageBuilder()
            .with_workspace_directory(WS_DIR)
            .with_step_name("code.py")
            .with_parameters({"a": 5})
            .with_input_path(CSV_FILE)
        )
        with builder.eval() as lineage:
            im_data = join(im_dir, "im_data.csv")
            lineage.add_output_path(im_data)
            with open(im_data, "w") as f:
                f.write(IM_DATA)
        self._run_dws(["snapshot", "tag1"])
        self._assert_exists("intermediate-data/lineage.json")
        shutil.copytree(im_dir, EXPORTED_RESOURCE_DIR)
        shutil.rmtree(WS_DIR)
        print("Exported resource now set up at %s"% EXPORTED_RESOURCE_DIR)

    def test_import(self):
        self._setup_exported_resource()
        os.mkdir(WS_DIR)
        self._setup_initial_repo(git_resources="code,results", hostname="test-host")
        self._run_dws(['add', 'git', '--imported', EXPORTED_RESOURCE_DIR])
        with open(CODE_FILE, "w") as f:
            f.write("print('hello')\n")
        builder = (
            LineageBuilder()
            .with_workspace_directory(WS_DIR)
            .with_step_name("code.py")
            .with_parameters({"a": 5})
            .with_input_path(join(EXPORTED_RESOURCE_DIR, 'im_data.csv'))
            .as_results_step(RESULTS_DIR)
        )
        with builder.eval() as lineage:
            lineage.write_results({"accuracy": 0.95, "recall": 0.8})
        tlist = make_lineage_table(WS_DIR, verbose=True)
        expected_refs = frozenset(['results',
                                   'exported-resource:/im_data.csv',
                                   'source-data:/data.csv'])
        actual_refs = frozenset([t[0] for t in tlist])
        self.assertEqual(expected_refs, actual_refs)
        graph_output_file = join(TEMPDIR, 'graph_pre_snapshot.html')
        make_lineage_graph(graph_output_file, WS_DIR, verbose=True)

        self._run_dws(["snapshot", "tag1"])
        tlist = make_lineage_table(WS_DIR, tag_or_hash='tag1', verbose=True)
        actual_refs = frozenset([t[0] for t in tlist])
        self.assertEqual(expected_refs, actual_refs)
        graph_output_file = join(TEMPDIR, 'graph_post_snapshot.html')
        make_lineage_graph(graph_output_file, WS_DIR, tag_or_hash='tag1', verbose=True)




if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--keep-outputs":
        KEEP_OUTPUTS = True
        del sys.argv[1]
        print("--keep-outputs specified, will skip cleanup steps")
        print("  Outputs in %s" % TEMPDIR)
    unittest.main()
