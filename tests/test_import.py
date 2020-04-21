"""
Test cases for importing a resource
"""
import sys
import os
from os.path import join, exists
import unittest
import shutil

from utils_for_tests import SimpleCase, BaseCase, WS_DIR, OTHER_WS, TEMPDIR
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

CSV_DATA2 = """X1,X2,Y
1,2,1
2,2,1
3,1,0
"""

IM_DATA2 = """
PRED
1
1
0
"""

RESULTS_DIR = join(WS_DIR, "results")
SOURCE_DATA_DIR = join(WS_DIR, "source-data")
CSV_FILE = join(SOURCE_DATA_DIR, "data.csv")
CODE_DIR = join(WS_DIR, "code")
CODE_FILE = join(CODE_DIR, "code.py")
EXPORTED_RESOURCE_DIR=join(TEMPDIR, 'exported-resource')
RESOURCE_ORIGIN=join(TEMPDIR, 'resource_origin.git')
IMPORT_WS_DIR=join(TEMPDIR, 'import-workspace')


class TestImportGitRepo(BaseCase):
    """Create a workspace and build lineage for an exported resource.
    Then, import that into a second workspace and run another step,
    reading from the imported resource. Take a snapshot and verify that
    it has the imported lineage.

    TODO: test pull, which should update the current lineage
    """

    def tearDown(self):
        if not KEEP_OUTPUTS:
            super().tearDown()

    def _assert_exists(self, ws_relpath, base_path=WS_DIR):
        full_path = join(base_path, ws_relpath)
        self.assertTrue(
            exists(full_path),
            "Expecting %s at %s, but did not find it" % (ws_relpath, full_path),
        )

    def _setup_exported_resource(self):
        os.mkdir(IMPORT_WS_DIR)
        self._run_dws(['init', '--create-resources=code,source-data', '--hostname=test-host'],
                      cwd=IMPORT_WS_DIR)
        im_dir = join(IMPORT_WS_DIR, "intermediate-data")
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

        self._run_dws(["add", "git", "--export", "--role", "intermediate-data", '--name', 'exported-resource', im_dir],
                      cwd=IMPORT_WS_DIR)
        csv_file = join(IMPORT_WS_DIR, 'source-data/data.csv')
        with open(csv_file, "w") as f:
            f.write(CSV_DATA)
        code_file=join(IMPORT_WS_DIR, 'code/code.py')
        with open(code_file, "w") as f:
            f.write("print('hello')\n")
        builder = (
            LineageBuilder()
            .with_workspace_directory(IMPORT_WS_DIR)
            .with_step_name("code.py")
            .with_parameters({"a": 5})
            .with_input_path(csv_file)
        )
        with builder.eval() as lineage:
            im_data = join(im_dir, "im_data.csv")
            lineage.add_output_path(im_data)
            with open(im_data, "w") as f:
                f.write(IM_DATA)
        self._run_dws(["snapshot", "tag1"], cwd=IMPORT_WS_DIR)
        self._assert_exists("intermediate-data/lineage.json", base_path=IMPORT_WS_DIR)
        # we should really do a dws push here, but that requires setting up an origin
        # for the whole import workspace
        self._run_git(['push'], cwd=join(IMPORT_WS_DIR, 'intermediate-data'))

        self._run_git(['clone', RESOURCE_ORIGIN, EXPORTED_RESOURCE_DIR], cwd=TEMPDIR)
        self._assert_exists('lineage.json', base_path=EXPORTED_RESOURCE_DIR)
        print("Exported resource now set up at %s"% EXPORTED_RESOURCE_DIR)

    def _update_exported_resource(self):
        """Run the exporting workspace again to update the exported resource"""
        csv_file2 = join(IMPORT_WS_DIR, 'source-data/data2.csv')
        with open(csv_file2, "w") as f:
            f.write(CSV_DATA2)
        code_file=join(IMPORT_WS_DIR, 'code/code.py')
        with open(code_file, "w") as f:
            f.write("print('hello')\n")
        builder = (
            LineageBuilder()
            .with_workspace_directory(IMPORT_WS_DIR)
            .with_step_name("code.py")
            .with_parameters({"a": 5})
            .with_input_path(csv_file2)
        )
        im_dir = join(IMPORT_WS_DIR, "intermediate-data")
        with builder.eval() as lineage:
            im_data = join(im_dir, "im_data.csv")
            lineage.add_output_path(im_data)
            with open(im_data, "w") as f:
                f.write(IM_DATA2)
        self._run_dws(["snapshot", "tag2"], cwd=IMPORT_WS_DIR)
        self._assert_exists("intermediate-data/lineage.json", base_path=IMPORT_WS_DIR)
        # we should really do a dws push here, but that requires setting up an origin
        # for the whole import workspace
        self._run_git(['push'], cwd=join(IMPORT_WS_DIR, 'intermediate-data'))

    def test_import(self):
        self._setup_exported_resource()
        #os.mkdir(WS_DIR)
        self._setup_initial_repo(create_resources="code,results", hostname="test-host")
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

        # verify that an update of the exported resource followed by a pull works correctly
        self._update_exported_resource()
        self._run_dws(['pull'])
        tlist = make_lineage_table(WS_DIR, verbose=True)
        actual_refs = frozenset([t[0] for t in tlist])
        expected_refs = frozenset(['exported-resource:/im_data.csv', 'source-data:/data2.csv'])
        self.assertEqual(expected_refs, actual_refs)



class TestImportLocalFiles(SimpleCase):
    """Create a workspace and build lineage for an exported resource.
    Then, import that into a second workspace and run another step,
    reading from the imported resource. Take a snapshot and verify that
    it has the imported lineage.
    """

    def tearDown(self):
        if not KEEP_OUTPUTS:
            super().tearDown()

    def _assert_exists(self, relpath, base_path=WS_DIR):
        full_path = join(base_path, relpath)
        self.assertTrue(
            exists(full_path),
            "Expecting %s at %s, but did not find it" % (relpath, full_path),
        )

    def _setup_exported_resource(self):
        self._setup_initial_repo(git_resources="code,source-data", hostname="test-host")
        os.mkdir(EXPORTED_RESOURCE_DIR)
        with open(join(EXPORTED_RESOURCE_DIR, 'readme.txt'), 'w') as f:
            f.write("This is an exported local files resource\n")
        self._run_dws(["add", "local-files", "--export", "--role", "intermediate-data", '--name', 'exported-resource',
                       EXPORTED_RESOURCE_DIR])
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
            im_data = join(EXPORTED_RESOURCE_DIR, "im_data.csv")
            lineage.add_output_path(im_data)
            with open(im_data, "w") as f:
                f.write(IM_DATA)
        self._run_dws(["snapshot", "tag1"])
        self._assert_exists("lineage.json", base_path=EXPORTED_RESOURCE_DIR)
        shutil.rmtree(WS_DIR)
        print("Exported resource now set up at %s"% EXPORTED_RESOURCE_DIR)

    def test_import(self):
        self._setup_exported_resource()
        os.mkdir(WS_DIR)
        self._setup_initial_repo(git_resources="code,results", hostname="test-host")
        self._run_dws(['add', 'local-files', '--imported', EXPORTED_RESOURCE_DIR])
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


class TestImportRclone(SimpleCase):
    """Create a workspace and build lineage for an exported resource.
    Then, import that into a second workspace and run another step,
    reading from the imported resource. Take a snapshot and verify that
    it has the imported lineage.
    """

    def tearDown(self):
        if not KEEP_OUTPUTS:
            super().tearDown()

    def _assert_exists(self, relpath, base_path=WS_DIR):
        full_path = join(base_path, relpath)
        self.assertTrue(
            exists(full_path),
            "Expecting %s at %s, but did not find it" % (relpath, full_path),
        )

    def _setup_exported_resource(self):
        self._setup_initial_repo(git_resources="code,source-data", hostname="test-host")
        os.mkdir(EXPORTED_RESOURCE_DIR)
        with open(join(EXPORTED_RESOURCE_DIR, 'readme.txt'), 'w') as f:
            f.write("This is an exported local files resource\n")
        self._run_dws(["add", "local-files", "--export", "--role", "intermediate-data", '--name', 'exported-resource',
                       EXPORTED_RESOURCE_DIR])
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
            im_data = join(EXPORTED_RESOURCE_DIR, "im_data.csv")
            lineage.add_output_path(im_data)
            with open(im_data, "w") as f:
                f.write(IM_DATA)
        self._run_dws(["snapshot", "tag1"])
        self._assert_exists("lineage.json", base_path=EXPORTED_RESOURCE_DIR)
        shutil.rmtree(WS_DIR)
        print("Exported resource now set up at %s"% EXPORTED_RESOURCE_DIR)

    def test_import(self):
        self._setup_exported_resource()
        os.mkdir(WS_DIR)
        self._setup_initial_repo(git_resources="code,results", hostname="test-host")
        imported_dir=join(WS_DIR, 'exported-resource')
        print("cmd would be: dws add rclone --imported 'localfs:%s' ./exported-resource"%EXPORTED_RESOURCE_DIR)
        self._run_dws(['add', 'rclone', '--imported', 'localfs:'+EXPORTED_RESOURCE_DIR,
                       './exported-resource'])
        with open(CODE_FILE, "w") as f:
            f.write("print('hello')\n")
        builder = (
            LineageBuilder()
            .with_workspace_directory(WS_DIR)
            .with_step_name("code.py")
            .with_parameters({"a": 5})
            .with_input_path(join(imported_dir, 'im_data.csv'))
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
