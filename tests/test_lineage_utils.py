import unittest
import os.path
import sys
import shutil
import datetime

try:
    import dataworkspaces
except ImportError:
    sys.path.append(os.path.abspath(".."))

from dataworkspaces.utils.lineage_utils import \
    StepLineage, LineageStoreCurrent, ResourceRef

TEMPDIR=os.path.abspath(os.path.expanduser(__file__)).replace('.py', '_data')
LOCAL_STORE_DIR=os.path.join(TEMPDIR, 'local_store')

class TestLineageStoreCurrent(unittest.TestCase):
    """Tests for the lineage current store api
    """
    def setUp(self):
        os.mkdir(TEMPDIR)
        os.mkdir(LOCAL_STORE_DIR)

    def tearDown(self):
        shutil.rmtree(TEMPDIR)

    def test_basic_senario(self):
        store = LineageStoreCurrent()
        step1_lineage = StepLineage.make_step_lineage('step1', datetime.datetime.now(),
                                                     [('p1', 'v1'), ('p2', 5)],
                                                      [ResourceRef('r1'),
                                                       ResourceRef('r2', 'foo/bar')],
                                                      store)
        step1_lineage.add_output(store, ResourceRef('intermediate', 's1'))
        step1_lineage.execution_time_seconds = 5
        store.add_step(step1_lineage)
        step2_lineage = StepLineage.make_step_lineage('step2', datetime.datetime.now(),
                                                      [('p3', 'v3')],
                                                      [ResourceRef('intermediate', 's1')],
                                                      store)
        step2_lineage.add_output(store, ResourceRef('intermediate', 's2'))
        step2_lineage.execution_time_seconds = 20
        store.add_step(step2_lineage)
        step3_lineage = StepLineage.make_step_lineage('step3', datetime.datetime.now(),
                                                      [('p4', 4)],
                                                      [ResourceRef('r2', 'foo/bar'),
                                                       ResourceRef('intermeidate', 's2')],
                                                      store)
        step3_lineage.add_output(store, ResourceRef('results'))
        step3_lineage.execution_time_seconds = 3
        store.save(LOCAL_STORE_DIR)
if __name__ == '__main__':
    unittest.main()
