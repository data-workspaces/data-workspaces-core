import unittest
import os.path
import sys
import shutil
import datetime
import json

try:
    import dataworkspaces
except ImportError:
    sys.path.append(os.path.abspath(".."))

from dataworkspaces.errors import LineageError
from dataworkspaces.utils.lineage_utils import \
    StepLineage, LineageStoreCurrent, ResourceRef, SourceDataLineage,\
    LineageConsistencyError, PlaceholderCertificate, HashCertificate

TEMPDIR=os.path.abspath(os.path.expanduser(__file__)).replace('.py', '_data')
LOCAL_STORE_DIR=os.path.join(TEMPDIR, 'local_store')

R1=ResourceRef('r1')
R2_FOO_BAR=ResourceRef('r2', 'foo/bar')
INTERMEDIATE_S1=ResourceRef('intermediate', 's1')
INTERMEDIATE_S2=ResourceRef('intermediate', 's2')
INTERMEDIATE_ROOT=ResourceRef('intermediate')
INTERMEDIATE_S1_SUBDIR=ResourceRef('intermediate', 's1/subdir')
RESULTS=ResourceRef=ResourceRef("results")

class TestLineageStoreCurrent(unittest.TestCase):
    """Tests for the lineage current store api
    """
    def setUp(self):
        if os.path.exists(TEMPDIR):
            shutil.rmtree(TEMPDIR)
        os.mkdir(TEMPDIR)
        os.mkdir(LOCAL_STORE_DIR)
        self.store = None

    def tearDown(self):
        shutil.rmtree(TEMPDIR)
        #pass

    def _assert_ds_hash(self, ref, expected_hash):
        (cert, lineage) = self.store.get_cert_and_lineage_for_ref(ref)
        self.assertIsInstance(lineage, SourceDataLineage)
        self.assertIsInstance(cert, HashCertificate)
        self.assertEqual(cert.hashval, expected_hash)

    def _assert_step_hash(self, ref, step_name, expected_hash):
        (cert, lineage) = self.store.get_cert_and_lineage_for_ref(ref)
        self.assertIsInstance(lineage, StepLineage)
        self.assertEqual(lineage.step_name, step_name)
        self.assertIsInstance(cert, HashCertificate)
        self.assertEqual(cert.hashval, expected_hash)

    def _assert_step_placeholder(self, ref, step_name, expected_version):
        (cert, lineage) = self.store.get_cert_and_lineage_for_ref(ref)
        self.assertIsInstance(lineage, StepLineage)
        self.assertEqual(lineage.step_name, step_name)
        self.assertIsInstance(cert, PlaceholderCertificate)
        self.assertEqual(cert.version, expected_version)



    def test_basic_senario(self):
        self.store = LineageStoreCurrent()
        s = self.store
        step1_lineage = StepLineage.make_step_lineage('step1', datetime.datetime.now(),
                                                     [('p1', 'v1'), ('p2', 5)],
                                                      [R1, R2_FOO_BAR], s)
        step1_lineage.add_output(s, INTERMEDIATE_S1)
        step1_lineage.execution_time_seconds = 5
        s.add_step(step1_lineage)
        step2_lineage = StepLineage.make_step_lineage('step2', datetime.datetime.now(),
                                                      [('p3', 'v3')],
                                                      [INTERMEDIATE_S1], s)
        step2_lineage.add_output(s, INTERMEDIATE_S2)
        step2_lineage.execution_time_seconds = 20
        s.add_step(step2_lineage)
        step3_lineage = StepLineage.make_step_lineage('step3', datetime.datetime.now(),
                                                      [('p4', 4)],
                                                      [R2_FOO_BAR,
                                                       INTERMEDIATE_S2], s)
        step3_lineage.add_output(s, RESULTS)
        step3_lineage.execution_time_seconds = 3
        s.add_step(step3_lineage)
        s.save(LOCAL_STORE_DIR)

        self.store = s = LineageStoreCurrent.load(LOCAL_STORE_DIR)
        warnings = s.replace_placeholders_with_real_certs({
            'r1':'r1hash',
            'r2':'r2hash',
            'intermediate':'intermediate_hash',
            'results':'results_hash'
        })
        self.assertEqual(0, warnings)
        s.save(LOCAL_STORE_DIR)
        self.store = s = LineageStoreCurrent.load(LOCAL_STORE_DIR)
        self._assert_ds_hash(R1, 'r1hash')
        self._assert_ds_hash(R2_FOO_BAR, 'r2hash')
        self._assert_step_hash(INTERMEDIATE_S1, 'step1', 'intermediate_hash')
        self._assert_step_hash(INTERMEDIATE_S2, 'step2', 'intermediate_hash')
        self._assert_step_hash(RESULTS, 'step3', 'results_hash')

        # Overwrite R2_FOO_BAR to create an inconsistent lineage
        step4_lineage = StepLineage.make_step_lineage('step4', datetime.datetime.now(),
                                                      [('p4', 'v4')],
                                                      [R1,], s)
        step4_lineage.add_output(s, R2_FOO_BAR)
        s.add_step(step4_lineage)
        self.assertEqual(s.validate([RESULTS]), 2)
        try:
            step3b_lineage = StepLineage.make_step_lineage('step3', datetime.datetime.now(),
                                                           [('p4', 5)], [R2_FOO_BAR, INTERMEDIATE_S2], s)
        except LineageConsistencyError as e:
            pass
            #print("Got expected consistency error: %s" % e)
        else:
            self.fail("Expecting a consistency error when creating step3b, but did not get one!")

        # now go back, and rerun steps to get our consistent output
        step1_lineage = StepLineage.make_step_lineage('step1', datetime.datetime.now(),
                                                     [('p1', 'v1'), ('p2', 5)],
                                                      [R1, R2_FOO_BAR], s)
        step1_lineage.add_output(s, INTERMEDIATE_S1)
        step1_lineage.execution_time_seconds = 5
        s.add_step(step1_lineage)
        #print(json.dumps(s.to_json(), indent=2))
        step2_lineage = StepLineage.make_step_lineage('step2', datetime.datetime.now(),
                                                      [('p3', 'v3')],
                                                      [INTERMEDIATE_S1], s)
        step2_lineage.add_output(s, INTERMEDIATE_S2)
        step2_lineage.execution_time_seconds = 20
        s.add_step(step2_lineage)
        step3_lineage = StepLineage.make_step_lineage('step3', datetime.datetime.now(),
                                                      [('p4', 4)],
                                                      [R2_FOO_BAR,
                                                       INTERMEDIATE_S2], s)
        step3_lineage.add_output(s, RESULTS)
        step3_lineage.execution_time_seconds = 3
        s.add_step(step3_lineage)

        self._assert_step_placeholder(R2_FOO_BAR, 'step4', 1)
        self._assert_ds_hash(R1, 'r1hash')
        self._assert_step_placeholder(INTERMEDIATE_S1, 'step1', 1)
        self._assert_step_placeholder(INTERMEDIATE_S2, 'step2', 1)
        self._assert_step_placeholder(RESULTS, 'step3', 1)

        # Rerun step3 and verify that we get an updated placeholder version
        step3_lineage = StepLineage.make_step_lineage('step3', datetime.datetime.now(),
                                                      [('p4', 4)],
                                                      [R2_FOO_BAR,
                                                       INTERMEDIATE_S2], s)
        step3_lineage.add_output(s, RESULTS)
        step3_lineage.execution_time_seconds = 5
        s.add_step(step3_lineage)
        self._assert_step_placeholder(RESULTS, 'step3', 2)
        self.assertEqual(s.validate([RESULTS]), 0)

        # update with snapshot. We add a situation where we
        # get a warning
        warnings = s.replace_placeholders_with_real_certs({
            'r1':'r1hash2',
            'r2':'r2hash2',
            'intermediate':'intermediate_hash2',
            'results':'results_hash2'
        })
        self.assertEqual(1, warnings)
        self.assertEqual(s.validate([RESULTS]), 0)
        s.save(LOCAL_STORE_DIR)
        self.store = s = LineageStoreCurrent.load(LOCAL_STORE_DIR)
        self._assert_ds_hash(R1, 'r1hash') # keeping the older version
        self._assert_step_hash(R2_FOO_BAR, 'step4', 'r2hash2')
        self._assert_step_hash(INTERMEDIATE_S1, 'step1', 'intermediate_hash2')
        self._assert_step_hash(INTERMEDIATE_S2, 'step2', 'intermediate_hash2')
        self._assert_step_hash(RESULTS, 'step3', 'results_hash2')
        self.assertEqual(s.validate([RESULTS]), 0)

    def test_inconsistent_writes(self):
        self.store = LineageStoreCurrent()
        s = self.store
        step1_lineage = StepLineage.make_step_lineage('step1', datetime.datetime.now(),
                                                     [('p1', 'v1'), ('p2', 5)],
                                                      [R1, R2_FOO_BAR], s)
        step1_lineage.add_output(s, INTERMEDIATE_S1)
        step1_lineage.execution_time_seconds = 5
        s.add_step(step1_lineage)
        step2_lineage = StepLineage.make_step_lineage('step2', datetime.datetime.now(),
                                                      [('p3', 'v3')],
                                                      [INTERMEDIATE_S1], s)
        try:
            step2_lineage.add_output(s, INTERMEDIATE_ROOT)
        except LineageError as e:
            pass
        else:
            self.fail("Expecting a lineage error for inconsistent writes, but did not get one")
        try:
            step2_lineage.add_output(s, INTERMEDIATE_S1_SUBDIR)
        except LineageError as e:
            pass
        else:
            self.fail("Expecting a lineage error for inconsistent writes, but did not get one")



if __name__ == '__main__':
    unittest.main()
