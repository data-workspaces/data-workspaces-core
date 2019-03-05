import unittest
import os.path
from os.path import exists, join
import sys
import shutil
import datetime
import json
from copy import copy

try:
    import dataworkspaces
except ImportError:
    sys.path.append(os.path.abspath(".."))

from dataworkspaces.errors import LineageError
from dataworkspaces.utils.lineage_utils import \
    StepLineage, LineageStoreCurrent, ResourceRef, SourceDataLineage,\
    LineageConsistencyError, PlaceholderCertificate, HashCertificate,\
    ResourceCert

TEMPDIR=os.path.abspath(os.path.expanduser(__file__)).replace('.py', '_data')
LOCAL_STORE_DIR=os.path.join(TEMPDIR, 'local_store')
SNAPSHOT1_DIR=os.path.join(TEMPDIR, 'snapshot1')
SNAPSHOT2_DIR=os.path.join(TEMPDIR, 'snapshot2')

R1=ResourceRef('r1')
R2_FOO_BAR=ResourceRef('r2', 'foo/bar')
INTERMEDIATE_S1=ResourceRef('intermediate', 's1')
INTERMEDIATE_S2=ResourceRef('intermediate', 's2')
INTERMEDIATE_S3=ResourceRef('intermediate', 's3')
INTERMEDIATE_ROOT=ResourceRef('intermediate')
INTERMEDIATE_S1_SUBDIR=ResourceRef('intermediate', 's1/subdir')
OUT4=ResourceRef('out4')
RESULTS=ResourceRef=ResourceRef("results")
BASE_SNAPSHOT_HASHES={
    'r1':'r1hash',
    'r2':'r2hash',
    'intermediate':'intermediate_hash',
    'results':'results_hash'
}

class TestResourceCert(unittest.TestCase):
    def test_rc(self):
        rc1 = ResourceCert(R2_FOO_BAR, HashCertificate('hv1', 'comment1'))
        rc1b = ResourceCert(R2_FOO_BAR, HashCertificate('hv1', 'comment1'))
        self.assertEqual(rc1, rc1b)
        self.assertTrue(rc1b==rc1)
        self.assertFalse(rc1b!=rc1)
        rc2 = ResourceCert(RESULTS, HashCertificate('results_hash', 'comment_r'))
        s = set([rc1, rc2])
        self.assertTrue(rc1b in s)
        self.assertFalse(rc1b not in s)

class TestLineageStoreCurrent(unittest.TestCase):
    """Tests for the lineage current store api
    """
    def setUp(self):
        if os.path.exists(TEMPDIR):
            shutil.rmtree(TEMPDIR)
        os.mkdir(TEMPDIR)
        os.mkdir(LOCAL_STORE_DIR)
        os.mkdir(SNAPSHOT1_DIR)
        os.mkdir(SNAPSHOT2_DIR)
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


    def _run_initial_workflow(self, s3_outputs=[RESULTS], snapshot_hash_overrides={}):
        """Common initial workflow for use in multiple test scenarios
        """
        s = self.store = LineageStoreCurrent()
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
        for output in s3_outputs:
            step3_lineage.add_output(s, output)
        step3_lineage.execution_time_seconds = 3
        s.add_step(step3_lineage)
        s.save(LOCAL_STORE_DIR)

        self.store = s = LineageStoreCurrent.load(LOCAL_STORE_DIR)
        snapshot_hash_values=copy(BASE_SNAPSHOT_HASHES)
        for (k, v) in snapshot_hash_overrides.items():
            snapshot_hash_values[k] = v
        warnings = s.replace_placeholders_with_real_certs(snapshot_hash_values)
        self.assertEqual(0, warnings)
        s.save(LOCAL_STORE_DIR)
        self.store = s = LineageStoreCurrent.load(LOCAL_STORE_DIR)
        self._assert_ds_hash(R1, snapshot_hash_values['r1'])
        self._assert_ds_hash(R2_FOO_BAR, snapshot_hash_values['r2'])
        self._assert_step_hash(INTERMEDIATE_S1, 'step1', snapshot_hash_values['intermediate'])
        self._assert_step_hash(INTERMEDIATE_S2, 'step2', snapshot_hash_values['intermediate'])
        self._assert_step_hash(RESULTS, 'step3', snapshot_hash_values['results'])
        return s

    def test_basic_scenario(self):
        s = self._run_initial_workflow()

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
        s = self.store = LineageStoreCurrent()
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

    def test_invalidation(self):
        """Test the in-memory and filesystem invalidation methods. These
        are used for situations where a resource may have changed and the
        state is unknown.
        """
        s = self._run_initial_workflow(s3_outputs=[RESULTS, INTERMEDIATE_S3])
        s.get_cert_and_lineage_for_ref(RESULTS) # should not throw KeyError
        s.get_cert_and_lineage_for_ref(INTERMEDIATE_S3) # should not throw KeyError
        s.get_cert_and_lineage_for_ref(INTERMEDIATE_S2) # should not throw KeyError

        # invalidate on the filesystem
        LineageStoreCurrent.invalidate_fsstore_entries(LOCAL_STORE_DIR,
                                                       ['intermediate', 'results'])
        self.assertFalse(exists(join(LOCAL_STORE_DIR, 'intermediate.json')))
        self.assertFalse(exists(join(LOCAL_STORE_DIR, 'results.json')))
        self.assertTrue(exists(join(LOCAL_STORE_DIR, 'r1.json')))
        self.assertTrue(exists(join(LOCAL_STORE_DIR, 'r2.json')))

        # invalidate a step's output (e.g. step failed)
        step3_lineage = StepLineage.make_step_lineage('step3', datetime.datetime.now(),
                                                      [('p4', 4)],
                                                      [R2_FOO_BAR,
                                                       INTERMEDIATE_S2], s)
        step3_lineage.add_output(s, RESULTS)
        step3_lineage.add_output(s, INTERMEDIATE_S3)
        s.invalidate_step_outputs(step3_lineage.output_resources)
        with self.assertRaises(KeyError):
            s.get_cert_and_lineage_for_ref(RESULTS)
        with self.assertRaises(KeyError):
            s.get_cert_and_lineage_for_ref(INTERMEDIATE_S3)
        s.get_cert_and_lineage_for_ref(INTERMEDIATE_S2) # should not throw KeyError

    def test_snapshot_and_restore(self):
        # save a first snapshot
        RESOURCE_NAMES=['r1', 'r2', 'results', 'intermediate']
        s = self._run_initial_workflow()
        self.assertEqual(set(LineageStoreCurrent.get_resource_names_in_fsstore(LOCAL_STORE_DIR)),
                         set(RESOURCE_NAMES))
        (files, warnings) = \
            LineageStoreCurrent.copy_fsstore_to_snapshot(LOCAL_STORE_DIR,
                                                         SNAPSHOT1_DIR,
                                                         RESOURCE_NAMES+['dummy'])
        self.assertEqual(1, warnings) # no lineage data available for dummy

        # warning situation in save
        s = self._run_initial_workflow(s3_outputs=[RESULTS, INTERMEDIATE_S3, OUT4],
                                       snapshot_hash_overrides={'out4':'out4hash',
                                                                'results':'resultshash2'})
        (files, warnings) = \
            LineageStoreCurrent.copy_fsstore_to_snapshot(LOCAL_STORE_DIR,
                                                         SNAPSHOT2_DIR,
                                                         RESOURCE_NAMES)
        self.assertEqual(1, warnings) # out4 has lineage data, but not included in snapshot

        # Save the real snapshot 2
        shutil.rmtree(SNAPSHOT2_DIR)
        os.makedirs(SNAPSHOT2_DIR)
        (files, warnings) = \
            LineageStoreCurrent.copy_fsstore_to_snapshot(LOCAL_STORE_DIR,
                                                         SNAPSHOT2_DIR,
                                                         RESOURCE_NAMES+['out4'])
        self.assertEqual(0, warnings)

        # restore the first snapshot
        warnings = LineageStoreCurrent.restore_store_from_snapshot(SNAPSHOT1_DIR,
                                                                   LOCAL_STORE_DIR,
                                                                   RESOURCE_NAMES)
        self.assertEqual(0, warnings)
        s = self.store = LineageStoreCurrent.load(LOCAL_STORE_DIR)
        self._assert_ds_hash(R1, 'r1hash')
        self._assert_ds_hash(R2_FOO_BAR, 'r2hash')
        self._assert_step_hash(INTERMEDIATE_S1, 'step1', 'intermediate_hash')
        self._assert_step_hash(INTERMEDIATE_S2, 'step2', 'intermediate_hash')
        self._assert_step_hash(RESULTS, 'step3', 'results_hash')
        self.assertFalse(exists(join(SNAPSHOT1_DIR, 'out4.json')))

        # restore the second snapshot
        warnings = LineageStoreCurrent.restore_store_from_snapshot(SNAPSHOT2_DIR,
                                                                   LOCAL_STORE_DIR,
                                                                   RESOURCE_NAMES+['out4'])
        self.assertEqual(0, warnings)
        s = self.store = LineageStoreCurrent.load(LOCAL_STORE_DIR)
        self._assert_ds_hash(R1, 'r1hash')
        self._assert_ds_hash(R2_FOO_BAR, 'r2hash')
        self._assert_step_hash(INTERMEDIATE_S1, 'step1', 'intermediate_hash')
        self._assert_step_hash(INTERMEDIATE_S2, 'step2', 'intermediate_hash')
        self._assert_step_hash(RESULTS, 'step3', 'resultshash2')
        self._assert_step_hash(OUT4, 'step3', 'out4hash')

        # another warning situation
        # TODO: this leaves a placeholder in the store. Should we give a warning?
        # That would require reading the files when copying them over.
        step3_lineage = StepLineage.make_step_lineage('step3', datetime.datetime.now(),
                                                      [('p4', 4)],
                                                      [R2_FOO_BAR,
                                                       INTERMEDIATE_S2], s)
        step3_lineage.add_output(s, RESULTS)
        step3_lineage.execution_time_seconds = 3
        s.add_step(step3_lineage)
        s.save(LOCAL_STORE_DIR)
        (files, warnings) = \
            LineageStoreCurrent.copy_fsstore_to_snapshot(LOCAL_STORE_DIR,
                                                         SNAPSHOT1_DIR,
                                                         RESOURCE_NAMES)
        self.assertEqual(warnings, 1) # old out4 is still in on-disk store
        warnings = LineageStoreCurrent.restore_store_from_snapshot(SNAPSHOT1_DIR,
                                                                   LOCAL_STORE_DIR,
                                                                   RESOURCE_NAMES)
        self.assertEqual(0, warnings) 

    def test_get_lineage_for_resource(self):
        s = self._run_initial_workflow()
        (lineages, complete) = s.get_lineage_for_resource('results')
        self.assertTrue(complete)
        rclist = []
        for l in lineages:
            rclist.extend(l.get_resource_certificates())
            if isinstance(l, StepLineage):
                print("  step %s" % l.step_name)
            else:
                print("  data source %s" % l.resource_cert)
        def check_for_rc(ref, hashval):
            for rc in rclist:
                if rc.ref==ref and rc.certificate.hashval==hashval:
                    return
            self.fail("Did not find an rc %s %s in rc list %s" %
                      (ref, hashval, rclist))
        check_for_rc(RESULTS, 'results_hash')
        check_for_rc(INTERMEDIATE_S2, 'intermediate_hash')
        check_for_rc(INTERMEDIATE_S1, 'intermediate_hash')
        check_for_rc(R2_FOO_BAR, 'r2hash')
        check_for_rc(R1, 'r1hash')
        self.assertEqual(len(rclist), 5)
        self.assertEqual(len(lineages), 5)

        # test case for an inconsistent lineage
        step1_lineage = StepLineage.make_step_lineage('step1', datetime.datetime.now(),
                                                     [('p1', 'v1'), ('p2', 5)],
                                                      [R1, R2_FOO_BAR], s)
        step1_lineage.add_output(s, INTERMEDIATE_S1)
        step1_lineage.execution_time_seconds = 5
        s.add_step(step1_lineage)
        (lineages, complete) = s.get_lineage_for_resource('results')
        self.assertFalse(complete)

        # test case where we don't have any lineage for a resource
        (lineages, complete) = s.get_lineage_for_resource('non-existent')
        self.assertFalse(complete)
        self.assertEqual(len(lineages), 0)

if __name__ == '__main__':
    unittest.main()
