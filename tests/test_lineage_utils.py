import unittest
import os.path
from os.path import exists, join
import sys
import shutil
import datetime
import json
from copy import copy
from abc import ABCMeta, abstractmethod

try:
    import dataworkspaces
except ImportError:
    sys.path.append(os.path.abspath(".."))

from dataworkspaces.errors import LineageError
from dataworkspaces.utils.lineage_utils import \
    StepLineage, LineageStore, ResourceRef, SourceDataLineage,\
    LineageConsistencyError, InputPlaceholderCert, OutputPlaceholderCert,\
    PlaceholderCertificate, HashCertificate,\
    FileLineageStore, CodeLineage, make_lineage_graph_for_visualization,\
    LineagePlaceHolderError

KEEP_OUTPUTS = False

TEMPDIR=os.path.abspath(os.path.expanduser(__file__)).replace('.py', '_data')

R1=ResourceRef('r1')
R2_FOO_BAR=ResourceRef('r2', 'foo/bar')
INTERMEDIATE_S1=ResourceRef('intermediate', 's1')
INTERMEDIATE_S2=ResourceRef('intermediate', 's2')
INTERMEDIATE_S3=ResourceRef('intermediate', 's3')
INTERMEDIATE_ROOT=ResourceRef('intermediate')
INTERMEDIATE_S1_SUBDIR=ResourceRef('intermediate', 's1/subdir')
CODE=ResourceRef('code')
OUT4=ResourceRef('out4')
RESULTS=ResourceRef("results")
BASE_SNAPSHOT_HASHES={
    'r1':'r1hash',
    'r2':'r2hash',
    'intermediate':'intermediate_hash',
    'results':'results_hash',
    'code':'code_hash'
}

class TestResourceRef(unittest.TestCase):
    def test_covers(self):
        self.assertTrue(ResourceRef('foo', None).covers(ResourceRef('foo', 'bar')))
        self.assertTrue(ResourceRef('foo', 'bar').covers(ResourceRef('foo', 'bar/baz')))
        self.assertFalse(ResourceRef('foo', None).covers(ResourceRef('foo2', 'bar')))
        self.assertFalse(ResourceRef('foo', None).covers(ResourceRef('foo', None)))
        self.assertFalse(ResourceRef('foo', 'bar/baz').covers(ResourceRef('foo', 'bar')))
        self.assertFalse(ResourceRef('foo', 'bar/b').covers(ResourceRef('foo', 'bar/baz')),
                         "covers should be based on paths, not substrings")

class TestResourceCert(unittest.TestCase):
    def test_cert_equality(self):
        rc1 = HashCertificate(R2_FOO_BAR, 'hv1', 'comment1')
        rc1b = HashCertificate(R2_FOO_BAR, 'hv1', 'comment1')
        self.assertEqual(rc1, rc1b)
        self.assertTrue(rc1b==rc1)
        self.assertFalse(rc1b!=rc1)
        rc2 = HashCertificate(RESULTS, 'results_hash', 'comment_r')
        s = set([rc1, rc2])
        self.assertTrue(rc1b in s)
        self.assertFalse(rc1b not in s)
        rcp = InputPlaceholderCert(R2_FOO_BAR, 1, 'comment2')
        self.assertNotEqual(rcp, rc1)
        rch = rcp.create_hash_cert(rc1.hashval)
        self.assertEqual(rc1, rch)
        rcpo = OutputPlaceholderCert(R2_FOO_BAR, 1, 'comment')
        self.assertNotEqual(rcp, rcpo)




class TestStoreMixin(metaclass=ABCMeta):
    """This is a mixin for testing a LineageStore. It is independent of
    the implementation. You can use this as a building block to
    implement implementation-specific tests
    """
    @abstractmethod
    def _get_store(self): pass

    @abstractmethod
    def _get_instance(self): pass

    @abstractmethod
    def _make_another_store_instance(self):
        """Create another instance of the in-memory store, replacing the other.
        This is used to reload the store from its persistent representation
        """
        pass

    def _assert_datasource_hash(self, ref, expected_hash):
        lineage = self._get_store().retrieve_entry(self._get_instance(), ref)
        self.assertIsInstance(lineage, SourceDataLineage)
        self.assertIsInstance(lineage.cert, HashCertificate)
        self.assertEqual(lineage.cert.hashval, expected_hash)

    def _assert_step_hash(self, ref, step_name, expected_hash):
        lineage = self._get_store().retrieve_entry(self._get_instance(), ref)
        self.assertIsInstance(lineage, StepLineage)
        self.assertEqual(lineage.step_name, step_name)
        cert = lineage.get_cert_for_ref(ref)
        self.assertIsInstance(cert, HashCertificate)
        self.assertEqual(cert.hashval, expected_hash)

    def _run_step(self, name, inputs, outputs, params={}):
        lineage = StepLineage.make_step_lineage(self._get_instance(), name, datetime.datetime.now(),
                                                params, inputs, [CODE], self._get_store())
        for output in outputs:
            lineage.add_output(self._get_instance(), self._get_store(), output)
        lineage.execution_time_seconds = 5
        self._get_store().store_entry(self._get_instance(), lineage)

    def _run_initial_workflow(self, s3_outputs=[RESULTS], snapshot_hash_overrides={}):
        s = self._get_store()
        instance = self._get_instance()
        self._run_step('step1', [R1, R2_FOO_BAR], [INTERMEDIATE_S1], params={'p1':'v1', 'p2':5})
        self._run_step('step2', [INTERMEDIATE_S1], [INTERMEDIATE_S2])
        self._run_step('step3', [R2_FOO_BAR, INTERMEDIATE_S2], s3_outputs, params={'p4':4})

        # retrieve all these entries
        s1o = s.retrieve_entry(instance, INTERMEDIATE_S1)
        self.assertTrue(isinstance(s1o, StepLineage))
        self.assertEqual(s1o.step_name, 'step1')
        s1i1 = s.retrieve_entry(instance, R1)
        self.assertTrue(isinstance(s1i1, SourceDataLineage))
        self.assertTrue(isinstance(s1i1.cert, PlaceholderCertificate))
        self.assertEqual(s1i1.cert.version, 1)
        s1i2 = s.retrieve_entry(instance, R2_FOO_BAR)
        self.assertTrue(isinstance(s1i2, SourceDataLineage))
        self.assertTrue(isinstance(s1i2.cert, PlaceholderCertificate))
        self.assertEqual(s1i2.cert.version, 1)
        s2o = s.retrieve_entry(instance, INTERMEDIATE_S2)
        self.assertTrue(isinstance(s2o, StepLineage))
        self.assertEqual(s2o.step_name, 'step2')
        for output in s3_outputs:
            s3o = s.retrieve_entry(instance, output)
            self.assertTrue(isinstance(s3o, StepLineage))
            self.assertEqual(s3o.step_name, 'step3')
        code_lineage = s.retrieve_entry(instance, CODE)
        self.assertTrue(isinstance(code_lineage, CodeLineage))
        self.assertTrue(isinstance(code_lineage.cert, PlaceholderCertificate))
        self.assertEqual(code_lineage.cert.version, 1)

        snapshot_hash_values=copy(BASE_SNAPSHOT_HASHES)
        for (k, v) in snapshot_hash_overrides.items():
            snapshot_hash_values[k] = v
        s.replace_placeholders(instance, snapshot_hash_values)
        self._assert_datasource_hash(R1, snapshot_hash_values['r1'])
        self._assert_datasource_hash(R2_FOO_BAR, snapshot_hash_values['r2'])

        self._assert_step_hash(INTERMEDIATE_S1, 'step1', snapshot_hash_values['intermediate'])
        self._assert_step_hash(INTERMEDIATE_S2, 'step2', snapshot_hash_values['intermediate'])
        self._assert_step_hash(RESULTS, 'step3', snapshot_hash_values['results'])

        s.snapshot_lineage(instance, 'snapshot1', snapshot_hash_values.keys())

    def test_basic_scenaio(self):
        self._run_initial_workflow()

    def test_inconsistency(self):
        self._run_initial_workflow()
        s = self._get_store()
        instance = self._get_instance()

        # Overwrite RS_FOO_BAR to create an inconsistent lineage
        self._run_step('step4', [R1], [R2_FOO_BAR], params={'p4':"v4"})
        # Now, the store has the new version that was just written by step4,
        # but step1 is reading a previously hashed version.
        try:
            self._run_step('step3b', [R2_FOO_BAR, INTERMEDIATE_S2], [])
        except LineageConsistencyError as e:
            print("Got expected consistency error: %s" % e)
        else:
            self.fail("Expecting a consistency error when creating step3b, but did not get one!")

    def test_placeholder_substitution(self):
        """
        """
        s = self._get_store()
        instance = self._get_instance()
        self._run_step('step1', [R1], [INTERMEDIATE_S1])
        r1_lineage = s.retrieve_entry(instance, R1)
        self.assertTrue(isinstance(r1_lineage, SourceDataLineage))
        self.assertTrue(isinstance(r1_lineage.get_cert_for_ref(R1), InputPlaceholderCert))
        is1_lineage = s.retrieve_entry(instance, INTERMEDIATE_S1)
        self.assertTrue(isinstance(is1_lineage, StepLineage))
        self.assertTrue(isinstance(is1_lineage.get_cert_for_ref(INTERMEDIATE_S1), OutputPlaceholderCert))
        s.replace_placeholders(instance, {'r1':'r1_snapshot1', 'intermediate':'i_snapshot1', 'code':'code_hash'})
        s.snapshot_lineage(instance, 'snapshot1', ['r1', 'code', 'intermediate'])
        r1_lineage = s.retrieve_entry(instance, R1)
        self.assertTrue(isinstance(r1_lineage, SourceDataLineage))
        self.assertTrue(isinstance(r1_lineage.get_cert_for_ref(R1), HashCertificate))
        is1_lineage = s.retrieve_entry(instance, INTERMEDIATE_S1)
        self.assertTrue(isinstance(is1_lineage, StepLineage))
        self.assertTrue(isinstance(is1_lineage.get_cert_for_ref(INTERMEDIATE_S1), HashCertificate))

        self._run_step('step2', [R1, INTERMEDIATE_S1], [INTERMEDIATE_S2])
        s2_lineage = s.retrieve_entry(instance, INTERMEDIATE_S2)
        self.assertTrue(isinstance(s2_lineage, StepLineage))
        self.assertTrue(isinstance(s2_lineage.get_cert_for_ref(INTERMEDIATE_S2), OutputPlaceholderCert))
        r1_lineage2 = s.retrieve_entry(instance, R1)
        self.assertEqual(r1_lineage2, r1_lineage)

    def test_inconsistent_writes(self):
        s = self._get_store()
        instance = self._get_instance()
        self._run_step('step1', [R1, R2_FOO_BAR], [INTERMEDIATE_S1])
        try:
            self._run_step('step2', [INTERMEDIATE_S1], [INTERMEDIATE_ROOT])
        except LineageError as e:
            pass
        else:
            self.fail("Expecting a lineage error for inconsistent writes, but did not get one")
        try:
            self._run_step('step2', [INTERMEDIATE_S1], [INTERMEDIATE_S1_SUBDIR])
        except LineageError as e:
            pass
        else:
            self.fail("Expecting a lineage error for inconsistent writes, but did not get one")


    def test_invalidation(self):
        """Test the invalidation of resources. This is used for
        situations where a resource may have changed and the
        state is unknown (e.g. the resource was pulled from a remote source)
        """
        self._run_initial_workflow(s3_outputs=[RESULTS, INTERMEDIATE_S3])
        s = self._get_store()
        instance = self._get_instance()
        self.assertTrue(s.has_entry(instance, RESULTS))
        self.assertTrue(s.has_entry(instance, INTERMEDIATE_S2))
        self.assertTrue(s.has_entry(instance, INTERMEDIATE_S3))

        s.clear_entry(instance, RESULTS)
        s.clear_entry(instance, INTERMEDIATE_ROOT)
        self.assertFalse(s.has_entry(instance, RESULTS))
        self.assertFalse(s.has_entry(instance, INTERMEDIATE_S2))
        self.assertFalse(s.has_entry(instance, INTERMEDIATE_S3))

    def test_invalidation_in_step(self):
        """Test the invalidation of a step's outputs before the step is saved"""
        self._run_initial_workflow(s3_outputs=[RESULTS, INTERMEDIATE_S3])
        s = self._get_store()
        instance = self._get_instance()
        step3_lineage = StepLineage.make_step_lineage(instance, 'step3', datetime.datetime.now(),
                                                      {},
                                                      [R2_FOO_BAR,
                                                       INTERMEDIATE_S2], [CODE], s)
        step3_lineage.add_output(instance, s, RESULTS)
        step3_lineage.add_output(instance, s, INTERMEDIATE_S3)
        step3_lineage.add_output(instance, s, OUT4)

        for cert in step3_lineage.get_certs():
            s.clear_entry(instance, cert.ref)
        # inputs should be still fine
        self.assertTrue(s.has_entry(instance, R2_FOO_BAR))
        self.assertTrue(s.has_entry(instance, INTERMEDIATE_S2))
        self.assertFalse(s.has_entry(instance, RESULTS))
        self.assertFalse(s.has_entry(instance, INTERMEDIATE_S3))
        self.assertFalse(s.has_entry(instance, OUT4))

    def test_unreplaced_placeholders(self):
        s = self._get_store()
        instance = self._get_instance()
        self._run_step('step1', [R1, R2_FOO_BAR], [INTERMEDIATE_S1], params={'p1':'v1', 'p2':5})
        self._run_step('step2', [INTERMEDIATE_S1], [INTERMEDIATE_S2])
        self._run_step('step3', [R2_FOO_BAR, INTERMEDIATE_S2], [RESULTS], params={'p4':4})
        try:
            s.replace_placeholders(instance, {'intermediate':'i_snapshot1', 'code':'code_hash'})
        except LineagePlaceHolderError as e:
            print("Got the expected missing placeholder error: %s"%e)
        else:
            self.fail("Did not get expected missing placeholder error for R1")

    def _get_ref_to_hash(self):
        ref_to_hash = {}
        s = self._get_store()
        instance = self._get_instance()
        for (ref, lineage) in s.iterate_all(instance):
            cert = lineage.get_cert_for_ref(ref)
            self.assertTrue(isinstance(cert, HashCertificate),
                            "certificate %s for ref %s is not a hash!" %
                            (cert, ref))
            ref_to_hash[ref] = cert.hashval
        return ref_to_hash

    def test_snapshot_and_restore(self):
        RESOURCE_NAMES=['r1', 'r2', 'results', 'intermediate', 'code', 'out4']
        s = self._get_store()
        instance = self._get_instance()

        # save a first snapshot
        self._run_initial_workflow(s3_outputs=[RESULTS, OUT4], snapshot_hash_overrides={'out4':'out4hash'})
        self._assert_step_hash(OUT4, 'step3', 'out4hash')
        s1_ref_to_hash = self._get_ref_to_hash()
        # run a second version of step 3 and take a new snapshot
        # This version does not write to OUT4
        self._run_step('step3b', [R2_FOO_BAR, INTERMEDIATE_S2], [RESULTS])
        new_hashes = {
            'r1':'r1hash',
            'r2':'r2hash',
            'intermediate':'intermediate_hash',
            'results':'results_hash_v2',
            'code':'code_hash',
        }
        s.replace_placeholders(instance, new_hashes)
        s.snapshot_lineage(instance, 'snapshot2', RESOURCE_NAMES)
        s2_ref_to_hash = self._get_ref_to_hash()

        s.restore_lineage(instance, 'snapshot1', RESOURCE_NAMES, verbose=False)
        s1_restored_ref_to_hash = self._get_ref_to_hash()
        self.assertEqual(s1_ref_to_hash, s1_restored_ref_to_hash)

        s.restore_lineage(instance, 'snapshot2', RESOURCE_NAMES, verbose=False)
        s2_restored_ref_to_hash = self._get_ref_to_hash()
        self.assertEqual(s2_ref_to_hash, s2_restored_ref_to_hash)

    def test_loading(self):
        """Run a workflow and then reload the store from disk. Make sure it is eqivalent.
        """
        instance = self._get_instance()
        self._run_initial_workflow(s3_outputs=[RESULTS, OUT4], snapshot_hash_overrides={'out4':'out4hash'})
        s= self._get_store()
        initial_store = sorted([(ref, lineage.get_cert_for_ref(ref)) for (ref, lineage) in s.iterate_all(instance)])

        self._make_another_store_instance()
        s2 = self._get_store()
        second_store = sorted([(ref, lineage.get_cert_for_ref(ref)) for (ref, lineage) in s2.iterate_all(instance)])
        self.assertEqual(initial_store, second_store)
        

LOCAL_STORE_DIR=os.path.join(TEMPDIR, 'local_store')
SNAPSHOT_DIR=os.path.join(TEMPDIR, 'lineage_snapshots')
SNAPSHOT1_DIR=os.path.join(SNAPSHOT_DIR, 'snapshot1')
SNAPSHOT2_DIR=os.path.join(SNAPSHOT_DIR, 'snapshot2')

class TestFileLineageStore(unittest.TestCase, TestStoreMixin):
    """Tests for the lineage store api file-based implementation"""
    def setUp(self):
        if os.path.exists(TEMPDIR):
            shutil.rmtree(TEMPDIR)
        os.mkdir(TEMPDIR)
        os.mkdir(LOCAL_STORE_DIR)
        os.mkdir(SNAPSHOT_DIR)
        self.store = FileLineageStore('test_inst', LOCAL_STORE_DIR, SNAPSHOT_DIR)

    def _get_store(self):
        return self.store

    def _make_another_store_instance(self):
        self.store = FileLineageStore('test_inst', LOCAL_STORE_DIR, SNAPSHOT_DIR)

    def _get_instance(self):
        return 'test_inst'

    def tearDown(self):
        if exists(TEMPDIR) and not KEEP_OUTPUTS:
            shutil.rmtree(TEMPDIR)



    # def test_get_lineage_for_resource(self):
    #     s = self._run_initial_workflow()
    #     (lineages, complete) = s.get_lineage_for_resource('results')
    #     self.assertTrue(complete)
    #     rclist = []
    #     for l in lineages:
    #         rclist.extend(l.get_resource_certificates())
    #         if isinstance(l, StepLineage):
    #             print("  step %s" % l.step_name)
    #         else:
    #             print("  data source %s" % l.resource_cert)
    #     def check_for_rc(ref, hashval):
    #         for rc in rclist:
    #             if rc.ref==ref and rc.certificate.hashval==hashval:
    #                 return
    #         self.fail("Did not find an rc %s %s in rc list %s" %
    #                   (ref, hashval, rclist))
    #     check_for_rc(RESULTS, 'results_hash')
    #     check_for_rc(INTERMEDIATE_S2, 'intermediate_hash')
    #     check_for_rc(INTERMEDIATE_S1, 'intermediate_hash')
    #     check_for_rc(R2_FOO_BAR, 'r2hash')
    #     check_for_rc(R1, 'r1hash')
    #     self.assertEqual(len(rclist), 5)
    #     self.assertEqual(len(lineages), 5)

    #     # test case for an inconsistent lineage
    #     step1_lineage = StepLineage.make_step_lineage('step1', datetime.datetime.now(),
    #                                                  [('p1', 'v1'), ('p2', 5)],
    #                                                   [R1, R2_FOO_BAR], [CODE], s)
    #     step1_lineage.add_output(s, INTERMEDIATE_S1)
    #     step1_lineage.execution_time_seconds = 5
    #     s.add_step(step1_lineage)
    #     (lineages, complete) = s.get_lineage_for_resource('results')
    #     self.assertFalse(complete)

    #     # test case where we don't have any lineage for a resource
    #     (lineages, complete) = s.get_lineage_for_resource('non-existent')
    #     self.assertFalse(complete)
    #     self.assertEqual(len(lineages), 0)

if __name__ == '__main__':
    if len(sys.argv)>1 and sys.argv[1]=='--keep-outputs':
        KEEP_OUTPUTS=True
        del sys.argv[1]
        print("--keep-outputs specified, will skip cleanup steps")
    unittest.main()
