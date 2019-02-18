
"""
Utilities for data lineage files

For now, the resource overwriting rule is that, we either completely
match a subpath and replace it, or there is no intersection.
"""

import datetime
from os.path import exists
from typing import List, Any, Optional, Tuple, NamedTuple, Set
from copy import copy


from dataworkspaces.errors import InternalError, LineageError 

class JsonKeyError(InternalError):
    def __init__(self, classobj, key, filename=None):
        if filename is not None:
            super().__init__("Error parsing %s in file %s: missing key %s" % (classobj.__name__, filename, key))
        else:
            super().__init__("Error parsing %s: missing key %s " % (classobj.__name__, key))

class JsonTypeError(InternalError):
    def __init__(self, classobj, exptype, actualtype, filename=None):
        if filename is not None:
            super().__init__("Error parsing %s in file %s: expecting a %s, but got a %s" % (classobj.__name__, filename, exptype, actualtype))
        else:
            super().__init__("Error parsing %s: expecting a %s, but got a %s" % (classobj.__name__, exptype, actualtype))

class JsonValueError(InternalError):
    def __init__(self, classobj, key, expected_vals, actualval, filename=None):
        if filename is not None:
            super().__init__("Error parsing %s in file %s: key %s has value %s, valid values are: %s"%
                             (classobj.__name__, filename, key, actualval, ', '.join(expected_vals)))
        else:
            super().__init__("Error parsing %s: key %s has value %s, valid values are: %s"%
                             (classobj.__name__, key, actualval, ', '.join(expected_vals)))


def validate_json_keys(obj, classobj, keys, filename=None):
    if not isinstance(obj, dict):
        raise JsonTypeError(classobj, dict, type(obj), filename=filename)
    for key in keys:
        if key not in obj:
            raise JsonKeyError(classobj, key, filename=filename)


class ResourceRef(NamedTuple):
    name: str
    subpath: Optional[str] = None


class Certificate:
    @staticmethod
    def from_json(obj:Any, filename:Optional[str]=None):
        validate_json_keys(obj, Certificate, ['cert_type',], filename=filename)
        cert_type = obj['cert_type']
        if cert_type == 'hash':
            validate_json_keys(obj, HashCertificate, ['hashval'], filename=filename)
            return HashCertificate(obj['hashval'])
        elif cert_type=='placeholder':
            validate_json_keys(obj, PlaceholderCertificate, ['version', 'comment'], filename=filename)
            return PlaceholderCertificate(obj['version'], obj['comment'])
        else:
            raise JsonValueError(Certificate, 'cert_type', ['hash', 'placeholder'], cert_type)

class HashCertificate(Certificate):
    __slots__ = ('hashval',)
    def __init__(self, hashval:str):
        self.hashval = hashval

    def __hash__(self):
        return hash(self.hashval)

    def __repr__(self):
        return 'HashCertificate(hashval=%s)' % self.hashval

    def __str__(self):
        return self.__repr__()

    def __eq__(self, other):
        return isinstance(other, HashCertificate) and other.hashval==self.hashval

    def __ne__(self, other):
        return (not isinstance(other, HashCertificate)) or other.hashval!=self.hashval

    def to_json(self):
        return {'cert_type':'hash', 'hashval':self.hashval}


class PlaceholderCertificate(Certificate):
    __slots__ = ('version', 'comment')
    def __init__(self, version:int, comment:str):
        self.version = version
        self.comment = comment

    def __str__(self):
        return self.__repr__()

    def __repr__(self):
        return 'PlaceholderCertificate(version=%d, comment="%s")' % (self.version, self.comment)

    def __hash__(self):
        return hash(self.version)

    def __eq__(self, other):
        equal = isinstance(other, PlaceholderCertificate) and other.version==self.version
        if equal and other.comment!=self.comment:
            raise TypeError("Two placeholder certificates equal in versions, but not in comments: '%s' and '%s'" %
                            (self.comment, other.comment))
        else:
            return equal

    def __ne__(self, other):
        return (not isinstance(other, PlaceholderCertificate)) or other.versions!=self.version

    def to_json(self):
        return {'cert_type':'placeholder', 'version':self.version,
                'comment':self.comment}


class ResourceCert:
    """Combination of a resource name and a certificate
    """
    __slots__ = ('ref', 'certificate')
    def __init__(self, ref:ResourceRef, certificate:Certificate):
        self.ref = ref
        self.certificate = certificate

    def __hash__(self):
        return hash((self.ref, self.certificate),)

    def __eq__(self, other):
        return isinstance(other, ResourceCert) and other.ref==self.ref and other.certificate==self.certificate

    def __ne__(self, other):
        return (not isinstance(other, ResourceCert)) or other.ref!=self.ref or other.certificate!=self.certificate

    def to_json(self):
        return {'resource_name':self.ref.name, 'subpath':self.ref.subpath,
                'certificate':self.certificate}

    def __repr__(self):
        return 'ResourceCert(ref=%s, certificate=%s)' % (repr(self.ref), repr(self.certificate))

    def __str__(self):
        return 'ResourceCert(ref=%s, certificate=%s)' % (str(self.ref), str(self.certificate))

    @staticmethod
    def from_json(obj, filename=None):
        validate_json_keys(obj, ResourceCert, ['resource_name', 'certificate'],
                           filename=filename)
        return ResourceCert(ResourceRef(obj['resource_name'], obj.get('subpath', None)),
                            Certificate.from_json(obj['certificate'],
                                                  filename=filename))


class ResourceLineage:
    """Base class for the lineage of a resource, either a step that wrote it
    or a source data snapshot
    """
    @staticmethod
    def from_json(obj, filename=None):
        validate_json_keys(obj, ResourceLineage, ['type'], filename=filename)
        restype = obj['type']
        if restype=='step':
            return StepLineage.from_json(obj, filename=filename)
        elif restype=='source_data':
            return SourceDataLineage.from_json(obj, filename=filename)
        else:
            raise JsonValueError(ResourceLineage, 'type', ['step', 'source_data'], restype)

    def verify_compatble(self, resource_name, new_lineage):
        """Given a new lineage which will update this one
        (either data source or step), verify that the update is compatible.
        """
        nl_subpaths = new_lineage.get_subpaths_for_output(resource_name)
        ol_subpaths = self.get_subpaths_for_output(resource_name)
        def map_subpath(plist):
            return [(p if p is not None else '/') for p in plist]
        if nl_subpaths != ol_subpaths:
            raise LineageError(("Error updating lineage for resource %s: "+
                                "subpaths do not match previous lineage. Current "+
                                "paths: %s, new paths: %s")%
                               (resource_name,
                                ', '.join(sorted(map_subpath(ol_subpaths))),
                                ', '.join(sorted(map_subpath(nl_subpaths)))))

    def get_resource_cert_for_resource(self, resource_name:str,
                                       subpath:Optional[str]=None) -> \
                                       Optional[ResourceCert]:
        """Return the resource cert if this resource/subpath are contained
        in this lineage object. If not, return None
        """
        raise NotImplementedError(self.__class__.__name__)

    def validate_subpath_compatible(self, resource_name:str,
                                    subpath:Optional[str]=None) -> None:
        """Validate that specified subpath would be compatible with
        the existing subpath(s) for this resource. Throws a LineageError
        if there is a problem.
        """
        raise NotImplementedError(self.__class__.__name__)

class StepLineage(ResourceLineage):
    __slots__ = ['step_name', 'start_time', 'parameters', 'input_resources',
                 'output_resources', 'execution_time_seconds']
    def __init__(self, step_name:str, start_time:datetime.datetime,
                 parameters:List[Tuple[str, Any]],
                 input_resources:List[ResourceCert],
                 output_resources:Optional[List[ResourceCert]]=None,
                 execution_time_seconds:Optional[float]=None):
        self.step_name = step_name
        self.start_time = start_time
        self.parameters = parameters
        self.input_resources = input_resources
        self.output_resources = output_resources
        self.execution_time_seconds = execution_time_seconds
        self.outputs_by_resource = {} # map from output resource to set of paths
        if output_resources is not None:
            # build the map of outputs by resource
            for rc in output_resources:
                if rc.resource_name in self.outputs_by_resource:
                    s = self.outputs_by_resource[rc.resource_name]
                else:
                    s = set()
                    self.outputs_by_resource[rc.resource_name] = s
                for p in s:
                    self._validate_paths_compatible(rc.resource_name,
                                                    p, rc.subpath)
                s.add(rc.subpath)

    @staticmethod
    def make_step_lineage(step_name:str, start_time:datetime.datetime,
                          parameters:List[Tuple[str, Any]],
                          input_resource_refs:List[ResourceRef],
                          lineage_store:'LineageStoreCurrent'):
        """At the start of a step's run, create a step lineage object
        to be updated as the step progesses. Validates that the inputs
        to the step are consistent.
        """
        input_certs = [lineage_store.get_or_create_lineage(step_name, start_time, ref)
                       for ref in input_resource_refs] # List[ResourceCert]
        # validate that only one dependent version for each input version
        transitive_certs = set(input_certs)
        to_process = copy(input_certs)
        while len(to_process)>0:
            next_to_process = []
            for rc in to_process:
                lineage = lineage_store.get_lineage_for_cert(rc)
                other_cert = lineage.get_resource_cert_for_resource(rc.name,
                                                                    rc.subpath)
                assert other_cert is not None
                if other_cert!=rc:
                    raise LineageError("Step %s depends on two version of %s: %s and %s"
                                       %(step_name, rc.ref(), other_cert, rc))
                if isinstance(lineage, StepLineage):
                    for input_rc in lineage.input_resources:
                        if input_rc not in transitive_certs:
                            transitive_certs.add(input_rc)
                            next_to_process.append(input_rc)
            to_process = next_to_process
        # if we got here, we didn't find any inconsistencies
        return StepLineage(step_name, start_time, input_certs)

    def _validate_paths_compatible(self, resource_name, p1, p2):
        if p1==p2:
            return
        elif p1==None:
            raise LineageError("Output paths for step " + self.step_name +
                               " incompatible for resource "+
                               resource_name +
                               ": Cannot have both root path and a subpath ("+
                               p2 + ")")
        elif p2==None:
            raise LineageError("Output paths for step "+ self.step_name +
                               " incompatible for resource "+
                               resource_name +
                               ": Cannot have both root path and a subpath ("+
                               p1 + ")")
        elif p1.startswith(p2):
            raise LineageError("Output paths for step " + self.step_name +
                               " incompatible for resource " + resource_name +
                               ": Cannot have a path (" + p1 +
                               ")that is a subpath of another (" + p2 + ")")
        elif p2.startswith(p1):
            raise LineageError("Output paths for step " + self.step_name +
                               " incompatible for resource " + resource_name +
                               ": Cannot have a path (" + p2 +
                               ")that is a subpath of another (" + p1 + ")")

    def get_resource_cert_for_resource(self, resource_name:str,
                                       subpath:Optional[str]=None) -> \
                                       Optional[ResourceCert]:
        """Return the resource cert if this resource/subpath are outputs
        of the step. If not, return None.
        """
        if self.output_resources is None:
            raise InternalError("Output resources for %s not initialized" % self.step_name)
        for rc in self.output_resources:
            if rc.resource_name==resource_name and rc.subpath==subpath:
                return rc
        return None

    def validate_subpath_compatible(self, resource_name:str,
                                    subpath:Optional[str]=None) -> None:
        """Validate that specified subpath would be compatible with
        the existing subpaths for this resource. Throws a LineageError
        if there is a problem.
        """
        assert resource_name in self.outputs_by_resource
        for p in self.outputs_by_resource[resource_name]:
            self._validate_paths_compatible(resource_name, p, subpath)

    def get_subpaths_for_output(self, resource_name:str) -> Set[Optional[str]]:
        return self.outputs_by_resource[resource_name]


    def add_output(self, lineage_store:'LineageStoreCurrent', ref:ResourceRef):
        # first, validate that this path is compatibile with what we already have
        if ref.name in self.outputs_by_resource:
            s = self.outputs_by_resource[ref.name]
            if ref.subpath in s:
                raise LineageError("Resource %s referenced multiple times in outputs of step %s"%
                                   (ref, self.step_name))
        else:
            s = set()
            self.outputs_by_resource[ref.name] = s
        for p in s:
            self._validate_paths_compatible(ref.name, p, ref.subpath)
        # now, create a placeholder certificate, which also validates that
        # it is compatible with the LineageStore.
        rc = lineage_store.get_placeholder_resource_cert_for_output(self.step_name,
                                                                    self.start_time,
                                                                    ref)
        s.add(ref.subpath)
        self.output_resources.add(rc)

    def to_json(self):
        """Return a dictionary containing a json-serializable representation
        of the step lineage.
        """
        return {
            'type':'step',
            'step_name':self.step_name,
            'start_time':self.start_time,
            'execution_time_seconds':self.execution_time_seconds,
            'parameters':self.parameters,
            'input_resources':[r.to_json() for r in self.input_resources],
            'output_resources':[r.to_json() for r in self.output_resources]
        }

    def replaced_by_new(self, resource_name, new_lineage):
        """Given a new update to the specified resource, return True
        if the update replaces this lineage entry, False if there
        is no change to this entry, and raise an error if the update
        is incompatible.
        """
        nl_subpaths = new_lineage.get_subpaths_for_output(resource_name)
        ol_subpaths = self.get_subpaths_for_output(resource_name)
        if nl_subpaths==ol_subpaths:
            return True
        elif len(nl_subpaths.intersection(ol_subpaths))==0:
            return False
        else:
            def map_subpath(plist):
                return [(p if p is not None else '/') for p in plist]
            raise LineageError(("Error updating lineage for resource %s: "+
                                "subpaths do not match previous lineage. Current "+
                                "paths: %s, new paths: %s")%
                               (resource_name,
                                ', '.join(sorted(map_subpath(ol_subpaths))),
                                ', '.join(sorted(map_subpath(nl_subpaths)))))

    @staticmethod
    def from_json(obj, filename=None):
        validate_json_keys(obj, StepLineage,
                           ['step_name', 'start_tine', 'parameters',
                            'input_resources'],
                           filename=filename)
        return StepLineage(obj['step_name'], obj['start_time'], obj['parameters'],
                           obj['input_resources'], obj.get('output_resources', None),
                           obj.get('execution_time_seconds', None))


class SourceDataLineage(ResourceLineage):
    """Used for a source data resource that is not created
    by any workflow step.
    """
    __slots__ = ['resource_cert']
    def __init__(self, resource_cert:ResourceCert):
        self.resource_cert = resource_cert

    def to_json(self):
        obj = self.resource_cert.to_json()
        obj['type'] = 'source_data'
        return obj
    @staticmethod
    def from_json(obj, filename=None):
        assert obj['type']=='source_data'
        return ResourceCert.from_json(obj, filename=filename)

    def get_subpaths_for_output(self, resource_name:str) -> Set[Optional[str]]:
        return frozenset([self.resource_cert.subpath,])

    def get_resource_cert_for_resource(self, resource_name:str,
                                       subpath:Optional[str]=None) -> \
                                       Optional[ResourceCert]:
        """Return the resource cert if this resource/subpath are contained
        in this lineage object. If not, return None
        """
        return self.resource_cert \
            if (self.resource_cert.resource_name==resource_name and
             self.resource_cert.subpath==subpath) \
            else None

    def validate_subpath_compatible(self, resource_name:str,
                                    subpath:Optional[str]=None) -> None:
        """Validate that specified subpath would be compatible with
        the existing subpath(s) for this resource. Throws a LineageError
        if there is a problem.
        """
        assert self.resource_cert.resource_name==resource_name
        my_subpath = self.resource_cert.subpath
        assert my_subpath!=subpath, "Should be comparing a mew subpath"



class ResourceLineages:
    """All the step and source data lineages that built the current state of the
    resource. If there was no writing to subresources, this will just have one
    for the entire resource.
    """
    def __init__(self, resource_name:str, lineages:List[ResourceLineage]):
        self.resource_name =resource_name
        self.lineages = lineages

    def to_json(self):
        return {'resource_name':self.resource_name,
                'lineages':self.lineages}

    @staticmethod
    def from_json(obj, filename=None):
        validate_json_keys(obj, ResourceLineage, ['resource_name', 'lineages'])
        return ResourceLineages(obj['resource_name'],
                               [ResourceLineage.from_json(s, filename=filename)
                                for s in obj['lineages']])

    def add(self, lineage:ResourceLineage):
        """Add a lineage, potentially overwriting previous entries.
        If the subpath is incompatible with any of the existing lineages,
        raise an error
        """
        assert lineage.resource_name==self.resource_name
        new_lineages = [lineage,]
        for old_lineage in self.lineages:
            if not old_lineage.replaced_by_new(self.resource_name, lineage):
                new_lineages.append(old_lineage)
        self.lineages = new_lineages

    def get_or_create_lineage(self, step_name:str, step_time:datetime.datetime,
                              subpath:Optional[str]=None) -> Tuple[ResourceLineage, ResourceCert]:
        """Get the lineage matching the subpath and return the lineage and cert.
        If the lineage does not exist, and is compatible with the existing cases,
        create a placeholder. If there would be an incompatibility with the
        existing lineages, raise an error.
        """
        for existing in self.lineages:
            rc = existing.get_resource_cert_for_resource(self.resource_name,
                                                         subpath)
            if rc is not None:
                return (existing, rc)
            else:
                existing.validate_subpath_compatible(self.resource_name, subpath)
        cert = PlaceholderCertificate(1, "Read by step %s at %s" % (step_name, step_time))
        return (SourceDataLineage(cert), cert)

    def get_lineage(self, resource_cert:ResourceCert) -> ResourceLineage:
        for lineage in self.lineages:
            cert = lineage.get_resource_cert_for_resource(self.resource_name, resource_cert.subpath)
            if cert is not None:
                return lineage
        raise KeyError(cert)

    def get_placeholder_resource_cert_for_output(self, step_name:str, step_time:datetime.datatime,
                                                 subpath:Optional[str]=None) -> ResourceCert:
        """Get a placeholder resource cert to be used as a step output.
        Validates that the path is compatible with existing resources. If this is the
        first placeholder for this resource, return version 1. Otherwise, increment the version.
        """
        old_rc = None
        for existing in self.lineages:
            rc = existing.get_resource_cert_for_resource(self.resource_name,
                                                         subpath)
            if rc is not None:
                assert old_rc is None
                old_rc = rc
            else:
                existing.validate_subpath_compatible(self.resource_name, subpath)
        if old_rc and isinstance(old_rc.certificate, PlaceholderCertificate):
                version = old_rc.certificate.version + 1
        else:
            version=1
        return ResourceCert(ResourceRef(self.resource_name, subpath),
                            PlaceholderCertificate(version,
                                                   "Written by step %s at %s" %(step_name,
                                                                                step_time)))


class LineageStoreCurrent:
    """Lineage store for the current state of the resources.
    Invariants:

    * All lineage entries for a given resource are non-overlapping (no incompatible paths)
    * Only one lineage for a given resource references is stored. This lineage should
      correspond to the current state of the resource. Step lineages may reference
      older entries.
    * After a snapshot, all placeholder certificates are replaced with real hash-based
      certificates.
    """
    def __init__(self, local_path):
        if  not exists(local_path):
            raise InternalError("Missing directory %s for current lineage store" % local_path)
        self.local_path = local_path
        self.lineage_by_resource = {} # Map[str, ResourceLineages]
        self.lineage_by_resource_ref = {} # Map[ResourceRef, ResourceLineage]


    def add_step(self, lineage:StepLineage):
        """Given a completed step, update the lineage store
        """
        for rc in lineage.output_resources:
            name = rc.resource_name
            if name in self.lineage_by_resource:
                self.lineage_by_resource[name].add(lineage)
            else:
                self.lineage_by_resource[name] = \
                    ResourceLineages(name, [lineage,])
            self.lineage_by_resource_ref[rc.ref()] = lineage


    def get_or_create_lineage(self, step_name:str, step_time:datetime.datetime, ref:ResourceRef) \
        -> ResourceCert:
        if ref.name in self.lineage_by_resource:
            lineages = self.lineage_by_resource[ref.name]
        else:
            lineages = ResourceLineages(ref.name, [])
            self.lineage_by_resource[ref.name] = lineages
        (lineage, cert) = lineages.get_or_create_lineage(step_name, step_time, ref.subpath)
        if ref not in self.lineage_by_resource_ref:
            self.lineage_by_resource_ref[ref] = lineage
        else:
            assert self.lineage_by_resource_ref[ref] == lineage
        return cert

    def get_lineage_for_cert(self, rc:ResourceCert) -> ResourceLineage:
        assert rc.ref.name in self.lineage_by_resource
        self.lineage_by_resource[rc.ref.name].get_lineage(rc)

    def get_placeholder_resource_cert_for_output(self, step_name:str, step_time:datetime.datetime,
                                                 ref:ResourceRef) -> ResourceCert:
        """Get a placeholder resource cert to be used as a step output.
        Validates that the path is compatible with existing resources. If this is the
        first placeholder for this resource, return version 1. Otherwise, increment the version.
        This doesn't change the state of the store, we'll do that when we
        add the finalized step lineage.
        """
        if ref.name in self.lineage_by_resource:
            return self.lineage_by_resource[ref.name]\
                       .get_placeholder_resource_cert_for_output(step_name, step_time,
                                                                 ref.subpath)
        else:
            return ResourceCert(ref.name,
                                PlaceholderCertificate(1,
                                                       "Written by step %s at %s"%
                                                       (step_name, step_time)))


