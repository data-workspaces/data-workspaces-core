
"""
Utilities for data lineage files

For now, the resource overwriting rule is that, we either completely
match a subpath and replace it, or there is no intersection.
"""

import datetime
import os
from os.path import join, exists, isdir, basename
from typing import List, Any, Optional, Tuple, NamedTuple, Set, Dict, cast
from copy import copy
import json
import shutil
import sys

import click


from dataworkspaces.errors import InternalError, LineageError
from .regexp_utils import isots_to_dt

class LineageConsistencyError(LineageError):
    """Special case of LineageError where the inputs for a step
    have inconsistent versions.
    """
    pass

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
    """A namedtuple that is used to identify an input or output of a step.
    The ``name`` parameter is the name of a resource. The optional
    ``subpath`` parameter is a relative path within that resource.
    The subpath lets you store inputs/outputs from multiple steps
    within the same resource and track them independently.
    """
    name: str
    subpath: Optional[str] = None


class Certificate:
    @staticmethod
    def from_json(obj:Any, filename:Optional[str]=None):
        validate_json_keys(obj, Certificate, ['cert_type',], filename=filename)
        cert_type = obj['cert_type']
        if cert_type == 'hash':
            validate_json_keys(obj, HashCertificate, ['hashval', 'comment'], filename=filename)
            return HashCertificate(obj['hashval'], obj['comment'])
        elif cert_type=='placeholder':
            validate_json_keys(obj, PlaceholderCertificate, ['version', 'comment'], filename=filename)
            return PlaceholderCertificate(obj['version'], obj['comment'])
        else:
            raise JsonValueError(Certificate, 'cert_type', ['hash', 'placeholder'], cert_type)

    def to_json(self) -> Dict[str, Any]:
        raise NotImplementedError(self.__class__.__name__)


class HashCertificate(Certificate):
    __slots__ = ('hashval', 'comment')
    def __init__(self, hashval:str, comment:str):
        self.hashval = hashval
        self.comment = comment

    def __hash__(self):
        return hash(self.hashval)

    def __repr__(self):
        return 'HashCertificate(hashval=%s, comment="%s")' % \
            (self.hashval, self.comment)

    def __str__(self):
        return self.__repr__()

    def __eq__(self, other) -> bool:
        equal = isinstance(other, HashCertificate) and other.hashval==self.hashval
        if equal and other.comment!=self.comment:
            raise TypeError("Two hash certificates are equal in hash values but not in comments: '%s' and '%s'"%
                            (self.comment, other.comment))
        else:
            return equal

    def __ne__(self, other) -> bool:
        return (not isinstance(other, HashCertificate)) or other.hashval!=self.hashval

    def to_json(self) -> Dict[str, Any]:
        return {'cert_type':'hash', 'hashval':self.hashval, 'comment':self.comment}


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

    def __eq__(self, other) -> bool:
        equal = isinstance(other, PlaceholderCertificate) and other.version==self.version
        if equal and other.comment!=self.comment:
            raise TypeError("Two placeholder certificates equal in versions, but not in comments: '%s' and '%s'" %
                            (self.comment, other.comment))
        else:
            return equal

    def __ne__(self, other) -> bool:
        return (not isinstance(other, PlaceholderCertificate)) or other.version!=self.version

    def to_json(self) -> Dict[str, Any]:
        return {'cert_type':'placeholder', 'version':self.version,
                'comment':self.comment}


class ResourceCert:
    """Combination of a resource name and a certificate
    """
    __slots__ = ('ref', 'certificate')
    def __init__(self, ref:ResourceRef, certificate:Certificate):
        assert isinstance(ref, ResourceRef) # XXX
        self.ref = ref
        self.certificate = certificate

    def __hash__(self):
        return hash((self.ref, self.certificate),)

    def __eq__(self, other) -> bool:
        return isinstance(other, ResourceCert) and other.ref==self.ref and other.certificate==self.certificate

    def __ne__(self, other) -> bool:
        return (not isinstance(other, ResourceCert)) or other.ref!=self.ref or other.certificate!=self.certificate

    def to_json(self)  -> dict:
        return {'resource_name':self.ref.name, 'subpath':self.ref.subpath,
                'certificate':self.certificate.to_json()}

    def __repr__(self):
        return 'ResourceCert(ref=%s, certificate=%s)' % (repr(self.ref), repr(self.certificate))

    def __str__(self):
        return 'ResourceCert(ref=%s, certificate=%s)' % (str(self.ref), str(self.certificate))

    def is_placeholder(self):
        return isinstance(self.certificate, PlaceholderCertificate)

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

    def verify_compatble(self, resource_name:str, new_lineage:'ResourceLineage'):
        """Given a new lineage which will update this one
        (either data source or step), verify that the update is compatible.
        """
        nl_subpaths = new_lineage.get_subpaths_for_resource(resource_name)
        ol_subpaths = self.get_subpaths_for_resource(resource_name)
        def map_subpath(plist):
            return [(p if p is not None else '/') for p in plist]
        if nl_subpaths != ol_subpaths:
            raise LineageError(("Error updating lineage for resource %s: "+
                                "subpaths do not match previous lineage. Current "+
                                "paths: %s, new paths: %s")%
                               (resource_name,
                                ', '.join(sorted(map_subpath(ol_subpaths))),
                                ', '.join(sorted(map_subpath(nl_subpaths)))))

    def get_resource_cert_for_resource(self, ref:ResourceRef) -> \
                                       Optional[ResourceCert]:
        """Return the resource cert if this resource/subpath are contained
        in this lineage object. If not, return None
        """
        raise NotImplementedError(self.__class__.__name__)

    def validate_subpath_compatible(self, ref:ResourceRef) -> None:
        """Validate that specified subpath would be compatible with
        the existing subpath(s) for this resource. Throws a LineageError
        if there is a problem.
        """
        raise NotImplementedError(self.__class__.__name__)

    def get_subpaths_for_resource(self, resource_name:str) -> Set[Optional[str]]:
        """Return a set of all subpaths for this resource. For the step
        lineage, this will look at the outputs. For the source data lineage,
        this will just return a set containing one path.
        """
        raise NotImplementedError(self.__class__.__name__)

    def replace_certificate(self, old_rc:ResourceCert, new_rc:ResourceCert) -> None:
        """Replace the old certificate with the new one. This is used to
        replace placeholders with real hash certificates.
        """
        raise NotImplementedError(self.__class__.__name__)

    def replaced_by_new(self, resource_name:str, new_lineage:'ResourceLineage'):
        """Given a new update to the specified resource, return True
        if the update replaces this lineage entry, False if there
        is no change to this entry, and raise an error if the update
        is incompatible.

        Our current rule is that either an update completely replaces
        an old lineage or there are no intersections.
        """
        nl_subpaths = new_lineage.get_subpaths_for_resource(resource_name)
        ol_subpaths = self.get_subpaths_for_resource(resource_name)
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

    def get_resource_certificates(self) -> List[ResourceCert]:
        """Get all resource certificates associated with this lineage.
        Returns the output rc's for a step lineage and the certificate for
        a source data lineage.
        """
        raise NotImplementedError(self.__class__.__name__)

class StepLineage(ResourceLineage):
    __slots__ = ['step_name', 'start_time', 'parameters', 'input_resources',
                 'output_resources', 'execution_time_seconds',
                 'command_line']
    def __init__(self, step_name:str, start_time:datetime.datetime,
                 parameters:Dict[str, Any],
                 input_resources:List[ResourceCert],
                 output_resources:Optional[List[ResourceCert]]=None,
                 execution_time_seconds:Optional[float]=None,
                 command_line:Optional[List[str]]=None):
        self.step_name = step_name
        self.start_time = start_time
        self.parameters = parameters
        self.input_resources = input_resources # type: List[ResourceCert]
        self.execution_time_seconds = execution_time_seconds
        self.command_line = command_line
        # map from output resource name to sets of subpaths
        self.outputs_by_resource = {} # type: Dict[str, Set[Optional[str]]]
        if output_resources is not None:
            self.output_resources = output_resources
            # build the map of outputs by resource
            for rc in output_resources:
                if rc.ref.name in self.outputs_by_resource:
                    s = self.outputs_by_resource[rc.ref.name]
                else:
                    s = set()
                    self.outputs_by_resource[rc.ref.name] = s
                for p in s:
                    self._validate_paths_compatible(rc.ref.name,
                                                    p, rc.ref.subpath)
                s.add(rc.ref.subpath)
        else:
            self.output_resources  = [] # List[ResourceCert]

    @staticmethod
    def make_step_lineage(step_name:str, start_time:datetime.datetime,
                          parameters:Dict[str, Any],
                          input_resource_refs:List[ResourceRef],
                          lineage_store:'LineageStoreCurrent',
                          command_line:Optional[List[str]]=None) -> 'StepLineage':
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
                other_cert = lineage.get_resource_cert_for_resource(rc.ref)
                assert other_cert is not None
                if other_cert!=rc:
                    raise LineageConsistencyError("Step %s depends on two versions of %s: %s and %s"
                                                  %(step_name, rc.ref, other_cert, rc))
                if isinstance(lineage, StepLineage):
                    for input_rc in lineage.input_resources:
                        if input_rc not in transitive_certs:
                            transitive_certs.add(input_rc)
                            next_to_process.append(input_rc)
            to_process = next_to_process
        # if we got here, we didn't find any inconsistencies
        return StepLineage(step_name, start_time, parameters, input_certs,
                           command_line=command_line)

    def _validate_paths_compatible(self, resource_name:str,
                                   p1:Optional[str], p2:Optional[str]) -> None:
        if p1==p2:
            return
        elif p1==None:
            assert p2 is not None # for typechecker
            raise LineageError("Output paths for step " + self.step_name +
                               " incompatible for resource "+
                               resource_name +
                               ": Cannot have both root path and a subpath ("+
                               p2 + ")")
        elif p2==None:
            assert p1 is not None # for typechecker
            raise LineageError("Output paths for step "+ self.step_name +
                               " incompatible for resource "+
                               resource_name +
                               ": Cannot have both root path and a subpath ("+
                               p1 + ")")
        p1 = cast(str, p1)
        p2 = cast(str, p2)
        if p1.startswith(p2):
            raise LineageError("Output paths for step " + self.step_name +
                               " incompatible for resource " + resource_name +
                               ": Cannot have a path (" + p1 +
                               ")that is a subpath of another (" + p2 + ")")
        elif p2.startswith(p1):
            raise LineageError("Output paths for step " + self.step_name +
                               " incompatible for resource " + resource_name +
                               ": Cannot have a path (" + p2 +
                               ")that is a subpath of another (" + p1 + ")")

    def get_resource_cert_for_resource(self, ref:ResourceRef) -> \
                                       Optional[ResourceCert]:
        """Return the resource cert if this resource/subpath are outputs
        of the step. If not, return None.
        """
        for rc in self.output_resources:
            if rc.ref==ref:
                return rc
        return None

    def validate_subpath_compatible(self, ref:ResourceRef) -> None:
        """Validate that specified subpath would be compatible with
        the existing subpaths for this resource. Throws a LineageError
        if there is a problem.
        """
        assert ref.name in self.outputs_by_resource
        for p in self.outputs_by_resource[ref.name]:
            self._validate_paths_compatible(ref.name, p, ref.subpath)

    def get_subpaths_for_resource(self, resource_name:str) -> Set[Optional[str]]:
        return self.outputs_by_resource[resource_name]


    def get_resource_certificates(self) -> List[ResourceCert]:
        """Get all resource certificates associated with this lineage."""
        return self.output_resources

    def add_output(self, lineage_store:'LineageStoreCurrent', ref:ResourceRef):
        assert isinstance(ref, ResourceRef) # XXX Until we have typechecking working
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
        self.output_resources.append(rc)

    def replace_certificate(self, old_rc:ResourceCert, new_rc:ResourceCert) -> None:
        """Replace the old certificate with the new one. This is used to
        replace placeholders with real hash certificates.
        """
        for (i, rc) in enumerate(self.output_resources):
            if rc==old_rc:
                self.output_resources[i] = new_rc
                return
        assert 0, "Did not find rc %s in step %s" % (old_rc, self.step_name)

    def to_json(self):
        """Return a dictionary containing a json-serializable representation
        of the step lineage.
        """
        return {
            'type':'step',
            'step_name':self.step_name,
            'start_time':self.start_time.isoformat(),
            'execution_time_seconds':self.execution_time_seconds,
            'parameters':self.parameters,
            'input_resources':[r.to_json() for r in self.input_resources],
            'output_resources':[r.to_json() for r in self.output_resources],
            'command_line':self.command_line
        }

    @staticmethod
    def from_json(obj, filename=None):
        validate_json_keys(obj, StepLineage,
                           ['step_name', 'start_time', 'parameters',
                            'input_resources'],
                           filename=filename)
        return StepLineage(obj['step_name'], isots_to_dt(obj['start_time']),
                           obj['parameters'],
                           [ResourceCert.from_json(rcobj, filename) for rcobj
                            in obj['input_resources']],
                           [ResourceCert.from_json(rcobj, filename) for rcobj
                            in obj['output_resources']],
                           obj.get('execution_time_seconds', None),
                           obj.get('command_line', None))


class SourceDataLineage(ResourceLineage):
    """Used for a source data resource that is not created
    by any workflow step.
    """
    __slots__ = ['resource_cert']
    def __init__(self, resource_cert:ResourceCert):
        self.resource_cert = resource_cert # ResourceCert

    def to_json(self):
        obj = self.resource_cert.to_json()
        obj['type'] = 'source_data'
        return obj

    @staticmethod
    def from_json(obj, filename=None):
        assert obj['type']=='source_data'
        return SourceDataLineage(ResourceCert.from_json(obj, filename=filename))

    def get_subpaths_for_resource(self, resource_name:str) -> Set[Optional[str]]:
        return set([self.resource_cert.ref.subpath,])

    def get_resource_cert_for_resource(self, ref:ResourceRef) -> \
                                       Optional[ResourceCert]:
        """Return the resource cert if this resource/subpath are contained
        in this lineage object. If not, return None
        """
        return self.resource_cert if self.resource_cert.ref==ref else None

    def get_resource_certificates(self) -> List[ResourceCert]:
        """Get all resource certificates associated with this lineage."""
        return [self.resource_cert,]

    def validate_subpath_compatible(self, ref:ResourceRef) -> None:
        """Validate that specified subpath would be compatible with
        the existing subpath(s) for this resource. Throws a LineageError
        if there is a problem.
        """
        assert self.resource_cert.ref.name==ref.name
        assert self.resource_cert.ref.subpath!=ref.subpath, "Should be comparing a mew subpath"

    def replace_certificate(self, old_rc:ResourceCert, new_rc:ResourceCert) -> None:
        """Replace the old certificate with the new one. This is used to
        replace placeholders with real hash certificates.
        """
        assert self.resource_cert == old_rc
        self.resource_cert = new_rc



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
                'lineages':[lineage.to_json() for lineage in self.lineages]}

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
        new_lineages = [lineage,]
        for old_lineage in self.lineages:
            if not old_lineage.replaced_by_new(self.resource_name, lineage):
                new_lineages.append(old_lineage)
        self.lineages = new_lineages

    def get_or_create_lineage(self, step_name:str, step_time:datetime.datetime,
                              ref:ResourceRef) -> ResourceCert:
        """Get the lineage matching the subpath and return the resource ceert.
        If the lineage does not exist, and is compatible with the existing cases,
        create a placeholder. If there would be an incompatibility with the
        existing lineages, raise an error.
        """
        assert ref.name==self.resource_name
        for existing in self.lineages:
            rc = existing.get_resource_cert_for_resource(ref)
            if rc is not None:
                return rc
            else:
                existing.validate_subpath_compatible(ref)
        cert = ResourceCert(ref,
                            PlaceholderCertificate(1,
                                                   "Read by step %s at %s" %
                                                   (step_name, step_time)))
        self.lineages.append(SourceDataLineage(cert))
        return cert

    def get_lineage(self, resource_cert:ResourceCert) -> ResourceLineage:
        for lineage in self.lineages:
            cert = lineage.get_resource_cert_for_resource(resource_cert.ref)
            if cert is not None:
                return lineage
        raise KeyError(resource_cert)

    def get_cert_and_lineage_for_ref(self, ref:ResourceRef) -> \
        Tuple[Certificate, ResourceLineage]:
        for lineage in self.lineages:
            rc = lineage.get_resource_cert_for_resource(ref)
            if rc is not None:
                return (rc.certificate, lineage)
        raise KeyError(ref)

    def get_placeholder_resource_cert_for_output(self, step_name:str,
                                                 step_time:datetime.datetime,
                                                 ref:ResourceRef) -> ResourceCert:
        """Get a placeholder resource cert to be used as a step output.
        Validates that the path is compatible with existing resources. If this is the
        first placeholder for this resource, return version 1. Otherwise, increment the version.
        """
        assert ref.name==self.resource_name
        old_rc = None
        for existing in self.lineages:
            rc = existing.get_resource_cert_for_resource(ref)
            if rc is not None:
                assert old_rc is None
                old_rc = rc
            else:
                existing.validate_subpath_compatible(ref)
        if old_rc and isinstance(old_rc.certificate, PlaceholderCertificate):
                version = old_rc.certificate.version + 1
        else:
            version=1
        assert isinstance(ref, ResourceRef) # XXX
        return ResourceCert(ref,
                            PlaceholderCertificate(version,
                                                   "Written by step %s at %s" %(step_name,
                                                                                step_time)))
    def replace_placeholders_with_real_certs(self, hashval:str,
                                             placeholder_to_real:Dict[ResourceCert,ResourceCert]) -> int:
        """Find any placeholder certificates for this resource and replace
        with a certificate using the specified hashval. We add any substitutions made to
        placeholder_to_real. This will be used in a second pass to replace all the step inputs.
        """
        warnings = 0
        for lineage in self.lineages:
            for subpath in lineage.get_subpaths_for_resource(self.resource_name):
                ref = ResourceRef(self.resource_name, subpath)
                rc = lineage.get_resource_cert_for_resource(ref)
                assert rc is not None
                if rc.is_placeholder():
                    new_rc = ResourceCert(ref,
                                          HashCertificate(hashval,
                                                          cast(PlaceholderCertificate,
                                                               rc.certificate).comment))
                    lineage.replace_certificate(rc, new_rc)
                    placeholder_to_real[rc] = new_rc
                elif cast(HashCertificate, rc.certificate).hashval!=hashval:
                    old_hashval = cast(HashCertificate, rc.certificate).hashval
                    click.echo(("WARNING: lineage for resource %s, has hash value '%s', but snapshot has hash value '%s'."+
                                " Using older version (%s)") %
                               (self.resource_name, old_hashval, hashval,
                                old_hashval),
                               err=True)
                    warnings +=1
        return warnings

    def verify_no_placeholders_for_resource(self) -> int:
        """There was no hash generated for this resource,
        so we want to verify that it does not have any placeholders
        in the store. We print a warning if there is an unreplaced
        placeholder.
        """
        warnings = 0
        for lineage in self.lineages:
            for subpath in lineage.get_subpaths_for_resource(self.resource_name):
                ref = ResourceRef(self.resource_name, subpath)
                rc = lineage.get_resource_cert_for_resource(ref)
                assert rc is not None
                if rc.is_placeholder():
                    click.echo("WARNING: have a placeholder for %s, but resource %s was not included in snapshot" %
                                       (rc, ref.name),
                               err=True)
                    warnings += 1
        return warnings

    def replace_step_input_placeholders(self,
                                        placeholder_to_real:Dict[ResourceCert, ResourceCert]) -> int:
        """Use the mappings from placeholder rc's to hash rc's to replace the
        placeholder references in step inputs.
        """
        warnings = 0
        for lineage in self.lineages:
            if isinstance(lineage, StepLineage):
                lineage.input_resources = [
                    placeholder_to_real[rc] if rc in placeholder_to_real else rc
                    for rc in lineage.input_resources
                ]
                unsubstituted = [rc for rc in lineage.input_resources if
                                 rc.is_placeholder()]
                if len(unsubstituted)>0:
                    click.echo("WARNING: Step %s still has placeholder certificates: %s. Do you need to include more resources in your snapshot?"%
                               (lineage.step_name,
                                ', '.join([str(rc) for rc in unsubstituted])),
                               err=True)
                    warnings += len(unsubstituted)
        return warnings


    def drop_lineage_for_ref(self, ref:ResourceRef):
        """Drop the lineage for ref from our lineages, if it is present
        """
        self.lineages = [
            lineage for lineage in self.lineages
            if lineage.get_resource_cert_for_resource(ref) is None
        ]

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
    def __init__(self,
                 lineage_by_resource:Optional[Dict[str, ResourceLineages]]=None):
        if lineage_by_resource:
            self.lineage_by_resource = lineage_by_resource
        else:
            self.lineage_by_resource = {} # type: Dict[str, ResourceLineages]


    def add_step(self, lineage:StepLineage):
        """Given a completed step, update the lineage store
        """
        for rc in lineage.output_resources:
            assert isinstance(rc, ResourceCert), "%s is not a ResourceCert" % rc #XXX
            name = rc.ref.name
            if name in self.lineage_by_resource:
                self.lineage_by_resource[name].add(lineage)
            else:
                self.lineage_by_resource[name] = \
                    ResourceLineages(name, [lineage,])


    def get_or_create_lineage(self, step_name:str, step_time:datetime.datetime, ref:ResourceRef) \
        -> ResourceCert:
        if ref.name in self.lineage_by_resource:
            lineages = self.lineage_by_resource[ref.name]
        else:
            lineages = ResourceLineages(ref.name, [])
            self.lineage_by_resource[ref.name] = lineages
        return lineages.get_or_create_lineage(step_name, step_time, ref)

    def get_lineage_for_cert(self, rc:ResourceCert) -> ResourceLineage:
        assert rc.ref.name in self.lineage_by_resource
        return self.lineage_by_resource[rc.ref.name].get_lineage(rc)

    def get_placeholder_resource_cert_for_output(self, step_name:str,
                                                 step_time:datetime.datetime,
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
                                                                 ref)
        else:
            return ResourceCert(ref,
                                PlaceholderCertificate(1,
                                                       "Written by step %s at %s"%
                                                       (step_name, step_time)))

    def replace_placeholders_with_real_certs(self, resource_to_hash:Dict[str,str]) -> int:
        """Given a mapping of resource names to hashvals taken from a workspace
        snapshot, replace any placeholder certificates in our store with
        hash certificates. Note that the hashes are taken at the resource level,
        so all subpaths for a given resource have the same certificate. This works,
        as we disallow overlapping workflows.

        Returns the number of warnings (due to an existing hash not matching the snapshot
        hash)
        """
        placeholder_to_real = {} # type: Dict[ResourceCert,ResourceCert]
        warnings = 0
        for (name, lineages) in self.lineage_by_resource.items():
            if name in resource_to_hash:
                warnings += lineages.replace_placeholders_with_real_certs(resource_to_hash[name],
                                                                          placeholder_to_real)
            else:
                warnings += lineages.verify_no_placeholders_for_resource()
        for lineages in self.lineage_by_resource.values():
            warnings += lineages.replace_step_input_placeholders(placeholder_to_real)
        return warnings

    def get_cert_and_lineage_for_ref(self, ref:ResourceRef) -> \
        Tuple[Certificate, ResourceLineage]:
        """Given a ref, get the Certificate and Lineage that created it.
        This is currently used for testing.
        """
        if ref.name in self.lineage_by_resource:
            return self.lineage_by_resource[ref.name].get_cert_and_lineage_for_ref(ref)
        else:
            raise KeyError("No resource store entries for resource %s" %
                           ref.name)

    def invalidate_step_outputs(self, output_resources:List[ResourceCert]):
        """The currently running step has failed, so we donot know the state
        of the output resources. Go through the list and remove the lineage
        data corresponding to these refs, if they exist.
        """
        for rc in output_resources:
            if rc.ref.name in self.lineage_by_resource:
                self.lineage_by_resource[rc.ref.name].drop_lineage_for_ref(rc.ref)

    def validate(self, result_resources:List[ResourceRef]) -> int:
        """Validate that each step input certificate matches the current state of the associated resource.
        We do this transitively from the resource resources.
        Prints warnings to standard error and returns the number of warnings.
        """
        warnings = 0
        checked_set = set() # Set[ResourceRef]
        to_check = result_resources
        while len(to_check)>0:
            new_to_check = []
            for ref in to_check:
                (sink_cert, sink_lineage) = self.get_cert_and_lineage_for_ref(ref)
                checked_set.add(ref)
                if isinstance(sink_lineage, SourceDataLineage):
                    continue
                sink_lineage = cast(StepLineage, sink_lineage)
                for rc in sink_lineage.input_resources:
                    (source_cert, source_lineage) = self.get_cert_and_lineage_for_ref(rc.ref)
                    if source_cert!=rc.certificate:
                        click.echo("WARNING: step %s has input %s with lineage %s. However, the current state of %s is %s"%
                                   (sink_lineage.step_name, rc.ref, rc, rc.ref, source_cert), err=True)
                        warnings += 1
                    elif rc.ref not in checked_set:
                        #print("Verified input %s of %s matches source" % (rc, sink_lineage.step_name))
                        new_to_check.append(rc.ref)
            to_check = new_to_check
        return warnings

    def get_lineage_for_resource(self, resource_name:str) -> \
        Tuple[List[ResourceLineage], bool]:
        """Return a list of all transitive lineage for the specified
        resource and a boolean indicating whether the lineage is complete
        """
        if resource_name not in self.lineage_by_resource:
            return ([], False)
        complete = True
        checked_set = set() # type: Set[ResourceCert]
        lineages = [] # type: List[ResourceLineage]
        to_check = [] # type: List[ResourceCert]
        for lineage in self.lineage_by_resource[resource_name].lineages:
            lineages.append(lineage)
            checked_set = checked_set.union(set(lineage.get_resource_certificates()))
            if isinstance(lineage, StepLineage):
                sl = cast(StepLineage, lineage)
                for rc in sl.input_resources:
                    if rc not in checked_set:
                        to_check.append(rc)
        while len(to_check)>0:
            new_to_check = [] # type: List[ResourceCert]
            for rc in to_check:
                if rc in checked_set:
                    continue
                try:
                    (source_cert, source_lineage) = \
                        self.get_cert_and_lineage_for_ref(rc.ref)
                except KeyError:
                    click.echo("WARNING: Lineage incomplete, could not find lineage for %s"
                               % str(rc.ref),
                               err=True)
                    complete = False
                    continue
                if source_cert!=rc.certificate:
                    click.echo("WARNING: Lineage incomplete, resource %s was overwritten"%
                               rc, err=True)
                    complete = False
                    continue
                lineages.append(source_lineage)
                checked_set = checked_set.union(set(source_lineage.get_resource_certificates()))
                if isinstance(source_lineage, StepLineage):
                    sl = cast(StepLineage, source_lineage)
                    for rc_in in sl.input_resources:
                        if rc_in not in checked_set:
                            new_to_check.append(rc_in)
            to_check = new_to_check
        return (lineages, complete)

    def to_json(self):
        return {
            resource:lineages.to_json() for (resource, lineages) in self.lineage_by_resource.items()
        }

    ##################################################################
    #    Methods for interacting with the store on the filesystem    #
    ##################################################################

    def save(self, local_path):
        for (resource, lineages) in self.lineage_by_resource.items():
            with open(join(local_path, resource + '.json'), 'w') as f:
                json.dump(lineages.to_json(), f, indent=2)

    @staticmethod
    def load(local_path):
        lineage_by_resource = {} # Dict[str, ResourceLineages]
        for fname in os.listdir(local_path):
            if not fname.endswith('.json'):
                continue
            resource_name = fname[:-5]
            fpath = join(local_path, fname)
            with open(fpath, 'r') as f:
                lineages = ResourceLineages.from_json(json.load(f),
                                                      filename=fpath)
                assert lineages.resource_name==resource_name
                lineage_by_resource[resource_name] = lineages
        return LineageStoreCurrent(lineage_by_resource)

    @staticmethod
    def get_resource_names_in_fsstore(current_lineage_dir:str) -> List[str]:
        """Return a list of resources found in the current lineage store
        in the specified directory.
        """
        return [f[:-5] for f in os.listdir(current_lineage_dir) if f.endswith('.json')]

    @staticmethod
    def copy_fsstore_to_snapshot(current_lineage_dir:str, snapshot_lineage_dir:str,
                                 resource_names:List[str]) -> \
                                 Tuple[List[str], int]:
        """Copy the current lineage store to the snapshot's lineage
        directory. We only copy the resource names specified.
        Returns a pair: (list of output files, number of warnings)
        """
        warnings = 0
        copied_files = []
        for name in resource_names:
            src_path = join(current_lineage_dir, name+'.json')
            if exists(src_path):
                dest_path = join(snapshot_lineage_dir, name+'.json')
                shutil.copy(src_path, dest_path)
                copied_files.append(dest_path)
            else:
                click.echo("WARNING: no lineage data available for resource %s (maybe it was not used in your workflow)" % name,
                           err=True)
                warnings += 1
        store_resources=LineageStoreCurrent.get_resource_names_in_fsstore(current_lineage_dir)
        missing = set(store_resources).difference(set(resource_names))
        if len(missing)>0:
            click.echo("WARNING: The following resources have lineage data, but are not included in snapshot: %s" %
                       ', '.join(missing))
            warnings += 1
        return (copied_files, warnings)

    @staticmethod
    def restore_store_from_snapshot(snapshot_lineage_dir, current_lineage_dir,
                                    resource_names:List[str]) -> int:
        """We are restoring a snapshot. Update the current lineage store
        with the lineage from the snapshot. We only do this for the specified
        resource names. If there is no lineage data for the snapshot
        (the snapshot lineage directory does not exist), we remove
        the current lineage data. Likewise, when restoring, if there is
        is no lineage data for a given resource, we remove it from the current store.
        """
        warnings = 0
        if isdir(snapshot_lineage_dir):
            for name in resource_names:
                src_path = join(snapshot_lineage_dir, name +'.json')
                dest_path = join(current_lineage_dir, name +'.json')
                if exists(src_path):
                    shutil.copy(src_path, dest_path)
                elif exists(dest_path):
                    os.remove(dest_path)
                    click.echo("WARNING: Resource %s was in current lineage store, but no lineage data in snapshot, removing from store"%
                               name, err=True)
                    warnings += 1
        else:
            # we don't have any lineage for this snapshot. See if we need to remove anything
            current_resources = set(LineageStoreCurrent.get_resource_names_in_fsstore(current_lineage_dir))
            to_remove = current_resources.intersection(set(resource_names))
            if len(to_remove)>0:
                click.echo("WARNING: no lineage data for snapshot, removing the following resources from lineage store: %s" %
                           ', '.join(to_remove))
                warnings += 1
                for name in to_remove:
                    os.remove(join(current_lineage_dir, name+'.json'))
        return warnings

    @staticmethod
    def invalidate_fsstore_entries(current_lineage_dir,
                                   resource_names:List[str]) -> None:
        """When doing a pull, the resources may be put in an unknown state. We
        need to invalidate the current lineage store entries on disk.
        """
        for name in resource_names:
            path = join(current_lineage_dir, name +'.json')
            if exists(path):
                os.remove(path)


# Utilties for interacting with the dataworkspace metadata
def get_current_lineage_dir(workspace_dir):
    return join(workspace_dir, '.dataworkspace/current_lineage')

def get_snapshot_lineage_dir(workspace_dir, snapshot_hash):
    return join(workspace_dir, '.dataworkspace/snapshot_lineage/%s' % snapshot_hash)


def infer_step_name(argv=sys.argv):
    """Given the command line args, infer the step name
    """
    if argv[0].endswith('.py'):
        return basename(argv[0])[:-3]
    elif 'python' in argv[0] and argv[1].endswith('.py'):
        return basename(argv[1])[:-3]
    else:
        return basename(argv[0])
