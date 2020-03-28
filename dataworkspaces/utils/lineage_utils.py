"""
Utilities for data lineage files

For now, the resource overwriting rule is that, we either completely
match a subpath and replace it, or there is no intersection.

For pretty printing of resources and certs, the convention is:

* Don't indent before the first line
* For subsequent lines, indent as specified
* For sub-objects, call .pp(indent+2)
* No newline at the end

The __repr__ call should just be pp(0)
"""

import datetime
import os
from os.path import (
    join,
    exists,
    basename,
    realpath,
    abspath,
    expanduser,
    commonpath,
    dirname,
    isdir,
)
from typing import List, Any, Optional, Tuple, NamedTuple, Dict, Iterable, cast
import json
import shutil
import sys
from abc import ABCMeta, abstractmethod
from string import Template


from dataworkspaces.errors import InternalError, LineageError
from .regexp_utils import isots_to_dt
from .hash_utils import is_a_git_hash


class LineageConsistencyError(LineageError):
    """Special case of LineageError where the inputs for a step
    have inconsistent versions.
    """

    pass


class LineageConflictError(LineageError):
    """Thrown when attempting to save lineage data at a different
    granularity than other entries for the same resource.
    """

    pass


class LineageNotFoundError(LineageError):
    """Thrown when a requested entry is not found in the lineage store.
    """

    pass


class LineagePlaceHolderError(LineageError):
    """Thrown in the event that a snapshot contains a placeholder certificate.
    """


class JsonKeyError(InternalError):
    def __init__(self, classobj, key, filename=None):
        if filename is not None:
            super().__init__(
                "Error parsing %s in file %s: missing key %s" % (classobj.__name__, filename, key)
            )
        else:
            super().__init__("Error parsing %s: missing key %s " % (classobj.__name__, key))


class JsonTypeError(InternalError):
    def __init__(self, classobj, exptype, actualtype, filename=None):
        if filename is not None:
            super().__init__(
                "Error parsing %s in file %s: expecting a %s, but got a %s"
                % (classobj.__name__, filename, exptype, actualtype)
            )
        else:
            super().__init__(
                "Error parsing %s: expecting a %s, but got a %s"
                % (classobj.__name__, exptype, actualtype)
            )


class JsonValueError(InternalError):
    def __init__(self, classobj, key, expected_vals, actualval, filename=None):
        if filename is not None:
            super().__init__(
                "Error parsing %s in file %s: key %s has value %s, valid values are: %s"
                % (classobj.__name__, filename, key, actualval, ", ".join(expected_vals))
            )
        else:
            super().__init__(
                "Error parsing %s: key %s has value %s, valid values are: %s"
                % (classobj.__name__, key, actualval, ", ".join(expected_vals))
            )


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

    def covers(self, other: "ResourceRef") -> bool:
        """Return True if this reference is strictly higher in
        the hierarchy than the other reference. This happens when
        both have the same resource name and:

        1. this reference does not have a subpath and the other reference does, *or*
        2. both references have subpaths and this one is a parent of the other
        """
        if self.name == other.name and (
            (self.subpath == None and other.subpath is not None)
            or (
                self.subpath != other.subpath
                and (self.subpath is not None)
                and (other.subpath is not None)
                and commonpath([self.subpath, other.subpath]) == self.subpath
            )
        ):
            return True
        else:
            return False


class Certificate(metaclass=ABCMeta):
    __slots__ = ("ref", "comment")

    def __init__(self, ref: ResourceRef, comment: str):
        self.ref = ref
        self.comment = comment

    @abstractmethod
    def pp(self, indent: int = 2) -> str:
        """Pretty print with the specified indent level
        """
        pass

    @staticmethod
    def from_json(obj: Any, filename: Optional[str] = None):
        validate_json_keys(obj, Certificate, ["resource_name", "certificate"], filename=filename)
        ref = ResourceRef(obj["resource_name"], subpath=obj.get("subpath", None))
        cert_obj = obj["certificate"]
        validate_json_keys(cert_obj, Certificate, ["cert_type",], filename=filename)
        cert_type = cert_obj["cert_type"]
        if cert_type == "hash":
            validate_json_keys(cert_obj, HashCertificate, ["hashval", "comment"], filename=filename)
            return HashCertificate(ref, cert_obj["hashval"], cert_obj["comment"])
        elif cert_type == "placeholder":
            validate_json_keys(
                cert_obj, PlaceholderCertificate, ["version", "comment"], filename=filename
            )
            if cert_obj.get("is_output", False) == True:
                return OutputPlaceholderCert(ref, cert_obj["version"], cert_obj["comment"])
            else:
                return InputPlaceholderCert(ref, cert_obj["version"], cert_obj["comment"])
        else:
            raise JsonValueError(Certificate, "cert_type", ["hash", "placeholder"], cert_type)

    @abstractmethod
    def to_json(self) -> Dict[str, Any]:
        pass


class HashCertificate(Certificate):
    __slots__ = ("hashval",)

    def __init__(self, ref: ResourceRef, hashval: str, comment: str):
        super().__init__(ref, comment)
        self.hashval = hashval

    def __hash__(self):
        return hash((self.ref, self.hashval),)

    def __str__(self):
        return 'HashCertificate(ref=%s, hashval=%s, comment="%s")' % (
            self.ref,
            self.hashval,
            self.comment,
        )

    def __repr__(self):
        return self.pp()

    def __eq__(self, other) -> bool:
        return (
            isinstance(other, HashCertificate)
            and other.ref == self.ref
            and other.hashval == self.hashval
        )

    def __ne__(self, other) -> bool:
        return (
            (not isinstance(other, HashCertificate))
            or other.ref != self.ref
            or other.hashval != self.hashval
        )

    def to_json(self) -> Dict[str, Any]:
        return {
            "resource_name": self.ref.name,
            "subpath": self.ref.subpath,
            "certificate": {"cert_type": "hash", "hashval": self.hashval, "comment": self.comment},
        }

    def pp(self, indent: int = 2) -> str:
        """Pretty print with the specified indent level
        """
        return 'HashCertificate(ref=%s, hashval=%s,\n%scomment="%s")' % (
            self.ref,
            self.hashval,
            " " * (indent + 2),
            self.comment,
        )


class PlaceholderCertificate(Certificate):
    __slots__ = ("version",)

    def __init__(self, ref: ResourceRef, version: int, comment: str):
        super().__init__(ref, comment)
        self.version = version

    def pp(self, indent: int = 2) -> str:
        """Pretty print with the specified indent level
        """
        return 'PlaceholderCertificate(ref=%s,  version=%d,\n%scomment="%s")' % (
            self.ref,
            self.version,
            " " * (indent + 2),
            self.comment,
        )

    def create_hash_cert(self, hashval: str) -> HashCertificate:
        return HashCertificate(ref=self.ref, hashval=hashval, comment=self.comment)


class InputPlaceholderCert(PlaceholderCertificate):
    """Variant of placeholder certificate created when we read from a resource
    that has no lineage in the current store or when there is a prior has lineage
    that might be out-of-date.
    """

    __slots__ = ()

    def __str__(self):
        return 'InputPlaceholderCert(ref=%s, version=%d, comment="%s")' % (
            self.ref,
            self.version,
            self.comment,
        )

    def pp(self, indent: int = 2) -> str:
        """Pretty print with the specified indent level
        """
        return 'InputPlaceholderCert(ref=%s,  version=%d,\n%scomment="%s")' % (
            self.ref,
            self.version,
            " " * (indent + 2),
            self.comment,
        )

    def __repr__(self):
        return self.pp()

    def __hash__(self):
        return hash((self.ref, self.version, False),)

    def __eq__(self, other) -> bool:
        return (
            isinstance(other, InputPlaceholderCert)
            and other.ref == self.ref
            and other.version == self.version
        )

    def __ne__(self, other) -> bool:
        return (
            (not isinstance(other, InputPlaceholderCert))
            or other.ref != self.ref
            or other.version != self.version
        )

    def to_json(self) -> Dict[str, Any]:
        return {
            "resource_name": self.ref.name,
            "subpath": self.ref.subpath,
            "certificate": {
                "cert_type": "placeholder",
                "version": self.version,
                "comment": self.comment,
                "is_ouput": False,
            },
        }


class OutputPlaceholderCert(PlaceholderCertificate):
    """Variant of placeholder certificate created when we write to a resource
    (via a step). This one is never compatible with a previous hash, while an
    input placeholder cert might be compatible, if nothing has changed since the
    last snapshot.
    """

    def __str__(self):
        return 'OutputPlaceholderCert(ref=%s, version=%d, comment="%s")' % (
            self.ref,
            self.version,
            self.comment,
        )

    def pp(self, indent: int = 2) -> str:
        """Pretty print with the specified indent level
        """
        return 'OutputPlaceholderCert(ref=%s,  version=%d,\n%scomment="%s")' % (
            self.ref,
            self.version,
            " " * (indent + 2),
            self.comment,
        )

    def __repr__(self):
        return self.pp()

    def __hash__(self):
        return hash((self.ref, self.version, True),)

    def __eq__(self, other) -> bool:
        return (
            isinstance(other, OutputPlaceholderCert)
            and other.ref == self.ref
            and other.version == self.version
        )

    def __ne__(self, other) -> bool:
        return (
            (not isinstance(other, OutputPlaceholderCert))
            or other.ref != self.ref
            or other.version != self.version
        )

    def to_json(self) -> Dict[str, Any]:
        return {
            "resource_name": self.ref.name,
            "subpath": self.ref.subpath,
            "certificate": {
                "cert_type": "placeholder",
                "version": self.version,
                "comment": self.comment,
                "is_ouput": True,
            },
        }


class ResourceLineage(metaclass=ABCMeta):
    """Base class for the lineage of a resource, either a step that wrote it
    or a source data snapshot
    """

    __slots__ = ()

    @staticmethod
    def from_json(obj, filename=None):
        validate_json_keys(obj, ResourceLineage, ["type"], filename=filename)
        restype = obj["type"]
        if restype == "step":
            return StepLineage.from_json(obj, filename=filename)
        elif restype == "source_data":
            return SourceDataLineage.from_json(obj, filename=filename)
        elif restype == "code":
            return CodeLineage.from_json(obj, filename=filename)
        else:
            raise JsonValueError(ResourceLineage, "type", ["step", "source_data"], restype)

    @abstractmethod
    def to_json(self) -> Dict[str, Any]:
        pass

    @abstractmethod
    def pp(self, indent: int = 0) -> str:
        """Pretty print the lineage with the specified indentation level.
        """
        pass

    @abstractmethod
    def get_cert_for_ref(self, ref: ResourceRef) -> Optional[Certificate]:
        """Return the certificate if this resource/subpath, or one that covers
        this ref, is contained in this lineage object. If not, return None
        """
        pass

    @abstractmethod
    def get_certs(self) -> Iterable[Certificate]:
        """Get the certficates for data that was directly sourced by this
        lineage. For a step, these are the outputs of the step. For a
        source data or code lineage, this is the ref it is associated with.
        """
        pass

    @abstractmethod
    def get_input_certs(self) -> Iterable[Certificate]:
        """Get a list of source certs this lineage
        refers to. This will only have a value for step lineage.
        Others will return an empty iteration.
        """
        pass

    @abstractmethod
    def get_code_certs(self) -> Iterable[Certificate]:
        """Get a list of code certs this lineage
        refers to. This will only have a value for step lineage.
        Others will return an empty iteration.
        """
        pass

    @abstractmethod
    def replace_placeholders(self, hash_mapping: Dict[str, str]) -> bool:
        """Replace any placeholder certificates referenced by this lineage
        (either as an input, code or output reference) with HashCertificates referenced
        using the provided hashes. hash_mapping is a mapping from resource name
        to hashes. Returns True if changes were made, False otherwise.

        If a placeholder certificate has no mapping, and a call to get_certs() on the
        lineage would return one or more certificates associated with the resources in
        hash_mapping, then throw LineagePlaceHolderError. This is because the presence
        of a resource from hash_mapping in get_certs() means that the resource will be
        included in the snapshot.
        """
        pass


def _check_for_step_dependency_conflicts(step_name, refs: List[ResourceRef]) -> None:
    """Validate that the certs used by this step as inputs or code do not cover one another.
    Throws LineageConflictError if there is a conflict.
    """
    refs_by_resource = {}  # type: Dict[str, List[ResourceRef]]
    for ref in refs:
        rname = ref.name
        if rname in refs_by_resource:
            for other_ref in refs_by_resource[rname]:
                if other_ref.covers(ref):
                    raise LineageConflictError(
                        "Step %s has dependency on %s, which is a subpath of %s. Please use %s only."
                        % (step_name, ref, other_ref, other_ref)
                    )
                elif ref.covers(other_ref):
                    raise LineageConflictError(
                        "Step %s has dependency on %s, which is a subpath of %s. Please use %s only."
                        % (step_name, other_ref, ref, ref)
                    )
            refs_by_resource[rname].append(ref)
        else:
            refs_by_resource[rname] = [
                ref,
            ]


def _check_for_step_transitive_consistency(
    instance: str, step_name: str, refs: List[ResourceRef], store: "LineageStore"
) -> None:
    """For a list of refs, either for input or code dependencies, check that (transitively)
    only one version of each unique ref is reference. Throws LineageConsistencyError if a mismatch
    is found or if a reference has already been overwritten.

    This is to be called before placeholders are created. A missing entry is going to become
    a placeholder. We need to run *before* adding placeholders so that we do not
    overwrite an older hash with a placeholder.
    """
    to_process = [ref for ref in refs]
    ref_to_cert = {}  # type: Dict[ResourceRef, Certificate]
    while len(to_process) > 0:
        next_to_process = []  # type: List[ResourceRef]
        for ref in to_process:
            try:
                lineage = store.retrieve_entry(instance, ref)
            except LineageNotFoundError:
                # Ref is not yet in store, will be a placeholder
                continue
            cert = lineage.get_cert_for_ref(ref)
            assert cert is not None
            if ref in ref_to_cert:
                other_cert = ref_to_cert[ref]
                if other_cert != cert:
                    raise LineageConsistencyError(
                        "Step %s (transitively) depends on %s which has two versions: %s and %s"
                        % (step_name, ref, cert, other_cert)
                    )
            else:
                ref_to_cert[ref] = cert
            if isinstance(lineage, StepLineage):
                for input_cert in lineage.get_input_certs():
                    if input_cert.ref in ref_to_cert:
                        if input_cert != ref_to_cert[input_cert.ref]:
                            raise LineageConsistencyError(
                                "Step %s (transitively) depends on %s which has two versions: %s and %s"
                                % (
                                    step_name,
                                    input_cert.ref,
                                    input_cert,
                                    ref_to_cert[input_cert.ref],
                                )
                            )
                    else:
                        ref_to_cert[input_cert.ref] = input_cert
                        next_to_process.append(input_cert.ref)
        to_process = next_to_process


class StepLineage(ResourceLineage):
    __slots__ = [
        "step_name",
        "start_time",
        "parameters",
        "input_resources",
        "code_resources",
        "output_resources",
        "outputs_by_resource",
        "execution_time_seconds",
        "command_line",
        "run_from_directory",
    ]

    def __init__(
        self,
        step_name: str,
        start_time: datetime.datetime,
        parameters: Dict[str, Any],
        input_resources: List[Certificate],
        code_resources: List[Certificate],
        output_resources: Optional[List[Certificate]] = None,
        execution_time_seconds: Optional[float] = None,
        command_line: Optional[List[str]] = None,
        run_from_directory: Optional[ResourceRef] = None,
    ):
        self.step_name = step_name
        self.start_time = start_time
        self.parameters = parameters
        self.input_resources = input_resources  # type: List[Certificate]
        self.code_resources = code_resources  # type: List[Certificate]
        self.execution_time_seconds = execution_time_seconds
        self.command_line = command_line
        self.run_from_directory = run_from_directory
        self.output_resources = (
            output_resources if output_resources is not None else []
        )  # type: List[Certificate]
        self.outputs_by_resource = {}  # type: Dict[str, List[Certificate]]
        for oc in self.output_resources:
            rname = oc.ref.name
            if rname in self.outputs_by_resource:
                self.outputs_by_resource[rname].append(oc)
            else:
                self.outputs_by_resource[rname] = [
                    oc,
                ]

    def __str__(self):
        return "StepLineage(step_name=%s,\n  inputs=%s,\n  code=%s,\n  outputs=%s)" % (
            self.step_name,
            [c.ref for c in self.input_resources],
            [c.ref for c in self.code_resources],
            [c.ref for c in self.output_resources],
        )

    def __repr__(self):
        return self.pp()

    def pp(self, indent: int = 0) -> str:
        """Pretty print the lineage with the specified indentation level.
        """

        def pp_certs(name, lst, indent):
            spaces = " " * (indent + 2)
            return (
                indent * " "
                + name
                + "=["
                + (",\n" + spaces).join(c.pp(indent + 2) for c in lst)
                + "]"
            )

        s = " " * indent + "StepLineage(step_name=%s,\n" % self.step_name
        s += pp_certs("inputs", self.input_resources, indent + 2)
        s += ",\n" + pp_certs("outputs", self.output_resources, indent + 2)
        s == ",\n" + pp_certs("code", self.code_resources, indent + 2)
        s += ")"
        return s

    def add_input(self, instance: str, store: "LineageStore", ref: ResourceRef) -> None:
        """Add an input resource after the step has been created.
        This can be called more than once with the same ref - a given ref
        will only be added once.
        """
        for c in self.input_resources:
            if c.ref == ref:
                return  # nothing to add
        self.input_resources.append(
            store.get_or_create_cert(
                instance, ref, "Step %s at %s" % (self.step_name, self.start_time), for_code=False
            )
        )

    @staticmethod
    def make_step_lineage(
        instance: str,
        step_name: str,
        start_time: datetime.datetime,
        parameters: Dict[str, Any],
        input_resource_refs: List[ResourceRef],
        code_resource_refs: List[ResourceRef],
        lineage_store: "LineageStore",
        command_line: Optional[List[str]] = None,
        run_from_directory: Optional[ResourceRef] = None,
    ) -> "StepLineage":
        """At the start of a step's run, create a step lineage object
        to be updated as the step progesses. Validates that the inputs
        to the step are consistent.
        """
        # verify that no dependecy covers another
        _check_for_step_dependency_conflicts(step_name, input_resource_refs)
        _check_for_step_dependency_conflicts(step_name, code_resource_refs)
        # validate that only one dependent version for each input version
        _check_for_step_transitive_consistency(
            instance, step_name, input_resource_refs, lineage_store
        )
        # for now, don't be strict and don't enforce for the code level
        # _check_for_step_transitive_consistency(instance, step_name, code_resource_refs,
        #                                        lambda sl: sl.code_resources,
        #                                        lineage_store)

        # create placeholders for dependencies as needed
        input_certs = [
            lineage_store.get_or_create_cert(
                instance, ref, "Step %s at %s" % (step_name, start_time), for_code=False
            )
            for ref in input_resource_refs
        ]  # List[ResourceCert]
        code_certs = [
            lineage_store.get_or_create_cert(
                instance, ref, "Step %s at %s" % (step_name, start_time), for_code=True
            )
            for ref in code_resource_refs
        ]  # List[ResourceCert]

        # if we got here, we didn't find any inconsistencies
        return StepLineage(
            step_name,
            start_time,
            parameters,
            input_certs,
            code_certs,
            command_line=command_line,
            run_from_directory=run_from_directory,
        )

    def get_cert_for_ref(self, ref: ResourceRef) -> Optional[Certificate]:
        """Return the resource cert if this resource/subpath, or one covering
        this cert, are outputs of the step. If not, return None.
        """
        for cert in self.outputs_by_resource[ref.name]:
            if cert.ref == ref or cert.ref.covers(ref):
                return cert
        return None

    def get_certs(self) -> Iterable[Certificate]:
        """Get all resource certificates associated with this lineage."""
        return self.output_resources

    def get_input_certs(self) -> Iterable[Certificate]:
        return self.input_resources

    def get_code_certs(self) -> Iterable[Certificate]:
        return self.code_resources

    def replace_placeholders(self, hash_mapping: Dict[str, str]) -> bool:
        # has_substitutions = False
        has_resources_in_snapshot = False
        unmapped_placeholders = []
        # First substitute outputs. This is more complex due to the
        # by-resource-name mapping we also keep.
        for i in range(len(self.output_resources)):
            cert = self.output_resources[i]
            if cert.ref.name in hash_mapping:
                has_resources_in_snapshot = True
            if isinstance(cert, PlaceholderCertificate):
                if cert.ref.name in hash_mapping:
                    new_cert = cert.create_hash_cert(hash_mapping[cert.ref.name])
                    self.output_resources[i] = new_cert
                    found = False
                    for (j, other_cert) in enumerate(self.outputs_by_resource[cert.ref.name]):
                        if other_cert == cert:
                            self.outputs_by_resource[cert.ref.name][j] = new_cert
                            found = True
                            break
                    assert found
                    # has_substitutions = True
                else:
                    unmapped_placeholders.append(cert)
        # inputs and code are easier
        for i in range(len(self.input_resources)):
            cert = self.input_resources[i]
            if cert.ref.name in hash_mapping:
                has_resources_in_snapshot = True
            if isinstance(cert, PlaceholderCertificate):
                if cert.ref.name in hash_mapping:
                    new_cert = cert.create_hash_cert(hash_mapping[cert.ref.name])
                    self.input_resources[i] = new_cert
                    # has_substitutions = True
                else:
                    unmapped_placeholders.append(cert)
        for i in range(len(self.code_resources)):
            cert = self.code_resources[i]
            if cert.ref.name in hash_mapping:
                has_resources_in_snapshot = True
            if isinstance(cert, PlaceholderCertificate):
                if cert.ref.name in hash_mapping:
                    new_cert = cert.create_hash_cert(hash_mapping[cert.ref.name])
                    self.code_resources[i] = new_cert
                    # has_substitutions = True
                else:
                    unmapped_placeholders.append(cert)

        if has_resources_in_snapshot and len(unmapped_placeholders) > 0:
            raise LineagePlaceHolderError(
                "Lineage step %s will be included in snapshot, but has unmapped placeholders: %s"
                % (self.step_name, ", ".join([str(c) for c in unmapped_placeholders]))
            )
        # We alway return true, as a multi-output step may have been substituted for another step, but needs
        # needs to be written separately for each output.
        return True

    def add_output(self, instance: str, store: "LineageStore", ref: ResourceRef):
        # first, validate that this path is compatibile with what we already have
        if ref.name in self.outputs_by_resource:
            for cert in self.outputs_by_resource[ref.name]:
                if cert.ref == ref:
                    raise LineageConflictError(
                        "Attempt to add %s as an output to step %s multiple times."
                        % (ref, self.step_name,)
                    )
                elif cert.ref.covers(ref):
                    raise LineageConflictError(
                        "Error adding output %s to step %s: this is a subpath of %s"
                        % (ref, self.step_name, cert.ref)
                    )
                elif ref.covers(cert.ref):
                    raise LineageConflictError(
                        "Error adding output %s to step %s: a subpath already exists in the outputs: %s"
                        % (ref, self.step_name, cert.ref)
                    )
        placeholder = store.get_placeholder_cert_for_output(
            instance, ref, "Step %s at %s" % (self.step_name, self.start_time)
        )
        self.output_resources.append(placeholder)
        if ref.name in self.outputs_by_resource:
            self.outputs_by_resource[ref.name].append(placeholder)
        else:
            self.outputs_by_resource[ref.name] = [
                placeholder,
            ]

    def to_json(self):
        """Return a dictionary containing a json-serializable representation
        of the step lineage.
        """
        return {
            "type": "step",
            "step_name": self.step_name,
            "start_time": self.start_time.isoformat(),
            "execution_time_seconds": self.execution_time_seconds,
            "parameters": self.parameters,
            "input_resources": [r.to_json() for r in self.input_resources],
            "code_resources": [r.to_json() for r in self.code_resources],
            "output_resources": [r.to_json() for r in self.output_resources],
            "command_line": self.command_line,
            "run_from_directory": self.run_from_directory,
        }

    @staticmethod
    def from_json(obj, filename=None):
        validate_json_keys(
            obj,
            StepLineage,
            ["step_name", "start_time", "parameters", "input_resources", "code_resources"],
            filename=filename,
        )
        return StepLineage(
            obj["step_name"],
            isots_to_dt(obj["start_time"]),
            obj["parameters"],
            [Certificate.from_json(rcobj, filename) for rcobj in obj["input_resources"]],
            [Certificate.from_json(rcobj, filename) for rcobj in obj["code_resources"]],
            [Certificate.from_json(rcobj, filename) for rcobj in obj["output_resources"]],
            obj.get("execution_time_seconds", None),
            obj.get("command_line", None),
            obj.get("run_from_directory", None),
        )


class SourceDataLineage(ResourceLineage):
    """Used for a source data resource that is not created
    by any workflow step.
    """

    __slots__ = ["cert"]

    def __init__(self, cert: Certificate):
        self.cert = cert

    def to_json(self) -> Dict[str, Any]:
        obj = self.cert.to_json()
        obj["type"] = "source_data"
        return obj

    @staticmethod
    def from_json(obj, filename=None):
        assert obj["type"] == "source_data"
        return SourceDataLineage(Certificate.from_json(obj, filename=filename))

    def __str__(self):
        return "SourceDataLineage(%s)" % self.cert

    def __repr__(self):
        return self.pp()

    def pp(self, indent: int = 2) -> str:
        return "SourceDataLineage(%s)" % self.cert.pp(indent + 2)

    def get_cert_for_ref(self, ref: ResourceRef) -> Optional[Certificate]:
        if ref == self.cert.ref or self.cert.ref.covers(ref):
            return self.cert
        else:
            return None

    def get_certs(self) -> Iterable[Certificate]:
        return [self.cert]

    def get_input_certs(self) -> Iterable[Certificate]:
        return []

    def get_code_certs(self) -> Iterable[Certificate]:
        return []

    def replace_placeholders(self, hash_mapping: Dict[str, str]) -> bool:
        rname = self.cert.ref.name
        if rname in hash_mapping and isinstance(self.cert, PlaceholderCertificate):
            self.cert = self.cert.create_hash_cert(hash_mapping[rname])
            return True
        else:
            return False


class CodeLineage(ResourceLineage):
    """Used code resource that is referenced by a
    workflow step.
    """

    __slots__ = ["cert"]

    def __init__(self, cert: Certificate):
        self.cert = cert

    def to_json(self) -> Dict[str, Any]:
        obj = self.cert.to_json()
        obj["type"] = "code"
        return obj

    @staticmethod
    def from_json(obj, filename=None) -> "CodeLineage":
        assert obj["type"] == "code"
        return CodeLineage(Certificate.from_json(obj, filename=filename))

    def __str__(self):
        return "CodeLineage(%s)" % self.cert

    def __repr__(self):
        return self.pp()

    def pp(self, indent: int = 2) -> str:
        return "CodeLineage(%s)" % self.cert.pp(indent + 2)

    def get_cert_for_ref(self, ref: ResourceRef) -> Optional[Certificate]:
        if ref == self.cert.ref or self.cert.ref.covers(ref):
            return self.cert
        else:
            return None

    def get_certs(self) -> Iterable[Certificate]:
        return [self.cert]

    def get_input_certs(self) -> Iterable[Certificate]:
        return []

    def get_code_certs(self) -> Iterable[Certificate]:
        return []

    def replace_placeholders(self, hash_mapping: Dict[str, str]) -> bool:
        rname = self.cert.ref.name
        if rname in hash_mapping and isinstance(self.cert, PlaceholderCertificate):
            self.cert = self.cert.create_hash_cert(hash_mapping[rname])
            return True
        else:
            return False


class LineageStore(metaclass=ABCMeta):
    """Abstract interface for storing lineage data. This can have mutiple
    implementations. Workspaces that support lineage should include a lineage store
    and implement the workspace.LineageStoreMixin class.

    The :instance: parameter common to all methods represents the particular workspace copy or
    workflow instance that is running. For the git-based backend, this defaults to the hostname.

    The lineage is a graph with two types of nodes: ResourceLineage objects and ResourceCertificate
    objects. ResourceLineage nodes are the actual lineage, for steps, source data, and code.
    ResourceCertificate nodes represent the state of a resource reference at the time a step was run.
    All ResourceLineage nodes have outbound edges to ResourceCertificate's that represent the state
    created from these lineage nodes. All step lineage nodes also have edges to the resource
    certificates representing the step's inputs.

    The lineage is using an optimistic model where we store a placeholder until the next
    shapshot is taken. When the snapshot is taken, we replace all the placeholder certificates
    with real (hashed) certificates. Placeholder certificates should be generated as follows:

    1. A step lineage should assign a placeholder certificate to each input reference. If the current
       state of the reference is already a placeholder, than that should be used. Otherwise, if there
       is no entry or a hash entry, it should be replaced with a placeholder. When querying for an
       existing entry, we can substitute an entry that covers the requested one, if the requested one
       does not exist, but the covering one does. This will prevent some spurious conflict errors.
    2. Each step output should be assigned with a new placeholder certificate, even if there is already
       one for the reference.
    3. Before the snapshot_lineage() method is called, all placeholders should be replaced with hash
       certificates. If any are left when taking a snapshot, a LineagePlaceholderError should be thrown.
    """

    @abstractmethod
    def store_entry(self, instance: str, lineage: ResourceLineage) -> None:
        """Store the specified lineage object at the specific reference for the
        workspace instance. This should throw a LineageConflictError if it
        would cover an existing entry or an existing entry would cover this
        resource.

        Future queries based on the refs associated with this lineage should return it. 
        """
        pass

    @abstractmethod
    def retrieve_entry(self, instance: str, ref: ResourceRef) -> ResourceLineage:
        """Retrieve the specified entry. If an exact match is not found,
        return an entry that covers the specified ref. If still not match
        is found, raise a LineageNotFoundError.
        """
        pass

    @abstractmethod
    def has_entry(self, instance: str, ref: ResourceRef, include_covers: bool = True) -> bool:
        """Return True if the specified lineage exists in the store.
        If include_covers is True and there isn't an exact match,
        also return True if there exists a cover for this reference in the store.
        """
        pass

    @abstractmethod
    def get_refs_for_resource(self, instance: str, resource_name: str) -> Iterable[ResourceRef]:
        """Iterate through all the refs in this store belonging to this resource.
        This can be an empty list, if the resource is not in the lineage store,
        a one-element list if the resource name with no subpath is in the store, or
        a multi-element list of multiple subpaths for the resource are in the store
        """
        pass

    @abstractmethod
    def clear_entry(self, instance: str, ref: ResourceRef) -> None:
        """Clear any entry at the specified resource reference as well as any
        entries covered by this reference. 
        """
        pass

    @abstractmethod
    def replace_placeholders(
        self, instance: str, hash_mapping: Dict[str, str], verbose: bool = False
    ) -> None:
        """Replace any placeholder certificates associated with the specified resources
        with HashCertificate's that have the specified hash. This is done ahead of a snapshot.
        Throws a LineagePlaceHolderError if a placeholder is not replaced and would be included
        in a snapshot. See the documentation for ResourceLineage.replace_placeholders() for
        an explanation of where this would occur.

        hash_mapping is a mapping from resource *names* to hashes. All refs under a resource
        will have the same hash, because snapshots are capturing their hashes at the granularity
        at the resource level. TODO: look into doing this at the subpath level as well.
        """
        pass

    @abstractmethod
    def snapshot_lineage(
        self, instance: str, snapshot_hash: str, resource_names: List[str]
    ) -> None:
        """Save the current lineage data for the specified snapshot.
        Only the specified resources are processed. If there is no
        lineage available for a given resource, an empty entry should
        be saved, so that restoring will clear-out any existing lineage.
        """
        pass

    @abstractmethod
    def restore_lineage(
        self,
        instance: str,
        snapshot_hash: str,
        resources_to_restore: List[str],
        verbose: bool = False,
    ) -> None:
        """Restore the lineage for the specified resources from the specified snapshot.
        Any existing entries for the specified resources should first be cleared.
        Then, any entries for those resources copied to the current lineage.
        If a resource has not entries, that is fine, it remains in the cleared
        (unknown) state.
        """
        pass

    @abstractmethod
    def delete_snapshot_lineage(self, instance: str, snapshot_hash: str) -> None:
        """Delete any lineage data associated with the specified snapshot.
        """
        pass

    @abstractmethod
    def iterate_all(self, instance: str) -> Iterable[Tuple[ResourceRef, ResourceLineage]]:
        """Iterate through the contents of the store
        """
        pass

    @abstractmethod
    def iterate_all_as_of_snapshot(
        self, instance: str, snapshot_hash: str
    ) -> Iterable[Tuple[ResourceRef, ResourceLineage]]:
        """Iterate through the contents of the store, as of the specific snapshot.
        """
        pass

    @abstractmethod
    def dump(self, instance: str) -> None:
        """Print the current contents of the store (for debugging).
        """
        pass

    @abstractmethod
    def retrieve_entry_as_of_snapshot(
        self, instance: str, ref: ResourceRef, snapshot_hash: str
    ) -> ResourceLineage:
        """Retrieve the specified entry as of the specified snapshot hash. If an exact
        match is not found, return an entry that covers the specified ref. If still
        not match is found, raise a LineageNotFoundError.
        """
        pass

    @abstractmethod
    def has_entry_as_of_snapshot(
        self, instance: str, ref: ResourceRef, snapshot_hash: str, include_covers: bool = True
    ) -> bool:
        """Return True if the specified lineage exists in the specified snapshot.
        If include_covers is True and there isn't an exact match,
        also return True if there exists a cover for this reference in the store.
        """
        pass

    @abstractmethod
    def get_refs_for_resource_as_of_snapshot(
        self, instance: str, resource_name: str, snapshot_hash: str
    ) -> Iterable[ResourceRef]:
        """Iterate through all the refs in the snapshot belonging to this resource.
        This can be an empty list, if the resource is not in the lineage store,
        a one-element list if the resource name with no subpath is in the store, or
        a multi-element list of multiple subpaths for the resource are in the store
        """
        pass

    def get_or_create_cert(
        self, instance: str, ref: ResourceRef, comment: str, for_code: bool = False
    ) -> Certificate:
        """If there is a lineage at the specified ref, then return the associated certificate.
        Otherwise, create a new input placeholder with the specified comment and return that
        placeholder.
        """
        if self.has_entry(instance, ref):
            existing = self.retrieve_entry(instance, ref)
            cert = existing.get_cert_for_ref(ref)
            assert cert is not None
            return cert
        else:
            new_cert = InputPlaceholderCert(ref, version=1, comment=comment)
            new_lineage = CodeLineage(new_cert) if for_code else SourceDataLineage(new_cert)
            self.store_entry(instance, new_lineage)
            return new_cert

    def get_placeholder_cert_for_output(
        self, instance: str, ref: ResourceRef, comment: str
    ) -> OutputPlaceholderCert:
        """Get a new placeholder certificate for using in a step output.
        If there already is a placeholder there, we bump up the version.
        We don't store anything in the lineage store itself until the step
        completes.
        """
        if self.has_entry(instance, ref):
            existing = self.retrieve_entry(instance, ref)
            cert = existing.get_cert_for_ref(ref)
            assert cert is not None
            if isinstance(cert, PlaceholderCertificate):
                return OutputPlaceholderCert(ref, version=cert.version + 1, comment=comment)
        return OutputPlaceholderCert(ref, version=1, comment=comment)

    def get_lineage_for_resource(
        self, instance: str, resource_name: str
    ) -> Tuple[List[ResourceLineage], int]:
        """Return a list of all transitive lineage for the specified
        resource and a integer indicating the number of warnings.
        """
        ref_to_cert = {}  # type: Dict[ResourceRef, Certificate]
        result = []  # type: List[ResourceLineage]
        warnings = 0
        to_process = [ref for ref in self.get_refs_for_resource(instance, resource_name)]
        if len(to_process) == 0:
            print(
                "WARNING: no lineage data found for resource '%s'" % resource_name, file=sys.stderr
            )
            return ([], 1)

        while len(to_process) > 0:
            next_to_process = []  # type: List[ResourceRef]
            for ref in to_process:
                try:
                    lineage = self.retrieve_entry(instance, ref)
                except LineageNotFoundError:
                    assert 0, "No entry found for ref %s" % repr(
                        ref
                    )  # should not happen as we check inputs
                    continue
                cert = lineage.get_cert_for_ref(ref)
                assert cert is not None
                if ref in ref_to_cert:
                    other_cert = ref_to_cert[ref]
                    if other_cert != cert:
                        print(
                            "WARNING: Resource %s (transitively) depends on %s which has two versions: %s and %s"
                            % (resource_name, ref, cert, other_cert),
                            file=sys.stderr,
                        )
                        warnings += 1
                        continue
                else:
                    ref_to_cert[ref] = cert
                    result.append(lineage)
                if isinstance(lineage, StepLineage):
                    for input_cert in lineage.get_input_certs():
                        if input_cert.ref in ref_to_cert:
                            if input_cert != ref_to_cert[input_cert.ref]:
                                print(
                                    "WARNING: Resource %s (transitively) depends on %s which has two versions: %s and %s"
                                    % (
                                        resource_name,
                                        input_cert.ref,
                                        input_cert,
                                        ref_to_cert[input_cert.ref],
                                    ),
                                    file=sys.stderr,
                                )
                                warnings += 1
                                continue
                        else:
                            next_to_process.append(input_cert.ref)
                            ref_to_cert[input_cert.ref] = input_cert
                            try:
                                result.append(self.retrieve_entry(instance, input_cert.ref))
                            except LineageNotFoundError:
                                print(
                                    "WARNING: step %s references input %s, which has no lingeage"
                                    % (lineage.step_name, input_cert.ref),
                                    file=sys.stderr,
                                )
                    for code_cert in lineage.get_code_certs():
                        if code_cert.ref in ref_to_cert:
                            if code_cert != ref_to_cert[code_cert.ref]:
                                print(
                                    "WARNING: Resource %s (transitively) depends on %s which has two versions: %s and %s"
                                    % (
                                        resource_name,
                                        code_cert.ref,
                                        code_cert,
                                        ref_to_cert[code_cert.ref],
                                    ),
                                    file=sys.stderr,
                                )
                                warnings += 1
                                continue
                        else:
                            next_to_process.append(code_cert.ref)
                            ref_to_cert[code_cert.ref] = code_cert
                            try:
                                result.append(self.retrieve_entry(instance, code_cert.ref))
                            except LineageNotFoundError:
                                print(
                                    "WARNING: step %s references code resource %s, which has no lingeage"
                                    % (lineage.step_name, code_cert.ref),
                                    file=sys.stderr,
                                )
            to_process = next_to_process
        return (result, warnings)


class FileLineageStore(LineageStore):
    """Store lineage data on the local filesystem.
    """

    def __init__(self, instance: str, current_lineage_path: str, snapshot_lineage_path: str):
        """:current_lineage_path: is private to the instance.

        :snapshot_lineage_path: should be replicated/visible to all instances
        of the workspace.

        We pass in :instance: to the constructor as this implementation works against
        local state only and the instance parameters of the methods must all match
        this instance.
        """
        self.instance = instance
        self.current_lineage_path = current_lineage_path
        self.snapshot_lineage_path = snapshot_lineage_path
        # Write-through cache of the resources. We use this
        # to make following backlinks faster.
        # This is a dict from resource names to resource ref to lineage mappings.
        # Note that a a given lineage object may be independently repeated in multiple
        # places. This is OK, as long as any changes are made identically to all copies.
        self.resource_cache = {}  # type: Dict[str, Dict[ResourceRef, ResourceLineage]]

    def _rfile_exists(self, resource_name: str) -> bool:
        return exists(join(self.current_lineage_path, resource_name + ".json"))

    def _parse_rfile(self, resource_name: str) -> Dict[ResourceRef, ResourceLineage]:
        if resource_name in self.resource_cache:
            return self.resource_cache[resource_name]

        rfile_path = join(self.current_lineage_path, resource_name + ".json")
        with open(rfile_path, "r") as f:
            data = json.load(f)
        assert isinstance(data, dict), (
            "Resource lineage file %s is not in correct format" % rfile_path
        )
        # For backward compatibility, the resource file is a list of lineages rather than a map from
        # refs to lineages. We need to recreate the map.
        # TODO: support reading/writing a new format that uses the map directly
        lineages = [ResourceLineage.from_json(r, rfile_path) for r in data["lineages"]]
        mapping = {}  # type: Dict[ResourceRef, ResourceLineage]
        for l in lineages:
            for rc in l.get_certs():
                if rc.ref.name == resource_name:
                    mapping[rc.ref] = l
        self.resource_cache[resource_name] = mapping
        return mapping

    def _load_resource_cache(self):
        """When tracking backlinks, we need to go through the entire current
        resource database. We load it all in memory to speed things up.
        """
        files = os.listdir(self.current_lineage_path)
        for f in files:
            if f.endswith(".json"):
                rname = f[0 : -len(".json")]
                if rname not in self.resource_cache:
                    self._parse_rfile(rname)

    def _save_rfile_to_curr(
        self, resource_name: str, lineage_map: Dict[ResourceRef, ResourceLineage]
    ) -> str:
        """Save the resource mapping to the current lineage. Returns the path in case it
        is needed by a subpclass
        """
        rfile_path = join(self.current_lineage_path, resource_name + ".json")
        # for backward compability, we just save the lineage values
        with open(rfile_path, "w") as f:
            json.dump(
                {
                    "resource_name": resource_name,
                    "lineages": [r.to_json() for r in lineage_map.values()],
                },
                f,
                indent=2,
            )
        return rfile_path

    def _get_snapshot_path(self, resource_name: str, snapshot_hash: str) -> str:
        return join(join(self.snapshot_lineage_path, snapshot_hash), resource_name + ".json")

    def _snapshot_rfile_exists(self, resource_name: str, snapshot_hash: str) -> bool:
        return exists(self._get_snapshot_path(resource_name, snapshot_hash))

    def _ensure_snapshot_dir_exists(self, snapshot_hash: str) -> None:
        snapshot_dir = join(self.snapshot_lineage_path, snapshot_hash)
        if not exists(snapshot_dir):
            os.makedirs(snapshot_dir)

    def _get_resources_in_snapshot(self, snapshot_hash: str) -> Iterable[str]:
        snapshot_dir = join(self.snapshot_lineage_path, snapshot_hash)
        if not isdir(snapshot_dir):
            raise LineageNotFoundError("No lineage data found for snapshot hash %s" % snapshot_hash)
        for fname in sorted(os.listdir(snapshot_dir)):
            if fname.endswith(".json"):
                yield fname[0:-5]

    def _parse_snapshot_rfile(
        self, resource_name: str, snapshot_hash: str
    ) -> Dict[ResourceRef, ResourceLineage]:
        """Since this is for a snapshot, it does not use the cache.
        """
        rfile_path = self._get_snapshot_path(resource_name, snapshot_hash)
        with open(rfile_path, "r") as f:
            data = json.load(f)
        assert isinstance(data, dict), "Lineage file %s is not in expected format" % rfile_path
        # For backward compatibility, the resource file is a list of lineages rather than a map from
        # refs to lineages. We need to recreate the map.
        # TODO: support reading/writing a new format that uses the map directly
        lineages = [ResourceLineage.from_json(r, rfile_path) for r in data["lineages"]]
        mapping = {}  # type: Dict[ResourceRef, ResourceLineage]
        for l in lineages:
            for rc in l.get_certs():
                if rc.ref.name == resource_name:
                    mapping[rc.ref] = l
        return mapping

    def _save_rfile_to_snapshot(
        self,
        resource_name: str,
        lineage_map: Dict[ResourceRef, ResourceLineage],
        snapshot_hash: str,
    ) -> str:
        """Save the resource mapping to the snapshot. Returns the path in case it
        is needed by a subclass.
        """
        snapshot_path = self._get_snapshot_path(resource_name, snapshot_hash)
        # for backward compability, we just save the lineage values
        with open(snapshot_path, "w") as f:
            json.dump(
                {
                    "resource_name": resource_name,
                    "lineages": [r.to_json() for r in lineage_map.values()],
                },
                f,
                indent=2,
            )
        return snapshot_path

    def _copy_rfile_to_snapshot(self, resource_name: str, snapshot_hash: str) -> Tuple[str, str]:
        src_rpath = join(self.current_lineage_path, resource_name + ".json")
        dest_rpath = self._get_snapshot_path(resource_name, snapshot_hash)
        shutil.copyfile(src_rpath, dest_rpath)
        return (src_rpath, dest_rpath)

    def _copy_snapshot_rfile_to_current(
        self, resource_name: str, snapshot_hash: str
    ) -> Tuple[str, str]:
        src_rpath = self._get_snapshot_path(resource_name, snapshot_hash)
        dest_rpath = join(self.current_lineage_path, resource_name + ".json")
        shutil.copyfile(src_rpath, dest_rpath)
        return (src_rpath, dest_rpath)

    def _write_placeholder_to_snapshot(
        self, snapshot_hash: str, filename: str, content: str
    ) -> str:
        path = join(join(self.snapshot_lineage_path, snapshot_hash), filename)
        with open(path, "w") as f:
            f.write(content)
        return path

    def _delete_from_current(self, resource_name: str) -> str:
        rfile_path = join(self.current_lineage_path, resource_name + ".json")
        os.remove(rfile_path)
        return rfile_path

    def store_entry(self, instance: str, lineage: ResourceLineage) -> None:
        assert instance == self.instance
        for cert in lineage.get_certs():
            if self._rfile_exists(cert.ref.name):
                # case where we need to merge into data
                mapping = self._parse_rfile(cert.ref.name)
                # check for conflicts
                for other_ref in mapping.keys():
                    if other_ref == cert.ref:
                        break  # got an exact match, there won't be conflicts
                    elif other_ref.covers(cert.ref):
                        raise LineageConflictError(
                            "Cannot store new lineage data at %s: existing lineage data %s is a parent path"
                            % (cert.ref, other_ref)
                        )
                    elif cert.ref.covers(other_ref):
                        # TODO: Consider whether we can allow conflicts in this case.
                        raise LineageConflictError(
                            "Cannot store new lineage data at %s: existing lineage data %s is a child path"
                            % (cert.ref, other_ref)
                        )
                mapping[cert.ref] = lineage
            else:
                mapping = {cert.ref: lineage}
            self.resource_cache[cert.ref.name] = mapping
            self._save_rfile_to_curr(cert.ref.name, mapping)

    def retrieve_entry(self, instance: str, ref: ResourceRef) -> ResourceLineage:
        assert instance == self.instance
        if not self._rfile_exists(ref.name):
            raise LineageNotFoundError("No lineage exists for %s" % str(ref))
        mapping = self._parse_rfile(ref.name)
        for (other_ref, lineage) in mapping.items():
            if ref == other_ref or other_ref.covers(ref):
                return lineage
        raise LineageNotFoundError("No lineage exists for %s" % str(ref))

    def has_entry(self, instance: str, ref: ResourceRef, include_covers: bool = True) -> bool:
        assert instance == self.instance
        if not self._rfile_exists(ref.name):
            return False
        mapping = self._parse_rfile(ref.name)
        for (other_ref, lineage) in mapping.items():
            if ref == other_ref or (include_covers and other_ref.covers(ref)):
                return True
        return False

    def clear_entry(self, instance: str, ref: ResourceRef) -> None:
        assert instance == self.instance
        if ref.subpath is None:
            # special case when its the entire file
            if self._rfile_exists(ref.name):
                self._delete_from_current(ref.name)
            if ref.name in self.resource_cache:
                del self.resource_cache[ref.name]
        else:
            mapping = self._parse_rfile(ref.name)
            keys = [k for k in mapping.keys()]
            changed = False
            for key in keys:
                if ref == key or ref.covers(key):
                    del mapping[key]  # also updates the cache
                    changed = True
            if changed:
                self._save_rfile_to_curr(ref.name, mapping)

    def get_refs_for_resource(self, instance: str, resource_name: str) -> Iterable[ResourceRef]:
        """Iterate through all the refs in this store belonging to this resource.
        This can be an empty list, if the resource is not in the lineage store,
        a one-element list if the resource name with no subpath is in the store, or
        a multi-element list of multiple subpaths for the resource are in the store
        """
        assert instance == self.instance
        if not self._rfile_exists(resource_name):
            return []
        mapping = self._parse_rfile(resource_name)
        return mapping.keys()

    def replace_placeholders(
        self, instance: str, hash_mapping: Dict[str, str], verbose=False
    ) -> None:
        assert instance == self.instance
        # we load the entire current store, as following the backlinks can go to any resource
        self._load_resource_cache()
        dirty_resources = set()  # need to save these at the end
        for (rname, mapping) in self.resource_cache.items():
            for (ref, lineage) in mapping.items():
                dirty = lineage.replace_placeholders(hash_mapping)
                if dirty:
                    dirty_resources.add(rname)
                    if verbose:
                        print(
                            "replaced placeholders for ref %s lineage\n   %s" % (repr(ref), lineage)
                        )
                else:
                    if verbose:
                        print("No placeholders for ref %s lineage:  \n%s" % (repr(ref), lineage))
        for rname in dirty_resources:
            self._save_rfile_to_curr(rname, self.resource_cache[rname])

    def snapshot_lineage(
        self, instance: str, snapshot_hash: str, resource_names: List[str]
    ) -> None:
        assert instance == self.instance
        self._ensure_snapshot_dir_exists(snapshot_hash)
        if len(resource_names) == 0:
            self._write_placeholder_to_snapshot(
                snapshot_hash,
                "placeholder.txt",
                "No resources for lineage snapshot %s\n" % snapshot_hash,
            )
            return
        for resource_name in resource_names:
            if self._rfile_exists(resource_name):
                self._copy_rfile_to_snapshot(resource_name, snapshot_hash)
            else:
                self._save_rfile_to_snapshot(resource_name, {}, snapshot_hash)

    def restore_lineage(
        self, instance: str, snapshot_hash: str, resources_to_restore: List[str], verbose=False
    ) -> None:
        assert instance == self.instance
        snapshot_dir = join(self.snapshot_lineage_path, snapshot_hash)
        if not exists(snapshot_dir):
            raise LineageNotFoundError("Did not find lineage data for snapshot %s" % snapshot_hash)
        for resource_name in resources_to_restore:
            if self._snapshot_rfile_exists(resource_name, snapshot_hash):
                (src_rpath, dest_rpath) = self._copy_snapshot_rfile_to_current(
                    resource_name, snapshot_hash
                )
                if verbose:
                    print("Restore: copied %s to %s" % (src_rpath, dest_rpath))
            elif self._rfile_exists(resource_name):
                # if included in the restore, but no lineage data remove current
                deleted_rfile = self._delete_from_current(resource_name)
                if verbose:
                    print(
                        "Removed %s, as %s has no lineage data with this snapshot"
                        % (deleted_rfile, resource_name)
                    )
            else:
                if verbose:
                    print("No lineage data for resource %s" % resource_name)
        # invalidate the cache
        self.resource_cache = {}  # type: ignore

    def delete_snapshot_lineage(self, instance: str, snapshot_hash: str) -> None:
        """Delete any lineage data associated with the specified snapshot.
        """
        snapshot_dir = join(self.snapshot_lineage_path, snapshot_hash)
        if exists(snapshot_dir):
            shutil.rmtree(snapshot_dir)

    def iterate_all(self, instance: str) -> Iterable[Tuple[ResourceRef, ResourceLineage]]:
        """Iterate through the contents of the store
        """
        self._load_resource_cache()
        for (rname, mapping) in self.resource_cache.items():
            for (ref, lineage) in mapping.items():
                yield (ref, lineage)

    def iterate_all_as_of_snapshot(
        self, instance: str, snapshot_hash: str
    ) -> Iterable[Tuple[ResourceRef, ResourceLineage]]:
        """Iterate through the contents of the store, as of the specific snapshot.
        """
        for rname in self._get_resources_in_snapshot(snapshot_hash):
            for ref in self.get_refs_for_resource_as_of_snapshot(instance, rname, snapshot_hash):
                yield (ref, self.retrieve_entry_as_of_snapshot(instance, ref, snapshot_hash))

    def dump(self, instance: str) -> None:
        self._load_resource_cache()

        def _indent(s, level, underline=None):
            for line in s.split("\n"):
                indented = " " * level + line
                print(indented)
                if underline is not None:
                    print(" " * level + underline * len(line))

        _indent("Lineage store", 2, "=")
        for (rname, mapping) in self.resource_cache.items():
            _indent("Resource %s" % rname, 4, "-")
            for (ref, lineage) in mapping.items():
                _indent(str(ref) + ":", 6)
                _indent(json.dumps(lineage.to_json(), indent=2), 8)
        print()

    def retrieve_entry_as_of_snapshot(
        self, instance: str, ref: ResourceRef, snapshot_hash: str
    ) -> ResourceLineage:
        """Retrieve the specified entry as of the specified snapshot hash. If an exact
        match is not found, return an entry that covers the specified ref. If still
        not match is found, raise a LineageNotFoundError.
        """
        if not self._snapshot_rfile_exists(ref.name, snapshot_hash):
            raise LineageNotFoundError("%s as of snapshot %s" % (ref.name, snapshot_hash))
        mapping = self._parse_snapshot_rfile(ref.name, snapshot_hash)
        for (other_ref, lineage) in mapping.items():
            if ref == other_ref or other_ref.covers(ref):
                return lineage
        raise LineageNotFoundError("No lineage exists for %s as of %s" % (str(ref), snapshot_hash))

    def has_entry_as_of_snapshot(
        self, instance: str, ref: ResourceRef, snapshot_hash: str, include_covers: bool = True
    ) -> bool:
        """Return True if the specified lineage exists in the specified snapshot.
        If include_covers is True and there isn't an exact match,
        also return True if there exists a cover for this reference in the store.
        """
        if not self._snapshot_rfile_exists(ref.name, snapshot_hash):
            raise LineageNotFoundError("%s as of snapshot %s" % (ref.name, snapshot_hash))
        mapping = self._parse_snapshot_rfile(ref.name, snapshot_hash)
        for (other_ref, lineage) in mapping.items():
            if ref == other_ref or (include_covers and other_ref.covers(ref)):
                return True
        return False

    def get_refs_for_resource_as_of_snapshot(
        self, instance: str, resource_name: str, snapshot_hash: str
    ) -> Iterable[ResourceRef]:
        """Iterate through all the refs in the snapshot belonging to this resource.
        This can be an empty list, if the resource is not in the lineage store,
        a one-element list if the resource name with no subpath is in the store, or
        a multi-element list of multiple subpaths for the resource are in the store
        """
        if not self._snapshot_rfile_exists(resource_name, snapshot_hash):
            return []
        mapping = self._parse_snapshot_rfile(resource_name, snapshot_hash)
        return mapping.keys()


def make_lineage_table(
    instance: str, store: LineageStore, snapshot_hash: Optional[str] = None
) -> Iterable[Tuple[str, str, str, Optional[List[str]]]]:
    """Make a table of the lineage for each resource.
    The columns are: ref, lineage type, details, inputs
    """

    def ref_name(ref) -> str:
        return ref.name if ref.subpath is None else ref.name + ":/" + ref.subpath

    def cert_name(cert) -> str:
        if isinstance(cert, HashCertificate):
            if is_a_git_hash(cert.hashval):
                return "Hash:%s" % cert.hashval[0:8]
            else:
                return "Hash:%s" % cert.hashval
        else:
            assert isinstance(cert, PlaceholderCertificate)
            return "Placeholder:version=%d" % cert.version

    def input_to_str(cert):
        try:
            if snapshot_hash is not None:
                lg = store.retrieve_entry_as_of_snapshot(instance, cert.ref, snapshot_hash)
            else:
                lg = store.retrieve_entry(instance, cert.ref)
        except LineageNotFoundError:
            return "%s (%s)" % (ref_name(cert.ref), cert_name(cert))
        cur_cert = lg.get_cert_for_ref(cert.ref)
        if cur_cert is None or cur_cert != cert:
            return "%s (%s)" % (ref_name(cert.ref), cert_name(cert))
        else:
            return "%s (current)" % ref_name(cert.ref)

    def lineage_to_cols(lineage) -> Tuple[str, str, Optional[List[str]]]:
        if isinstance(lineage, StepLineage):
            sname = "%s at %s" % (lineage.step_name, lineage.start_time)
            return ("Step", sname, [input_to_str(i) for i in lineage.input_resources])
        elif isinstance(lineage, SourceDataLineage):
            return ("Source Data", cert_name(lineage.cert), None)
        elif isinstance(lineage, CodeLineage):
            return ("Code", cert_name(lineage.cert), None)
        else:
            assert 0

    iterator = (
        store.iterate_all_as_of_snapshot(instance, snapshot_hash)
        if snapshot_hash is not None
        else store.iterate_all(instance)
    )
    for (ref, lineage) in iterator:
        (ltype, details, inputs) = lineage_to_cols(lineage)
        yield (ref_name(ref), ltype, details, inputs)


GRAPH_TEMPLATE_FILE = abspath(join(dirname(__file__), "../third_party/lineage_graph_template.html"))


def make_lineage_graph_for_visualization(
    instance: str, store: LineageStore, output_file: str, width=1024, height=800
) -> None:
    """This builds a lineage graph of the entire repo, mostly for debugging
    purposes.
    """
    next_node_id = 1
    ref_nodes = {}  # type: Dict[ResourceRef, int]
    cert_nodes = {}  # type: Dict[Certificate, int]
    lineage_nodes = {}  # type: Dict[str, int]
    nodes = []  # type: List[Dict[str, Any]]
    links = []  # type: List[Dict[str, Any]]

    def ref_name(ref):
        return ref.name if ref.subpath is None else ref.name + ":/" + ref.subpath

    def cert_name(cert):
        return (
            ref_name(cert.ref)
            + ":"
            + (
                cert.hashval
                if isinstance(cert, HashCertificate)
                else "version=%d" % cast(PlaceholderCertificate, cert).version
            )
        )

    def lineage_to_names(lineage):
        if isinstance(lineage, StepLineage):
            sname = "Step %s at %s" % (lineage.step_name, lineage.start_time)
            return (sname, sname)
        elif isinstance(lineage, SourceDataLineage):
            return ("SourceData", cert_name(lineage.cert))
        elif isinstance(lineage, CodeLineage):
            return ("Code", cert_name(lineage.cert))

    for (ref, lineage) in store.iterate_all(instance):
        if ref not in ref_nodes:
            ref_node = {"name": ref_name(ref), "label": "Ref", "id": next_node_id}
            nodes.append(ref_node)
            ref_nodes[ref] = next_node_id
            next_node_id += 1
        cert = lineage.get_cert_for_ref(ref)
        assert cert is not None
        if cert not in cert_nodes:
            c_node = {"name": cert_name(cert), "label": "Cert", "id": next_node_id}
            nodes.append(c_node)
            cert_nodes[cert] = next_node_id
            next_node_id += 1
        (sname, lname) = lineage_to_names(lineage)
        if lname not in lineage_nodes:
            l_node = {"name": sname, "label": "Lineage", "id": next_node_id}
            nodes.append(l_node)
            lineage_nodes[lname] = next_node_id
            next_node_id += 1
        links.append({"source": ref_nodes[ref], "target": cert_nodes[cert], "type": "CERT"})
        links.append(
            {"source": cert_nodes[cert], "target": lineage_nodes[lname], "type": "LINEAGE"}
        )
        if isinstance(lineage, StepLineage):
            for icert in lineage.get_input_certs():
                if icert not in cert_nodes:
                    ic_node = {"name": cert_name(icert), "label": "Cert", "id": next_node_id}
                    cert_nodes[icert] = next_node_id
                    next_node_id += 1
                    nodes.append(ic_node)
                    if icert.ref not in ref_nodes:
                        iref_node = {
                            "name": ref_name(icert.ref),
                            "label": "Ref",
                            "id": next_node_id,
                        }
                        nodes.append(iref_node)
                        ref_nodes[icert.ref] = next_node_id
                        next_node_id += 1
                    links.append(
                        {
                            "source": ref_nodes[icert.ref],
                            "target": cert_nodes[icert],
                            "type": "CERT",
                        }
                    )
                links.append(
                    {"source": lineage_nodes[lname], "target": cert_nodes[icert], "type": "INPUT"}
                )
    if not exists(GRAPH_TEMPLATE_FILE):
        raise InternalError("Could not find lineage graph template")
    graph_str = json.dumps({"nodes": nodes, "links": links}, indent=2)
    with open(GRAPH_TEMPLATE_FILE, "r") as f, open(output_file, "w") as g:
        data = f.read()
        t = Template(data)
        g.write(t.substitute(LINEAGE_GRAPH=graph_str, WIDTH=str(width), HEIGHT=str(height)))


def make_simplified_lineage_graph_for_resource(
    instance: str,
    store: LineageStore,
    resource_name: str,
    output_file: str,
    snapshot_hash: Optional[str],
    format="html",
    width=1024,
    height=800,
) -> None:
    nodes = []  # type: List[Dict[str, Any]]
    links = []  # type: List[Dict[str, Any]]

    def ref_name(ref):
        return ref.name if ref.subpath is None else ref.name + ":/" + ref.subpath

    def cert_short_name(cert):
        return (
            cert.hashval[0:7]
            if isinstance(cert, HashCertificate)
            else "placeholder=%d" % cast(PlaceholderCertificate, cert).version
        )

    def step_lineage_to_name(lineage):
        assert isinstance(lineage, StepLineage)
        return "%s@%s" % (lineage.step_name, str(lineage.start_time)[0:16])

    class CertNodes:
        def __init__(self):
            self.next_node_id = 1
            self.cert_nodes = {}  # type: Dict[Certificate, int]

        def get_cert_node(self, cert: Certificate) -> Tuple[int, bool]:
            if cert in self.cert_nodes:
                return (self.cert_nodes[cert], False)
            else:
                node_id = self.next_node_id
                nodes.append(
                    {"name": ref_name(cert.ref), "label": cert_short_name(cert), "id": node_id}
                )
                self.cert_nodes[cert] = node_id
                self.next_node_id += 1
                return (node_id, True)

    def get_cert_and_lineage(ref: ResourceRef) -> Tuple[Certificate, ResourceLineage]:
        if snapshot_hash is not None:
            lineage = store.retrieve_entry_as_of_snapshot(instance, ref, snapshot_hash)
        else:
            lineage = store.retrieve_entry(instance, ref)
        cert = lineage.get_cert_for_ref(ref)
        assert cert is not None
        return (cert, lineage)

    def cert_in_lineage(cert: Certificate) -> bool:
        if snapshot_hash is not None:
            lineage = store.retrieve_entry_as_of_snapshot(instance, cert.ref, snapshot_hash)
        else:
            lineage = store.retrieve_entry(instance, cert.ref)
        other_cert = lineage.get_cert_for_ref(cert.ref)
        if other_cert == cert:
            return True
        else:
            print(
                "Warning: Certificate %s not found in store, was overwritten by %s"
                % (cert, other_cert)
            )
            return False

    cn = CertNodes()
    if snapshot_hash is not None:
        worklist = [
            ref
            for ref in store.get_refs_for_resource_as_of_snapshot(
                instance, resource_name, snapshot_hash
            )
        ]
    else:
        worklist = [ref for ref in store.get_refs_for_resource(instance, resource_name)]
    if len(worklist) == 0:
        raise LineageError("No lineage found for resource %s" % resource_name)
    while len(worklist) > 0:
        next_worklist = []
        for ref in worklist:
            (cert, lineage) = get_cert_and_lineage(ref)
            (node_id, is_new) = cn.get_cert_node(cert)
            if lineage is not None and isinstance(lineage, StepLineage):
                for input_cert in lineage.get_input_certs():
                    (input_node_id, input_is_new) = cn.get_cert_node(input_cert)
                    links.append(
                        {
                            "source": input_node_id,
                            "target": node_id,
                            "type": step_lineage_to_name(lineage),
                        }
                    )
                    if input_is_new and cert_in_lineage(input_cert):
                        next_worklist.append(input_cert.ref)
        worklist = next_worklist
    if format == "html":
        if not exists(GRAPH_TEMPLATE_FILE):
            raise InternalError("Could not find lineage graph template")
        graph_str = json.dumps({"nodes": nodes, "links": links}, indent=2)
        with open(GRAPH_TEMPLATE_FILE, "r") as f, open(output_file, "w") as g:
            data = f.read()
            t = Template(data)
            g.write(t.substitute(LINEAGE_GRAPH=graph_str, WIDTH=str(width), HEIGHT=str(height)))
    elif format == "dot":
        with open(output_file, "w") as g:
            g.write("digraph lineage {\n")
            g.write('  size="100,100";\n')
            for node in nodes:
                g.write('  %s [label="%s",font=4];\n' % (node["id"], node["name"]))
            for edge in links:
                g.write(
                    '  %s -> %s [label="%s",font=4,len=2];\n'
                    % (edge["source"], edge["target"], edge["type"])
                )
            g.write("}\n")


def infer_step_name(argv=sys.argv):
    """Given the command line args, infer the step name
    """
    if argv[0].endswith(".py"):
        return basename(argv[0])[:-3]
    elif "python" in argv[0] and argv[1].endswith(".py"):
        return basename(argv[1])[:-3]
    else:
        return basename(argv[0])


def infer_script_path(argv=sys.argv):
    """Given the command line args, infer the path to a script
    """

    def expand(p):
        return abspath(expanduser(p))

    if argv[0].endswith(".py") or argv[0].endswith(".ipynb"):
        return expand(argv[0])
    elif ("python" in argv[0]) or (realpath(argv[0]) == sys.executable):
        return expand(argv[1])
    else:
        return expand(argv[0])
