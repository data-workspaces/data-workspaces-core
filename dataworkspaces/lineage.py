"""
Utilities for tracking data lineage
"""
import os
from os.path import dirname, abspath, expanduser, exists, join, isdir, basename
import sys
import logging
from typing import List, Optional, Tuple, Iterable, Set, cast # noqa
from subprocess import run, PIPE
import json
from abc import ABC, abstractmethod
from collections import OrderedDict
import hashlib
import glob
import datetime
import platform
import numpy as np
import pandas as pd


logger = logging.getLogger(__name__)

from dataworkspaces.utils.git_utils import is_git_dirty
from dataworkspaces.errors import ConfigurationError


class GitStatusError(ConfigurationError):
    pass

class LineageFormatError(ConfigurationError):
    pass

class LineageMissingFileError(ConfigurationError):
    pass

class LineageInputFileChecksumError(ConfigurationError):
    pass


PROG=basename(sys.argv[0])

def describe_tags(allow_dirty=False):
    cwd=abspath(expanduser(dirname(__file__)))
    if (not allow_dirty) and is_git_dirty(cwd):
        raise GitStatusError("Git repository including %s is dirty!" % cwd)
    cmd=['/usr/bin/git', 'describe', '--tags']
    result = run(cmd, stdout=PIPE, cwd=cwd, encoding='utf-8')
    if result.returncode==0:
        return result.stdout.rstrip()
    else:
        return "Not running from a git repository"

class ProgInfo:
    def __init__(self, version, git_describe_tags, program, args,
                 run_timestamp, node, system, cpu_count):
        self.version = version
        self.git_describe_tags = git_describe_tags
        self.program = program
        self.args = args
        self.run_timestamp = run_timestamp
        self.node = node
        self.system = system
        self.cpu_count = cpu_count

    def __str__(self):
        s = "%s run at %s\n" % (self.program, self.run_timestamp)
        s += "Command line args: %s\n" % ' '.join(self.args)
        s += "VERSION: %s  Git describe tags: %s\n" % (self.version, self.git_describe_tags)
        s += "Run on %s, a %s system with %s cpus" % (self.node, self.system, self.cpu_count)
        return s

    def as_iter(self):
        """
        Returns as a sequence of individual lines without newlines. Useful
        if you want to dump to a logger or something similar.
        """
        return [
            "%s run at %s" % (self.program, self.run_timestamp),
            "Command line args: %s" % ' '.join(self.args),
            "VERSION: %s  Git describe tags: %s" % (self.version, self.git_describe_tags),
            "Run on %s, a %s system with %s cpus" % (self.node, self.system, self.cpu_count)
        ]

    def to_json(self):
        main = OrderedDict()
        main['version'] = self.version
        main['git_describe_tags'] = self.git_describe_tags
        main['program'] = self.program
        main['args'] = self.args
        main['run_timestamp'] = self.run_timestamp
        main['node'] = self.node
        main['system'] = self.system
        main['cpu_count'] = self.cpu_count
        return main

    @staticmethod
    def from_json(data):
        for key in ['version', 'git_describe_tags', 'program', 'args',
                    'run_timestamp', 'node', 'system', 'cpu_count']:
            if key not in data:
                raise Exception("ProgInfo missing required key %s" % key)
        return ProgInfo(data['version'],
                        data['git_describe_tags'],
                        data['program'],
                        data['args'],
                        data['run_timestamp'],
                        data['node'],
                        data['system'],
                        data['cpu_count'])

    def write_to_file(self, fpath):
        with open(fpath, 'w') as f:
            f.write("*"*60 + '\n')
            f.write(str(self) + '\n')
            f.write("*"*60 + '\n')


def get_program_info(version, allow_dirty_git_repo=False):
    return ProgInfo(version, describe_tags(allow_dirty=allow_dirty_git_repo), PROG,
                    ['"' + arg + '"' if ' ' in arg else arg for arg in sys.argv[1:]],
                    datetime.datetime.now().isoformat()[0:-10],
                    platform.node(),
                    platform.system(),
                    os.cpu_count())

# We file compute hashes ourselves rather than calling out to git, because it
# is about 5x faster than creating a subprocess. In the future, we can use
# something like pygit2 to call git as a library.
def compute_md5(filepath):
    m = hashlib.md5()
    with open(filepath, 'rb') as f:
        data = f.read(8192)
        while len(data)>0:
            m.update(data)
            data = f.read(8192)
    return m.hexdigest()


class Lineage(ABC):
    def __init__(self, step_name, step_type):
        self.step_name = step_name
        self.step_type = step_type

    def __repr__(self):
        return self.to_json()

    @abstractmethod
    def to_json(self):
        pass

    @staticmethod
    def from_json(data, filepath):
        if 'step_type' not in data:
            raise LineageFormatError("Missing required key 'step_type' in lineage file %s" % filepath)
        step_type = data['step_type']
        if step_type=='program':
            return ProgramLineage.from_json(data, filepath)
        elif step_type=='data_source':
            return DataSourceLineage.from_json(data, filepath)
        else:
            raise LineageFormatError("Invalid lineage step type '%s' in file %s" % (step_type, filepath))

    @staticmethod
    def read_from_file(filepath):
        with open(filepath, 'r') as f:
            data = json.load(f, object_pairs_hook=OrderedDict)
        return Lineage.from_json(data, filepath)


def json_fallback_serializer(obj):
    if isinstance(obj, (datetime.datetime, datetime.date)):
        return obj.isoformat()
    raise TypeError("Type %s not JSON serializable (value was %r)" % (type(obj), obj))

    
class DataSourceLineage(Lineage):
    def __init__(self, data_source_name, data_set_name, date_created, version, description):
        super().__init__(data_source_name, 'data_source')
        self.data_set_name = data_set_name
        self.date_created = date_created
        self.version = version
        self.description = description

    def to_json(self):
        main = OrderedDict()
        main['step_name'] = self.step_name
        main['step_type'] = self.step_type
        main['data_set_name'] = self.data_set_name
        main['date_created'] = self.date_created
        main['version'] = self.version
        main['description'] = self.description
        return main

    def write_to_file(self, data_dir):
        dep_file = join(abspath(expanduser(data_dir)), self.step_name + '.lineage.json')
        with open(dep_file, 'w') as f:
            json.dump(self.to_json(), f, indent=2, default=json_fallback_serializer)

    @staticmethod
    def from_json(data, filepath):
        for key in ['step_name', 'data_set_name', 'date_created', 'version', 'description']:
            if key not in data:
                raise LineageFormatError("Missing required key '%s' in lineage file %s" % (key, filepath))
        return DataSourceLineage(data['step_name'], data['data_set_name'],
                                 data['date_created'], data['version'],
                                 data['description'])

TIMESTAMP='timestamp' # name of timestamp col
GIT_TAGS='git_tags'

class _ResultsFileCols:
    def __init__(self, params:List[Tuple[str,str]], result_cols) -> None:
        self.params_by_step:OrderedDict = OrderedDict()
        self.result_cols = result_cols
        curr_step = None
        curr_list = []
        for (step, param) in params:
            if curr_step==None or curr_step==step:
                curr_step = step
                curr_list.append(param)
            else:
                self.params_by_step[curr_step] = curr_list
                curr_step = step
                curr_list = [param]
        if len(curr_list)>0:
            self.params_by_step[curr_step] = curr_list

    def get_added_cols(self, other:"_ResultsFileCols") -> List[str]:
        """Return a list of columns that were added to this version relative to other.
        """
        added_cols = []
        for step in self.params_by_step.keys():
            if step not in other.params_by_step:
                # entire step was added
                added_cols.extend([step + ':' + c for c in self.params_by_step[step]])
            else:
                old_cols = frozenset(other.params_by_step[step])
                for c in self.params_by_step[step]:
                    if c not in old_cols:
                        added_cols.append(step + ':' + c)
        old_results = frozenset(other.result_cols)
        for c in self.result_cols:
            if c not in old_results:
                added_cols.append(c)
        return added_cols

    def add_removed_cols(self, other:"_ResultsFileCols") -> List[str]:
        """For each step, find any parameters that were removed relative to other (which is the
        columns from the previous version). Add these to the beginning of each step. Do the
        same for the results.
        """
        removed_cols = []
        missing_steps = []
        for step in other.params_by_step.keys():
            if step not in self.params_by_step:
                # entire step was removed
                missing_steps.append(step)
                removed_cols.extend([step + ':' + c for c in other.params_by_step[step]])
            else:
                missing_cols = []
                new_cols = set(self.params_by_step[step])
                for c in other.params_by_step[step]:
                    if c not in new_cols:
                        missing_cols.append(c)
                        removed_cols.append(step+':'+c)
                if len(missing_cols)>0:
                    self.params_by_step[step] = missing_cols + self.params_by_step[step]
        missing_steps.reverse()
        # we put all the entirely missing steps at the beginning
        for step in missing_steps:
            self.params_by_step[step] = other.params_by_step[step]
            self.params_by_step.move_to_end(step, last=False)
        new_results = set(self.result_cols)
        missing_result_cols = []
        for c in other.result_cols:
            if c not in new_results:
                missing_result_cols.append(c)
                removed_cols.append(c)
        if len(missing_result_cols)>0:
            self.result_cols = missing_result_cols + self.result_cols
        return removed_cols

    def get_final_cols(self) -> List[str]:
        """Return the final column list, combining the timetamp, parameter cols,
         and the result cols.
        """
        param_cols = []
        for step in self.params_by_step:
            param_cols.extend([step + ':'+ c for c in self.params_by_step[step]])
        return [TIMESTAMP, GIT_TAGS] + param_cols + self.result_cols


def _update_results_csv(results_file:str, timestamp:str, git_tags:str,
                        param_generator:Iterable[Tuple[str,str,str]],
                        results:OrderedDict) -> None:
    if exists(results_file): # need to merge, columns could have been added or deleted
        # we merge the results putting any removed parameters at the start of the parameter columns
        # for a given step and any removed results at the start of the result columns.
        existing_df = pd.read_csv(results_file)
        if TIMESTAMP!=existing_df.columns[0]:
            raise Exception("Results file %s missing header column '%s'" % (results_file, TIMESTAMP))
        existing_df.set_index('timestamp', drop=False, inplace=True)
        existing_cols = _ResultsFileCols([(cname.split(':')[0], cname.split(':')[1]) for cname
                                         in existing_df.columns if ':' in cname],
                                         [cname for cname in existing_df.columns if
                                          (':' not in cname) and (cname not in (TIMESTAMP, GIT_TAGS))])

        new_row = {}
        new_row[TIMESTAMP] = timestamp
        new_row[GIT_TAGS] = git_tags
        new_params = []
        for (step, pname, pvalue) in param_generator:
            colname = step + ':' + pname
            new_row[colname] = pvalue
            new_params.append((step, pname),)
        new_result_cols = []
        for (rk, rv) in results.items():
            new_row[rk] = rv
            new_result_cols.append(rk)
        new_cols = _ResultsFileCols(new_params, new_result_cols)

        added_cols = new_cols.get_added_cols(existing_cols)
        for col in added_cols:
            existing_df[col] = np.nan # columns that weren't in the old version set to all NaN
        new_cols.add_removed_cols(existing_cols) # add back in any columns that were removed
        new_cols_in_order = new_cols.get_final_cols()
        existing_df = existing_df[new_cols_in_order[1:]] # re-order the old df

        new_row_df = pd.DataFrame([new_row], columns=new_cols_in_order)
        new_row_df.set_index(TIMESTAMP, drop=True, inplace=True)
        new_df = existing_df.append(new_row_df)
        os.rename(results_file, results_file + '.bak')
        new_df.to_csv(results_file)
    else: # new file, no merging necessary
        new_row = {}
        new_row[TIMESTAMP] = timestamp
        new_row[GIT_TAGS] = git_tags
        columns = [TIMESTAMP, GIT_TAGS]
        for (step, pname, pvalue) in param_generator:
            colname = step + ':' + pname
            new_row[colname] = pvalue
            columns.append(colname)
        for (rkey, rval) in results.items():
            new_row[rkey] = rval
            columns.append(rkey)
        new_row_df = pd.DataFrame([new_row], columns=columns)
        new_row_df.set_index(TIMESTAMP, inplace=True, drop=True)
        new_row_df.to_csv(results_file)

    

class ProgramLineage(Lineage):
    """The lineage subclass for programs that transform data.
    """
    def __init__(self, prog_info:ProgInfo, parameters:OrderedDict, dependencies:List[str],
                 data_dir:Optional[str]=None, dep_lineages:Optional[OrderedDict]=None,
                 results:Optional[OrderedDict]=None,
                 output_files:Optional[list]=None,
                 test_mode=False) -> None:
        super().__init__(prog_info.program.replace('.py', ''), 'program')
        self.prog_info = prog_info
        self.parameters = parameters
        self.dependencies = dependencies
        self.results = results
        self.output_files = output_files
        self.test_mode = test_mode
        # the dependency lineages can either be provided directly (when called from
        # from_json()) or can be read from the data directory (when created by a program)
        if data_dir is not None:
            self.dep_lineages:OrderedDict = OrderedDict()
            data_dirpath = abspath(expanduser(data_dir))
            if not exists(data_dirpath):
                raise LineageMissingFileError("Cannot find data directory %s" % data_dirpath)
            self.data_dirpath:Optional[str] = data_dirpath
            for dep in self.dependencies:
                dep_file = join(data_dirpath, dep + '.lineage.json')
                if not exists(dep_file):
                    if self.test_mode:
                        continue
                    else:
                        raise LineageMissingFileError("Did not find lineage file %s for %s" %
                                                      (dep_file, dep))
                self.dep_lineages[dep] = Lineage.read_from_file(dep_file)
        elif dep_lineages is not None:
            self.dep_lineages = dep_lineages
            self.data_dirpath:Optional[str] = None
        else:
            assert 0, "Either data_dir or dep_lineages must be specified"

    def add_result(self, k, v):
        if self.results is None:
            self.results = OrderedDict()
        self.results[k] = v
            
    def add_results(self, results:OrderedDict):
        if self.results is None:
            self.results = results
        else:
            for (k, v) in results.items():
                self.results[k] = v

    def set_output_files(self, output_files_list:List[str]) -> None:
        """Specify the output files. These are relative to the data
        directory and can be globs (e.g. *.csv)
        """
        assert self.data_dirpath is not None
        if self.test_mode:
            return
        self.output_files:list = []
        for fname in output_files_list:
            fpath = join(self.data_dirpath, fname)
            globbed_files = glob.glob(fpath)
            if len(globbed_files)==0:
                raise LineageMissingFileError("No matches for output file/directory '%s'" % fpath)
            for globbed_file in globbed_files:
                f = globbed_file[len(self.data_dirpath)+1:]
                if isdir(globbed_file):
                    self.output_files.append(f)
                else:
                    self.output_files.append([f, compute_md5(globbed_file)])

    def validate_input_files(self, input_file_list:Optional[List[str]]=None) -> None:
        """Validate the input files referenced in the lineage. Call this before running.
        If you are not necessary using all the files produced by previous steps, enumerate
        the files you need via input_file_list. These entries can be specific files or globs.
        """
        assert self.data_dirpath is not None
        if self.test_mode:
            return # no validation in test mode
        if input_file_list is not None:
            def expand_file(f):
                if not f.startswith('/'):
                    # relative paths are relative to data_dirpath
                    return glob.glob(join(self.data_dirpath, f))
                else:
                    return glob.glob(f)
            globbed_files_lists = [expand_file(f) for f in input_file_list]
            checked_files:Optional[Set[str]] = set([f for sublist in globbed_files_lists for f in sublist])
        else:
            checked_files = None
        for (dep, lineage) in self.dep_lineages.items():
            if lineage.step_type is not 'program':
                continue
            for input_entry in lineage.output_files:
                if isinstance(input_entry, list):
                    fname = input_entry[0]
                    cksum = input_entry[1]
                    fpath = join(self.data_dirpath, fname)
                    if (checked_files is not None) and (fpath not in checked_files):
                        continue
                    elif not exists(fpath):
                        raise LineageMissingFileError("Missing input file %s" % fpath)
                    md5 = compute_md5(fpath)
                    if md5!=cksum:
                        raise LineageInputFileChecksumError(
                            "Input file %s has checksum %s but dependency %s had a checksum of %s"
                            %(fpath, md5, dep, cksum))
                    if checked_files is not None:
                        checked_files.remove(fpath)
                    print("Validated input file %s against lineage" % fpath)
                else: # just a directory
                    fpath = join(self.data_dirpath, input_entry)
                    if (checked_files is not None) and (fpath not in checked_files):
                        continue
                    if not isdir(fpath):
                        raise LineageMissingFileError("Input directory %s missing" % fpath)
                    if checked_files is not None:
                        checked_files.remove(fpath)
                    print("Validated input directory %s against lineage" % fpath)
        assert (checked_files is None) or (len(checked_files)==0), \
            "Files that were not in lineage: %s" % ', '.join(cast(List[str], checked_files))


    def add_to_results_csv(self, filename):
        """Add the parmameters and results to a csv file.
        """
        assert self.data_dirpath is not None
        if self.results is None:
            raise Exception("No results to add")
        _update_results_csv(join(self.data_dirpath, filename),
                            self.prog_info.run_timestamp,
                            self.prog_info.git_describe_tags,
                            self.iterate_over_parameter_values(),
                            self.results)


    def to_json(self):
        main = OrderedDict()
        main['step_name'] = self.step_name
        main['step_type'] = self.step_type
        main['prog_info'] = self.prog_info.to_json()
        main['parameters'] = self.parameters
        main['dependencies'] = self.dependencies
        dl = OrderedDict()
        main['dep_lineages'] = dl
        for dep in self.dependencies:
            if dep in self.dep_lineages:
                dl[dep] = self.dep_lineages[dep].to_json()
            else:
                dl[dep] = None
        main['output_files'] = self.output_files
        main['results'] = self.results
        return main

    def save_to_file(self):
        assert self.data_dirpath is not None, "Did not specify data dir when creating lineage object"
        if self.test_mode:
            return
        dep_file = join(self.data_dirpath, self.step_name + '.lineage.json')
        with open(dep_file, 'w') as f:
            json.dump(self.to_json(), f, indent=2, default=json_fallback_serializer)

    def iterate_over_parameter_values(self) -> Iterable[Tuple[str,str,str]]:
        """
        Generator that iterates over all the parameters in dependencies and the
        current program, returning a (step_name, parameter, value) triple for
        each parameter. This is useful for logging the parameters, etc.
        """
        for lineage in ([l for l in self.dep_lineages.values()] + [self]):
            if isinstance(lineage, ProgramLineage) and (lineage.parameters is not None):
                for (k, v) in lineage.parameters.items():
                    yield (lineage.step_name, k, v)
            
    @staticmethod
    def from_json(data, filepath):
        for key in ['step_name', 'prog_info', 'parameters', 'dependencies', 'results',
                    'dep_lineages', 'output_files']:
            if key not in data:
                raise LineageFormatError("Missing required key '%s' in lineage file %s" % (key, filepath))
        dep_lineages = OrderedDict()
        for dep in data['dependencies']:
            dep_lineages[dep] = Lineage.from_json(data['dep_lineages'][dep], filepath) \
                                if data['dep_lineages'][dep] is not None else None
        return ProgramLineage(prog_info=ProgInfo.from_json(data['prog_info']),
                              parameters=data['parameters'],
                              dependencies=data['dependencies'],
                              results=data['results'],
                              dep_lineages=dep_lineages,
                              output_files=data['output_files'])

    @staticmethod
    def make_lineage(data_dir:str, parameters:Optional[OrderedDict]=None,
                     dependencies:Optional[List[str]]=None,
                     test_mode=False) -> Lineage:
        if parameters is None:
            parameters = OrderedDict()
        if dependencies is None:
            dependencies=[]
        return ProgramLineage(get_program_info(allow_dirty_git_repo=test_mode),
                              parameters=parameters, dependencies=dependencies,
                              data_dir=data_dir, test_mode=test_mode)


##########################################################################
#        Classes for defining program parameters
##########################################################################

class LineageParameter(ABC):
    def __init__(self, name, default):
        self.name = name
        self.default = default

    @abstractmethod
    def get_value(self, parsed_args):
        pass


class CmdLineParameter(LineageParameter):
    def __init__(self, name, default, type, help):
        super().__init__(name, default)
        self.type = type
        self.help = help

    def get_arg_name(self):
        return '--' + self.name.replace('_', '-')

    def add_to_arg_parser(self, arg_parser):
        arg_parser.add_argument(self.get_arg_name(), type=self.type, default=self.default,
                                help=self.help)

    def get_value(self, parsed_args):
        return getattr(parsed_args, self.name)


class BooleanParameter(CmdLineParameter):
    def __init__(self, name, default, help):
        super().__init__(name, default, bool, help)
        if self.default:
            self.action='store_false'
        else:
            self.action='store_true'

    def get_arg_name(self):
        if self.default:
            return '--no-' + self.name.replace('_', '-')
        else:
            return '--' + self.name.replace('_', '-')

    def add_to_arg_parser(self, arg_parser):
        arg_parser.add_argument(self.get_arg_name(), default=self.default,
                                action=self.action,
                                help=self.help, dest=self.name)


class ChoiceParameter(CmdLineParameter):
    def __init__(self, name, choices, default, type, help):
        super().__init__(name, default, type, help)
        self.choices = choices
        assert default in choices

    def add_to_arg_parser(self, arg_parser):
        arg_parser.add_argument(self.get_arg_name(), type=self.type, default=self.default,
                                choices=self.choices,
                                help=self.help)


class ConstantParameter(LineageParameter):
    def get_value(self, parsed_args):
        return self.default


def add_lineage_parameters_to_arg_parser(parser, params):
    for param in params:
        param.add_to_arg_parser(parser)


def get_lineage_parameter_values(params, parsed_args):
    values = OrderedDict()
    for param in params:
        values[param.name] = param.get_value(parsed_args)
    return values
