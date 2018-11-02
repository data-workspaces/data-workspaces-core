import os
from os.path import isabs, exists, expanduser, basename, dirname, isdir, join
import shutil
import json
import datetime

import click

import dataworkspaces.commands.actions as actions
from dataworkspaces.errors import ConfigurationError

EXECUTABLES_TO_EXCLUDE_FROM_STEP_NAME = ['python', 'python3', 'python2']

class RemovePreviousLineage(actions.Action):
    def __init__(self, ns, verbose, lineage_file):
        super().__init__(ns, verbose)
        self.lineage_file = lineage_file

    def run(self):
        if exists(self.lineage_file):
            os.remove(self.lineage_file)

    def __str__(self):
        return "Remove old lineage file '%s', if present" % self.lineage_file


class RunCommand(actions.Action):
    def __init__(self, ns, verbose, command_and_args, cwd):
        super().__init__(ns, verbose)
        self.command_and_args = command_and_args
        self.cwd = cwd

    def run(self):
        actions.call_subprocess(self.command_and_args, self.cwd, verbose=self.verbose)

    def __str__(self):
        return "Run %s from directory %s" % (self.command_and_args, self.cwd)


class WriteLineage(actions.Action):
    def __init__(self, ns, verbose, lineage_file, lineage_data):
        super().__init__(ns, verbose)
        self.lineage_file = lineage_file
        self.lineage_data = lineage_data

    def run(self):
        parent_dir = dirname(self.lineage_file)
        if not isdir(parent_dir):
            os.mkdir(parent_dir)
        with open(self.lineage_file, 'w') as f:
            json.dump(self.lineage_data, f, indent=2)

    def __str__(self):
        return "Write lineage data to %s" % self.lineage_file


def remove_extension(fname):
    try:
        return fname[:fname.rindex('.')]
    except ValueError:
        return fname


def run_command(workspace_dir, step_name, cwd, command, args, batch, verbose):
    ns = actions.Namespace()
    plan = [ ]
    # find the command executable
    if isabs(command):
        if not exists(command):
            raise ConfigurationError("Command executable '%s' does not exist" % command)
        if not os.access(command, os.X_OK):
            raise ConfigurationError("Command '%s' is not executable" % command)
        command_path = command
    else:
        command_path = shutil.which(command)
        if command_path is None:
            raise ConfigurationError("Could not find command '%s'" % command)
    command_and_args = [command_path] + list(args)

    cwd = expanduser(cwd)

    # figure out the step name
    if step_name is None:
        if basename(command_path) in EXECUTABLES_TO_EXCLUDE_FROM_STEP_NAME:
            step_name = remove_extension(basename(args[0]))
        else:
            step_name = remove_extension(basename(command_path))

    lineage_file = join(workspace_dir, '.dataworkspace/current_lineage/%s.json' % step_name)

    lineage_data = {
        'step_name':step_name,
        'command_path':command_path,
        'args': args,
        'cwd': cwd,
        'timestamp':datetime.datetime.now().isoformat()
    }
    plan.append(RemovePreviousLineage(ns, verbose, lineage_file))
    plan.append(RunCommand(ns, verbose, command_and_args, cwd))
    plan.append(WriteLineage(ns, verbose, lineage_file, lineage_data))
    try:
        actions.run_plan(plan, "Run command with lineage", "run command with lineage", batch=batch, verbose=verbose)
    except:
        # if we get an error, we need to wipe out the lineage file
        if exists(lineage_file):
            os.remove(lineage_file)
        raise

