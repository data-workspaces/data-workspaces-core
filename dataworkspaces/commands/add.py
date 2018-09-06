
from os.path import exists, join
import click
import json

from dataworkspaces.errors import InternalError, ConfigurationError
import dataworkspaces.commands.actions as actions
from dataworkspaces.resources.resource import get_resource_from_command_line

class AddResource(actions.Action):
    def __init__(self, verbose, resource):
        super().__init__(verbose)
        self.resource = resource
        self.resource.add_prechecks()

    def run(self):
        self.resource.add()

    def __str__(self):
        return "Run add actions for %s" % str(self.resource)


class AddResourceToFile(actions.Action):
    def __init__(self, verbose, resource, resource_file_path):
        super().__init__(verbose)
        self.resource = resource
        self.resource_file_path = resource_file_path
        if not exists(resource_file_path):
            raise InternalError("Missing resources file '%s'. Is something wrong in your environment?" %
                                resource_file_path)
        with open(resource_file_path, 'r') as f:
            data = json.load(f)
        if not isinstance(data, list):
            raise InternalError("Resources file '%s' has incorrect format" % resource_file_path)
        for r in data:
            if r['url']==resource.url:
                raise ConfigurationError("Resource '%s' already in workspace" % resource.url)

    def run(self):
        with open(self.resource_file_path, 'r') as f:
            data = json.load(f)
        data.append(self.resource.to_json())
        with open(self.resource_file_path, 'w') as f:
            json.dump(data, f, indent=2)

    def __str__(self):
        return "Add '%s' to resources.json file" % str(self.resource)
    
def add_command(scheme, role, workspace_dir, batch, verbose, *args):
    r = get_resource_from_command_line(scheme, role, workspace_dir,
                                       batch, verbose, *args)
    plan = []
    plan.append(AddResource(verbose, r))
    rfile = join(workspace_dir, '.dataworkspace/resources.json')
    plan.append(AddResourceToFile(verbose, r, rfile))
    plan.append(actions.GitAdd(workspace_dir, [rfile], verbose))
    plan.append(actions.GitCommit(workspace_dir, 'Added resource %s'%str(r),
                                  verbose))
    actions.run_plan(plan, 'Add %s to workspace'%str(r), 'Added %s to workspace'%str(r), batch, verbose)
