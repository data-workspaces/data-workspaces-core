
from os.path import exists, join
import click
import json

from dataworkspaces.errors import InternalError, ConfigurationError
import dataworkspaces.commands.actions as actions
from dataworkspaces.resources.resource import get_resource_from_command_line,\
    suggest_resource_name, read_current_resources, get_resource_names



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
    def __init__(self, verbose, resource, resource_file_path, current_json):
        super().__init__(verbose)
        self.resource = resource
        self.resource_file_path = resource_file_path
        self.current_json = current_json
        for r in current_json:
            if r['url']==resource.url:
                raise ConfigurationError("Resource '%s' already in workspace" % resource.url)

    def run(self):
        self.current_json.append(self.resource.to_json())
        with open(self.resource_file_path, 'w') as f:
            json.dump(self.current_json, f, indent=2)

    def __str__(self):
        return "Add '%s' to resources.json file" % str(self.resource)
    
def add_command(scheme, role, name, workspace_dir, batch, verbose, *args):
    resource_json = read_current_resources(workspace_dir)
    current_names = get_resource_names(resource_json)
    if batch:
        if name==None:
            name = suggest_resource_name(scheme, role, current_names,
                                         **args)
        else:
            if name in current_names:
                raise ConfigurationError("Resource name '%s' already in use"%
                                         name)
    else:
        suggested_name = None
        while (name is None) or (name in current_names):
            if suggested_name==None:
                suggested_name = suggest_resource_name(scheme, role,
                                                       current_names,
                                                       *args)
            name = click.prompt("Please enter a short, unique name for this resource",
                                default=suggested_name)
            if name in current_names:
                click.echo("Resource name '%s' already in use." %
                           name, err=True)

    r = get_resource_from_command_line(scheme, role, name, workspace_dir,
                                       batch, verbose, *args)
    plan = []
    plan.append(AddResource(verbose, r))
    rfile = join(workspace_dir, '.dataworkspace/resources.json')
    plan.append(AddResourceToFile(verbose, r, rfile, resource_json))
    plan.append(actions.GitAdd(workspace_dir, [rfile], verbose))
    plan.append(actions.GitCommit(workspace_dir, 'Added resource %s'%str(r),
                                  verbose))
    actions.run_plan(plan, 'Add %s to workspace'%str(r), 'Added %s to workspace'%str(r), batch, verbose)
