
import click

from dataworkspaces.errors import ConfigurationError
import dataworkspaces.commands.actions as actions
from dataworkspaces.resources.resource import get_resource_from_command_line,\
    suggest_resource_name, CurrentResources



class AddResource(actions.Action):
    def __init__(self, ns, verbose, resource):
        super().__init__(ns, verbose)
        self.resource = resource
        self.resource.add_prechecks()

    def run(self):
        self.resource.add()

    def __str__(self):
        return "Run add actions for %s" % str(self.resource)


class AddResourceToFile(actions.Action):
    def __init__(self, ns, verbose, resource, current_resources):
        super().__init__(ns, verbose)
        self.resource = resource
        self.current_resources = current_resources
        # A given resource should resolve to a unique name, so this is the best way
        # to check for duplication.
        if current_resources.is_a_current_name(resource.name):
            raise ConfigurationError("Resource '%s' already in workspace" % resource.name)

    def run(self):
        self.current_resources.add_resource(self.resource)
        self.current_resources.write_current_resources()

    def __str__(self):
        return "Add '%s' to resources.json file" % str(self.resource)


def add_command(scheme, role, name, workspace_dir, batch, verbose, *args):
    current_resources = CurrentResources.read_current_resources(workspace_dir, batch, verbose)
    current_names = current_resources.get_names()
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

    ns = actions.Namespace()
    r = get_resource_from_command_line(scheme, role, name, workspace_dir,
                                       batch, verbose, *args)
    plan = []
    plan.append(AddResource(ns, verbose, r))
    plan.append(AddResourceToFile(ns, verbose, r, current_resources))
    plan.append(actions.GitAdd(ns, verbose,
                               workspace_dir, [current_resources.json_file]))
    plan.append(actions.GitCommit(ns, verbose,
                                  workspace_dir, 'Added resource %s'%str(r)))
    actions.run_plan(plan, 'Add %s to workspace'%str(r), 'Added %s to workspace'%str(r), batch, verbose)
