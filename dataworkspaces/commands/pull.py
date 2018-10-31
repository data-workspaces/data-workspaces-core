
import click

import dataworkspaces.commands.actions as actions
from dataworkspaces.resources.resource import CurrentResources
from .push import get_resources_to_process
from dataworkspaces.resources.git_resource import is_git_dirty


class PullResource(actions.Action):
    def __init__(self, ns, verbose, r):
        super().__init__(ns, verbose)
        self.r = r
        r.pull_prechecks()

    def run(self):
        click.echo("Pulling resource %s..." % self.r.name)
        self.r.pull()

    def __str__(self):
        return "Pull state of resource '%s' to origin" % self.r.name


class PullWorkspace(actions.Action):
    def __init__(self, ns, verbose, workspace_dir):
        super().__init__(ns, verbose)
        self.workspace_dir = workspace_dir
        if is_git_dirty(workspace_dir):
            raise ConfigurationError("Data workspace metadata repo at %s has uncommitted changes. Please commit before pulling." %
                                     workspace_dir)

    def run(self):
        click.echo("Pulling workspace...")
        actions.call_subprocess([actions.GIT_EXE_PATH, 'pull', 'origin', 'master'],
                                cwd=self.workspace_dir, verbose=self.verbose)

    def __str__(self):
        return "Pull state of data workspace metadata to origin"


def pull_command(workspace_dir, batch=False, verbose=False,
                 only=None, skip=None, only_workspace=False):
    plan = []
    ns = actions.Namespace()
    if not only_workspace:
        current_resources = CurrentResources.read_current_resources(workspace_dir,
                                                                    batch, verbose)
        for name in get_resources_to_process(current_resources, only, skip):
            r = current_resources.by_name[name]
            plan.append(PullResource(ns, verbose, r))

    plan.append(PullWorkspace(ns, verbose, workspace_dir))
    actions.run_plan(plan, "pull state from origins",
                     "pulled state from origins", batch=batch, verbose=verbose)




