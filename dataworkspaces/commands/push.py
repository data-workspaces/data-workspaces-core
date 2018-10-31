
import click

from dataworkspaces.errors import ConfigurationError
import dataworkspaces.commands.actions as actions
from dataworkspaces.resources.resource import CurrentResources
from dataworkspaces.resources.git_resource import \
    is_git_dirty, is_pull_needed_from_remote

class PushResource(actions.Action):
    def __init__(self, ns, verbose, r):
        super().__init__(ns, verbose)
        self.r = r
        r.push_prechecks()

    def run(self):
        click.echo("Pushing resource %s..." % self.r.name)
        self.r.push()

    def __str__(self):
        return "Push state of resource '%s' to origin" % self.r.name


class PushWorkspace(actions.Action):
    def __init__(self, ns, verbose, workspace_dir):
        super().__init__(ns, verbose)
        self.workspace_dir = workspace_dir
        if is_git_dirty(workspace_dir):
            raise ConfigurationError("Data workspace metadata repo at %s has uncommitted changes. Please commit before pushing." %
                                     workspace_dir)
        if is_pull_needed_from_remote(workspace_dir, self.verbose):
            raise ConfigurationError("Data workspace at %s requires a pull from remote origin"%
                                     workspace_dir)

    def run(self):
        click.echo("Pushing workspace...")
        actions.call_subprocess([actions.GIT_EXE_PATH, 'push', 'origin', 'master'],
                                cwd=self.workspace_dir, verbose=self.verbose)

    def __str__(self):
        return "Push state of data workspace metadata to origin"


def push_command(workspace_dir, batch=False, verbose=False,
                 only=None, skip=None, only_workspace=False):
    plan = []
    ns = actions.Namespace()
    current_resources = CurrentResources.read_current_resources(workspace_dir,
                                                                batch, verbose)
    for r in current_resources.resources:
        plan.append(PushResource(ns, verbose, r))
    plan.append(PushWorkspace(ns, verbose, workspace_dir))
    actions.run_plan(plan, "push state to origins",
                     "pushed state to origins", batch=batch, verbose=verbose)



