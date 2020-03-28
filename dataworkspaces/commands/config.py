# Copyright 2018,2019 by MPI-SWS and Data-ken Research. Licensed under Apache 2.0. See LICENSE.txt.
import click
from typing import Optional, Dict, Any, List
from abc import ABCMeta, abstractmethod

from dataworkspaces.workspace import Workspace, Resource, LocalStateResourceMixin
from dataworkspaces.utils.param_utils import (
    PARAM_DEFS,
    LOCAL_PARAM_DEFS,
    ParamNotFoundError,
    ParamDef,
)
from dataworkspaces.utils.print_utils import print_columns, ColSpec
from dataworkspaces.errors import ConfigurationError


class ParamConfigHandler(metaclass=ABCMeta):
    def __init__(self, params: Dict[str, Any], defs: Dict[str, ParamDef]):
        self.params = params
        self.defs = defs

    def get_value(self, name: str) -> Any:
        if name in self.params:
            return self.params[name]
        else:
            return self.defs[name].default_value

    def is_default(self, name) -> bool:
        return (name not in self.params) or (self.params[name] == self.defs[name].default_value)

    @abstractmethod
    def get_scope(self) -> str:
        """Return 'global' or 'local'
        """
        pass

    @abstractmethod
    def get_what_for(self) -> str:
        """This should return either 'workspace' or
        'resource NAME'
        """
        pass

    @abstractmethod
    def set_value(self, name: str, value: Any) -> None:
        pass


class GlobalWorkspaceHandler(ParamConfigHandler):
    def __init__(self, workspace):
        super().__init__(workspace._get_global_params(), PARAM_DEFS)
        self.workspace = workspace

    def get_scope(self) -> str:
        return "global"

    def set_value(self, name: str, value: Any) -> None:
        self.workspace.set_global_param(name, value)

    def get_what_for(self) -> str:
        return "workspace"


class LocalWorkspaceHandler(ParamConfigHandler):
    def __init__(self, workspace):
        super().__init__(workspace._get_local_params(), LOCAL_PARAM_DEFS)
        self.workspace = workspace

    def get_scope(self) -> str:
        return "local"

    def set_value(self, name: str, value: Any) -> None:
        self.workspace.set_local_param(name, value)

    def get_what_for(self) -> str:
        return "workspace"


class GlobalResourceHandler(ParamConfigHandler):
    def __init__(self, resource: Resource, workspace: Workspace):
        super().__init__(resource.get_params(), resource.param_defs.global_defs)
        self.resource = resource
        self.workspace = workspace

    def get_scope(self) -> str:
        return "global"

    def set_value(self, name: str, value: Any) -> None:
        self.workspace._set_global_param_for_resource(self.resource.name, name, value)

    def get_what_for(self) -> str:
        return "resource '%s'" % self.resource.name


class LocalResourceHandler(ParamConfigHandler):
    def __init__(self, resource: Resource, workspace: Workspace):
        assert isinstance(resource, LocalStateResourceMixin)
        super().__init__(resource.get_local_params(), resource.param_defs.local_defs)
        self.resource = resource
        self.workspace = workspace

    def get_scope(self) -> str:
        return "local"

    def set_value(self, name: str, value: Any) -> None:
        self.workspace._set_local_param_for_resource(self.resource.name, name, value)

    def get_what_for(self) -> str:
        return "resource '%s'" % self.resource.name


def config_command(
    workspace: Workspace,
    param_name: Optional[str],
    param_value: Optional[str],
    resource: Optional[str],
):
    if param_name is None and param_value is None:
        names = []
        scopes = []
        values = []
        isdefaults = []
        helps = []
        if resource is None:
            handlers = [
                GlobalWorkspaceHandler(workspace),
                LocalWorkspaceHandler(workspace),
            ]  # type: List[ParamConfigHandler]
        else:
            if resource not in workspace.get_resource_names():
                raise ConfigurationError("No resource in this workspace with name '%s'" % resource)
            resource_obj = workspace.get_resource(resource)
            handlers = [GlobalResourceHandler(resource_obj, workspace)]
            if isinstance(resource_obj, LocalStateResourceMixin):
                handlers.append(LocalResourceHandler(resource_obj, workspace))
        for handler in handlers:
            for name in handler.defs.keys():
                names.append(name)
                scopes.append(handler.get_scope())
                helps.append(handler.defs[name].help)
                values.append(handler.get_value(name))
                isdefaults.append("Y" if handler.is_default(name) else "N")
        print_columns(
            {
                "Name": names,
                "Scope": scopes,
                "Value": values,
                "Default?": isdefaults,
                "Description": helps,
            },
            spec={"Description": ColSpec(width=40)},
            paginate=False,
        )
        click.echo()
    else:
        assert param_name is not None
        if resource is None:
            if param_name in PARAM_DEFS:
                handler = GlobalWorkspaceHandler(workspace)
            elif param_name in LOCAL_PARAM_DEFS:
                handler = LocalWorkspaceHandler(workspace)
            else:
                raise ParamNotFoundError("No workspace parameter named '%s'" % param_name)
        else:  # resource-specific
            if resource not in workspace.get_resource_names():
                raise ConfigurationError("No resource in this workspace with name '%s'" % resource)
            resource_obj = workspace.get_resource(resource)
            if isinstance(resource_obj, LocalStateResourceMixin) and (
                param_name in resource_obj.get_local_params()
            ):
                handler = LocalResourceHandler(resource_obj, workspace)
            elif param_name in resource_obj.get_params().keys():
                handler = GlobalResourceHandler(resource_obj, workspace)
            else:
                raise ParamNotFoundError(
                    "Resource %s has no parameter named '%s'" % (resource, param_name)
                )

        if param_value is None:
            # just print for the specific param
            title = "%s parameter '%s'" % (handler.get_what_for().capitalize(), param_name)
            click.echo(title)
            click.echo("=" * len(title))
            click.echo()
            print_columns(
                {
                    "Value": [handler.get_value(param_name)],
                    "Scope": [handler.get_scope()],
                    "Default?": ["Y" if handler.is_default(param_name) else "N"],
                    "Description": [handler.defs[param_name].help],
                },
                spec={"Description": ColSpec(width=60)},
                paginate=False,
            )
            click.echo()
        else:  # setting the parameter
            parsed_value = handler.defs[param_name].parse(param_value)
            handler.set_value(param_name, handler.defs[param_name].to_json(parsed_value))
            param_for = handler.get_what_for()
            workspace.save("Update of %s parameter %s" % (param_for, param_name))
            click.echo(
                "Successfully set %s %s parameter '%s' to %s."
                % (param_for, handler.get_scope(), param_name, repr(parsed_value))
            )
