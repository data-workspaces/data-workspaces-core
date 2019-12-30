# Copyright 2018,2019 by MPI-SWS and Data-ken Research. Licensed under Apache 2.0. See LICENSE.txt.
import click
from typing import Optional

from dataworkspaces.workspace import Workspace
from dataworkspaces.utils.param_utils import PARAM_DEFS, LOCAL_PARAM_DEFS,\
    ParamNotFoundError
from dataworkspaces.utils.print_utils import print_columns, ColSpec






def config_command(workspace:Workspace, param_name:Optional[str],
                   param_value:Optional[str]):
    if param_name is None and param_value is None:
        names = []
        scopes = []
        values = []
        isdefaults = []
        helps = []
        for local_or_global in ['local', 'global']:
            if local_or_global=='local':
                params = workspace._get_local_params()
                defs = LOCAL_PARAM_DEFS
            else:
                params = workspace._get_global_params()
                defs = PARAM_DEFS
            for (name, pdef) in defs.items():
                names.append(name)
                scopes.append(local_or_global)
                helps.append(pdef.help)
                if name in params:
                    values.append(params[name])
                    isdefaults.append('N')
                else:
                    values.append(pdef.default_value)
                    isdefaults.append('Y')
        print_columns({'Name':names,
                       'Scope':scopes,
                       'Value':values,
                       'Default?':isdefaults,
                       'Description':helps},
                      spec={'Description':ColSpec(width=40)},
                      paginate=False)
        click.echo()
    else:
        if param_name in PARAM_DEFS:
            local_or_global='global'
            params = workspace._get_global_params()
            defs = PARAM_DEFS
        elif param_name in LOCAL_PARAM_DEFS:
            local_or_global='local'
            params = workspace._get_local_params()
            defs = LOCAL_PARAM_DEFS
        else:
            raise ParamNotFoundError("No parameter named '%s'" % param_name)
        if param_value is None:
            # just print for the specific param
            title = "%s Parameter %s" % ('Local' if local_or_global=='local' else 'Global',
                                         param_name)
            click.echo(title)
            click.echo('='* len(title))
            click.echo()
            print_columns(
                {'Value':[params[param_name] if param_name in params
                          else defs[param_name].default_value],
                 'Scope':[local_or_global],
                 'Default?':['Y' if param_name not in params else 'N'],
                 'Description':[defs[param_name].help]},
                spec={'Description':ColSpec(width=60)},
                paginate=False)
            click.echo()
        else: # setting the parameter
            parsed_value = defs[param_name].parse(param_value)
            if local_or_global=='local':
                workspace.set_local_param(param_name, parsed_value)
            else:
                workspace.set_global_param(param_name, parsed_value)
            workspace.save('Update of parameter %s' % param_name)
            click.echo("Successfully set %s parameter %s to '%s'."%
                       (local_or_global, param_name, param_value))


