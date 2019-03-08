# Copyright 2018,2019 by MPI-SWS and Data-ken Research. Licensed under Apache 2.0. See LICENSE.txt.

import sys
import traceback

import click

from .errors import CalledProcessError, UserAbort, ConfigurationError,\
                              InternalError, BatchModeError

from .dws import cli, is_verbose_mode

def main():
    try:
        cli()
        sys.exit(0)
    except CalledProcessError as e:
        click.echo(e.stdout)
        click.echo(e.stderr)
        tb = traceback.format_exc()
        click.echo(tb, err=True)
        click.echo("Error in subprocess call. Command was:\n   %s" % e.cmd, err=True)
        sys.exit(1)
    except UserAbort as e:
        click.echo("Not a positive response, exiting without doing anything", err=True)
        sys.exit(1)
    except ConfigurationError as e:
        if is_verbose_mode():
            tb = traceback.format_exc()
            click.echo(tb, err=True)
        click.echo("Configuration error: " + str(e), err=True)
        sys.exit(1)
    except BatchModeError as e:
        if is_verbose_mode():
            tb = traceback.format_exc()
            click.echo(tb, err=True)
        click.echo("Running in --batch mode, but user input required for %s"%
                   str(e), err=True)
        sys.exit(1)
    except InternalError as e:
        tb = traceback.format_exc()
        click.echo(tb, err=True)
        click.echo("Aborting due to internal error: %s" % str(e), err=True)
        sys.exit(1)
    except Exception as e:
        tb = traceback.format_exc()
        click.echo(tb, err=True)
        click.echo("Aborting due to unexpected exception: %s" % repr(e), err=True)
        sys.exit(1)

