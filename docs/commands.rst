.. _commands:

3. Command Reference
====================
In this section, we describe the full command line interface for Data Workspaces.
This interface is built around a script ``dws``, which is installed into your
path when you install the ``dataworkspaces`` package. The overall interface
for ``dws`` is::

  dws [--batch] [--verbose] [--help] COMMAND [--help] [OPTIONS] [ARGS]...

``dws`` has three options common to all commands:

* ``--batch``, which runs the command in a mode that never asks for user confirmation and
  will error out if it absolutely requires an input (useful for automation),
* ``--verbose``, which will print a lot of detail about what will be and has been done for a command
  (useful for debugging), and
* ``--help``, which prints these common options and a list of available commands.

Next on your command line comes the command name (e.g. ``init``, ``clone``, ``snapshot``).
Each command has its own arguments and options, as documented below.
All commands take a ``--help`` argument, which will print the specific options and
arguments for the command. Finally,
the ``add`` subcommand has further subcommands, representing the
individual resource types (e.g. `git`, `local-files`, `rclone`).

.. click:: dataworkspaces.dws:cli
   :prog: dws
   :show-nested:
