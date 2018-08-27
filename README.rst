===============
Data Workspaces
===============
Easy mangement of source data, intermediate data, and results for
data science projects.

To install for development
--------------------------
First, create a virtual environment. If you are using Anaconda3,
here are the steps::

    conda create --name dws

To activate the environment::

    source activate dws

Now, install the data workspaces library via pip::

    pip install --editable `pwd`


You can edit the files directly in your git repo -- the changes will
be reflected when you run the commands.

To run, just type ``dws SUBCOMMAND`` or ``dws --help``.
