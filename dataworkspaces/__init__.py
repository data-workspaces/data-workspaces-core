# Copyright 2018,2019 by MPI-SWS and Data-ken Research. Licensed under Apache 2.0. See LICENSE.txt.
"""
Welcome to Data Workspaces. To get started, see the documentation at
https://data-workspaces-core.readthedocs.io/en/latest/. For most applications,
you will interact through the command line interface (dws), the integrations
with common libraries like Scikit-learn (the modules dataworkspaces.kits.*), or the
lineage API (the module dataworkspaces.lineage).

Some other submodules (mostly for extending dataworkspaces):

* backends  - backends for storing workspace metadata (currently only git)
* commands  - implementations of the command line commands
* dws       - command line interface and option parsing
* errors    - exception classes
* resoures  - implementations of the different resource types
* utils     - common utilities
* workspace - core APIs for dataworkspaces internals, including Workspace,
              Resource, and the various mixins that represent different
              capabilities.
"""

__version__ = "1.4.0alpha"

from . import api
from . import backends
from . import commands
from . import dws
from . import errors
from . import kits
from . import lineage
from . import resources
from . import utils
from . import workspace
