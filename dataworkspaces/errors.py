# Copyright 2018,2019 by MPI-SWS and Data-ken Research. Licensed under Apache 2.0. See LICENSE.txt.
"""
Error class defintions
"""

from subprocess import CalledProcessError
class ConfigurationError(Exception):
    """Thrown when something is wrong with the system environment.
    """
    pass

class LineageError(ConfigurationError):
    """An error related to data lineage (e.g. action would make it impossible
    to track lineage)
    """
    pass

class UserAbort(Exception):
    """Thrown when the user requests not to perform the action.
    """
    pass

class InternalError(Exception):
    """Thrown when something unexpected happens.
    """
    pass

class BatchModeError(Exception):
    """Thrown when running in batch mode but user input is required"""
    pass

class ApiParamError(Exception):
    """Incorrect value for an API parameter"""
    pass
