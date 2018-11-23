"""
Definition of configuration parameters
"""

from dataworkspaces.resources.results_utils import validate_template
from dataworkspaces.errors import ConfigurationError

PARAM_DEFS = {}

class ParamDef:
    def __init__(self, name, default_value, help, validation_fn=None):
        self.name = name
        self.default_value = default_value
        self.validation_fn = validation_fn
        if validation_fn:
            validation_fn(default_value)

    def validate(self, value):
        if self.validation_fn:
            try:
                self.validation_fn(value)
            except Exception as e:
                raise ConfigurationError("Error in validation of configuration parameter '%s'"%
                                         self.name) from e

def define_param(name, default_value, help, validation_fn=None):
    global PARAM_DEFS
    PARAM_DEFS[name] = ParamDef(name, default_value, help, validation_fn)
    return name

RESULTS_DIR_TEMPLATE=define_param(
    'results.dir_template',
    "snapshots/{YEAR}-{MONTH}/{SHORT_MONTH}-{DAY}-{HOUR}:{MIN}:{SEC}-{TAG}",
    "Template describing where results files will be moved during snapshot",
    validate_template
)

def validate_move_exclude_files(value):
    if not isinstance(value, list) and not isinstance(value, tuple):
        raise ConfigurationError("Move excluded files parameter must be a list or a tuple")
    for v in value:
        if not isinstance(v, str):
            raise ConfigurationError("Move excluded files parameter elements must be strings")

RESULTS_MOVE_EXCLUDE_FILES=define_param(
    'results.move_exclude_files',
    ['README.txt', 'README.rst', 'README.md'],
    "List of files to exclude when moving results to a subdirectory during snapshot.",
    validate_move_exclude_files
)


def get_config_param_value(config_data, param_name):
    assert param_name in PARAM_DEFS
    if 'global_params' not in config_data:
        return PARAM_DEFS[param_name].default_value
    params = config_data['global_params']
    return params.get(param_name, PARAM_DEFS[param_name].default_value)

def get_all_defaults():
    """Return a mapping of all default values for use in generating
    the initial config file.
    """
    return {param:PARAM_DEFS[param].default_value for param in PARAM_DEFS.keys()}
