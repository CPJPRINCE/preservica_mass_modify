"""
preservica_mass_modify package definitions

Author: Christopher Prince
license: Apache License 2.0"
"""

from .pres_modify import PreservicaMassMod
from .cli import main, create_parser, run_cli
from .common import check_nan, check_bool
from .upload_mode import PreservicaUploadMode
from importlib import metadata

__author__ = "Christopher Prince (c.pj.prince@gmail.com)"
__license__ = "Apache License Version 2.0"

try:
    __version__ = metadata.version("preservica_mass_modify")
except metadata.PackageNotFoundError:
    __version__ = "0.0.0"