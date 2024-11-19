"""
preservica_mass_modify package definitions

Author: Christopher Prince
license: Apache License 2.0"
"""

from preservica_modify.pres_modify import PreservicaMassMod
from preservica_modify.cli import *
from preservica_modify.common import *
import importlib.metadata

__author__ = "Christopher Prince (c.pj.prince@gmail.com)"
__license__ = "Apache License Version 2.0"
__version__ = importlib.metadata.version("preservica_mass_modify")