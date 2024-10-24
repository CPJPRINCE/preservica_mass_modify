"""
preservica_mass_modify package definitions

Author: Christopher Prince
license: Apache License 2.0"
"""

from .preservica_modify import PreservicaMassMod
from .cli import *
import importlib.metadata

__author__ = "Christopher Prince (c.pj.prince@gmail.com)"
__license__ = "Apache License Version 2.0"
__version__ = importlib.metadata.version("preservica_mass_modify")