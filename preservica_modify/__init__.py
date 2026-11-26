"""
Preservica Mass Modify package definitions

Author: Christopher Prince
license: Apache License 2.0"
"""

from preservica_modify.pres_modify import PreservicaMassMod
# from .pres_upload import PreservicaMassUpload --- IGNORE ---
from preservica_modify.cli import main, create_parser, run_cli
from preservica_modify.common import check_nan, check_bool, export_csv, export_json, export_xml, export_xl, export_ods
from importlib import metadata

__author__ = "Christopher Prince (c.pj.prince@gmail.com)"
__license__ = "Apache License Version 2.0"

try:
    __version__ = metadata.version("preservica_mass_modify")
except metadata.PackageNotFoundError:
    __version__ = "0.0.0"