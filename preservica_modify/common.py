"""
Common Library for Preservica Mass Modify

Author: Christopher Prince
license: Apache License 2.0"
"""

import pandas as pd
import logging, time
from typing import Literal, Optional, Any
import argparse

logger = logging.getLogger(__name__)

def _cell(df, idx: Any, column: str) -> Any:
    return df.at[idx, column]

def check_nan(value) -> Optional[Any]:
    if str(value).lower() in {"nan","nat"}:
        value = None
    return value

def check_bool(value) -> bool:
    if str(value).lower() in {"true","1","yes"}:
        return True
    elif check_nan(value) in {None,"","false","0","no","nan","nat"}:
        return False
    else: 
        return False

def export_csv(df: pd.DataFrame, output_filename: str, sep: str = ",", index: bool = False):
    try:
        df.to_csv(output_filename,index = index, sep = sep, encoding = "utf-8")
        logger.info(f"Saved to: {output_filename}")
    except ModuleNotFoundError:
        logger.warning('Pandas module not found, cannot export to csv. Please install via: pip install pandas')
        raise SystemExit()
    except PermissionError as e:
        logger.warning(f'File {e} failed to open; waiting 10 seconds to try again...')
        time.sleep(10)
        export_csv(df, output_filename, sep = sep, index = index)

def export_json(df: pd.DataFrame, output_filename: str, orient: Literal['index', 'records', 'split', 'columns', 'values','table', None]
 = 'index'):
    try:
        df.to_json(output_filename, orient=orient, indent=4)
        logger.info(f"Saved to: {output_filename}")
    except ModuleNotFoundError:
        logger.warning('Pandas Module not found, cannot export to json. Please install via: pip install pandas')
        raise SystemExit()
    except PermissionError as e:
        logger.warning(f'File {e} failed to open; waiting 10 seconds to try again...')
        time.sleep(10)
        export_json(df, output_filename, orient = orient)

def export_xml(df: pd.DataFrame, output_filename: str, index: bool = False):
    try:
        df.to_xml(output_filename, index = index)
        logger.info(f"Saved to: {output_filename}")
    except ModuleNotFoundError:
        logger.warning('lxml Module not found, cannot export to xml, please install via: pip install lxml')
        raise SystemExit()
    except PermissionError as e:
        logger.warning(f'File {e} failed to open; waiting 10 seconds to try again...')
        time.sleep(10)
        export_xml(df, output_filename, index = index)

def export_xl(df: pd.DataFrame, output_filename: str, index: bool = False):
    try:
        with pd.ExcelWriter(output_filename,mode = 'w') as writer:
            df.to_excel(writer, index = index)
        logger.info(f"Saved to: {output_filename}")
    except ModuleNotFoundError:
        logger.warning('openpyxl Module not found, cannot export to xlsx, please install via: pip install openpyxl')
        raise SystemExit()
    except PermissionError as e:
        logger.warning(f'File {e} failed to open; waiting 10 seconds to try again...')
        time.sleep(10)
        export_xl(df,output_filename, index = index)

def export_ods(df: pd.DataFrame, output_filename: str, index: bool = False):
    try:
        with pd.ExcelWriter(output_filename,engine='odf',mode = 'w') as writer:
            df.to_excel(writer, index = index)
        logger.info(f"Saved to: {output_filename}")
    except ModuleNotFoundError:
        logger.warning('odfpy Module not found, cannot export to ods, please install via: pip install odfpy')
        raise SystemExit()
    except PermissionError as e:
        logger.warning(f'File {e} failed to open; waiting 10 seconds to try again...')
        time.sleep(10)
        export_ods(df, output_filename, index = index)

def server_helper(server_str: str) -> str:
    if server_str.startswith("https://"):
        return server_str.replace("https://", "", 1)
    if server_str.startswith("http://"):
        return server_str.replace("http://", "", 1)
    return server_str

def fmthelper(x: str):
    x = x.lower()
    if x in ('xlsx', 'xlsm', 'xltx', 'xltm', 'xlsb', 'xls', 'excel', 'xl'):
        x = 'xlsx'
    if x in ('csv', 'txt', 'comma', 'comma_separated', 'c'):
        x = 'csv'
    if x in ('json', 'jsn', 'j'):
        x = 'json'
    if x in ('ods', 'open_document_spreadsheet', 'o'):
        x = 'ods'
    if x in ('xml', 'html', 'htm'):
        x = 'xml'
    if x in ('dict','dictionary', 'd'):
        x = 'dict'
    if x not in ('xlsx', 'csv', 'json', 'ods', 'xml', 'dict'):
        raise argparse.ArgumentTypeError(f"Invalid format specified: {x}. Valid options are Excel (xlsx), CSV (csv), JSON (json), ODS (ods), XML (xml), or Dictionary (dict).")
    return x.lower()

def metadata_helper(x: str):
    x = x.lower()
    if x in ('e', 'exact'):
        x = 'exact'
    if x in ('f', 'flat'):
        x = 'flat'
    if x not in ('exact', 'flat'):
        raise argparse.ArgumentTypeError(f"Invalid metadata handling method: {x}. Valid options are 'exact' or 'flat'.")
    return x.lower()