"""
Common Library for Preservica Mass Modify

Author: Christopher Prince
license: Apache License 2.0"
"""

from lxml import etree
from pyPreservica import EntityAPI, RetentionAPI, UploadAPI, WorkflowAPI, AdminAPI
import pandas as pd
import logging, time
from typing import Literal, Optional, Any, Union
import os
from getpass import getpass

try:
    import keyring
except Exception:
    keyring = None
    class KeyringError(Exception):
        pass
logger = logging.getLogger(__name__)

class AuthenticatedPreservicaClient:
    def __init__(self, credentials: Optional[str] = None, use_keyring: bool = False, keyring_service: str = "preservica_modify", save_password_to_keyring: bool = False, username: Optional[str] = None, password: Optional[str] = None, manager_username: Optional[str] = None, manager_password: Optional[str] = None, server: Optional[str] = None, tenant: Optional[str] = None):
        if credentials is not None:
            if os.path.isfile(credentials):
                self.credentials_file = credentials
            else: 
                self.credentials_file = None
        else:
            self.credentials_file = None

        self.username = username
        self.password = password
        self.server = server
        self.tenant = tenant
        self.manager_username = manager_username
        self.manager_password = manager_password

        self.use_keyring = use_keyring
        self.keyring_service = keyring_service
        self.save_password_to_keyring = save_password_to_keyring

    def _keyring_entry_name(self) -> str:
        tenant = self.tenant or "default"
        server = self.server or "default-server"
        return f"{self.keyring_service}:{server}:{tenant}"

    def _get_password_from_keyring(self, username) -> Optional[str]:
        if not self.use_keyring:
            return None
        if keyring is None:
            raise RuntimeError("keyring package is not installed. Install with: pip install keyring")
        if not username or not self.server:
            return None
        try:
            return keyring.get_password(self._keyring_entry_name(), str(username))
        except KeyringError as e:
            logger.warning(f"Unable to read password from keyring: {e}")
            return None
        
    def _set_password_in_keyring(self, username: str, password: str) -> None:
        if not self.save_password_to_keyring:
            return
        if keyring is None:
            logger.error("keyring package is not installed. Install with: pip install keyring")
            raise RuntimeError("keyring package is not installed. Install with: pip install keyring")
        if not self.username or not self.server:
            return
        try:
            keyring.set_password(self._keyring_entry_name(), str(username), password)
            logger.info("Password saved to keyring.")
        except KeyringError as e:
            logger.warning(f"Unable to save password to keyring: {e}")

    def login_preservica(self):
        """
        Logs into Preservica. Either through manually logging in.
        """
        try:
            if self.credentials_file:
                logger.info('Using credentials file.')
                self.entity = EntityAPI(credentials_path=self.credentials_file)
                self.retention = RetentionAPI(credentials_path=self.credentials_file)
                self.upload = UploadAPI(credentials_path=self.credentials_file)
                self.workflow = WorkflowAPI(credentials_path=self.credentials_file)
                self.admin = AdminAPI(credentials_path=self.credentials_file)
                logger.info(f'Successfully logged into Preservica Server {self.server}, as user: {self.username}')
                return
                        
            def _check_password(username: str, password: Optional[str]):

                if None in (username, self.server):
                    logger.error('A Username or Server has not been provided... Please try again...')
                    raise ValueError('A Username or Server has not been provided... Please try again...')

                if password is None and self.save_password_to_keyring is False:
                    password = self._get_password_from_keyring(username)
                
                if password is None:
                    password = getpass(prompt=f"Please enter your password for Preservica for {username}: ")
                    if self.save_password_to_keyring is True:
                        self._set_password_in_keyring(username, password)
                
                if password is not None:
                    return password
                else:
                    logger.error('Password not provided and could not be retrieved from keyring. Please try again...')
                    raise ValueError('Password not provided and could not be retrieved from keyring. Please try again...')
            
            if self.username:
                self.password = _check_password(self.username, self.password)

            if self.manager_username:
                self.manager_password = _check_password(self.manager_username, self.manager_password)

            self.entity = EntityAPI(username=str(self.username), password=str(self.password),server=str(self.server), tenant=str(self.tenant) if self.tenant else None)
            self.retention = RetentionAPI(username=str(self.username), password=str(self.password),server=str(self.server), tenant=str(self.tenant) if self.tenant else None)
            self.upload = UploadAPI(username=str(self.username), password=str(self.password),server=str(self.server), tenant=str(self.tenant) if self.tenant else None)
            self.workflow = WorkflowAPI(username=str(self.username), password=str(self.password),server=str(self.server), tenant=str(self.tenant) if self.tenant else None)
            self.admin = AdminAPI(username=str(self.username), password=str(self.password),server=str(self.server), tenant=str(self.tenant) if self.tenant else None)
            logger.info(f'Successfully logged into Preservica Server {self.server}, as user {self.username}')
        except Exception:
            logger.exception('Failed to login to Preservica')
            raise

    def test_login(self):
        """
        Test Login function, to ensure credentials are correct before running main.
        """
        try:
            self.login_preservica()
            logger.info(f'Login successful: {self.server} as {self.username}')
        except Exception:
            logger.exception('Login failed')
            raise

class ContinueTokenManager:
    def __init__(self, input_file: str, disable_continue: bool = False):
        self.input_file = input_file
        self.disable_continue = disable_continue

    def _save_continue_token(self, token_file: str, token: Optional[int]) -> None:
        try:
            token_file = token_file + "_continue.txt"
            if token is not None:
                with open(token_file, 'w') as f:
                    f.write(str(token))
                logger.info(f'Continue token saved to {token_file}')
        except FileNotFoundError:
            logger.exception(f'Continue token file not found: {token_file}, unable to save continue token...')
            raise
        except PermissionError as e:
            logger.exception(f'Permission denied when saving continue token to file {token_file}: {e}')
            raise
        except Exception as e:
            logger.exception(f'Failed to save continue token to file {token_file}: {e}')
            raise

    def _load_continue_token(self, token_file: str) -> Optional[int]:
        try:
            token_file = token_file + "_continue.txt"
            if os.path.isfile(token_file):
                with open(token_file, 'r') as f:
                    token_str = f.read().strip()
                    token = int(token_str)
                    if isinstance(token, int):
                        logger.info(f'Continue token loaded from {token_file}, processing from index: {token}')
                        return token
                    else:
                        logger.warning(f'Invalid continue token value in {token_file}: {token_str}')
                        raise ValueError(f'Invalid continue token value in {token_file}: {token_str}')
            else:
                logger.debug(f'No continue token file found at {token_file}, starting from beginning...')
                return 0
        except PermissionError:
            logger.warning(f'Permission denied when loading token {token_file}, starting from beggining')
            return 0
        except Exception:
            logger.warning(f'Invalid continue token value in {token_file}, starting from beginning...')
            return 0
                        
    def _remove_continue_token(self, token_file: str) -> None:
        token_file = token_file + "_continue.txt"
        if os.path.isfile(token_file):
            os.remove(token_file)
            logger.info(f'Continue token file {token_file} removed.')

    def process_continue_token(self, data_dict: dict) -> tuple[list[int], int]:
        if self.disable_continue is True:
            start_idx = 0
        else:
            start_idx = self._load_continue_token(self.input_file)
            if not isinstance(start_idx, int):
                logger.error(f'Invalid continue token: {start_idx}, must be an integer index. Please ensure the continue token file contains a valid integer index.')
                raise ValueError(f'Invalid continue token: {start_idx}, must be an integer index. Please ensure the continue token file contains a valid integer index.')
            keys = list(data_dict.keys())
            if start_idx in keys:
                start_pos = keys.index(start_idx)
            else:
                start_pos = max(0, int(start_idx))
        keys = list(data_dict.keys())
        if start_idx in keys:
            start_pos = keys.index(start_idx)
        else:
            start_pos = max(0, int(start_idx))
        return keys, start_pos

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


def xml_merge(self, xml_a: Union[etree._Element, etree._ElementTree], xml_b: Union[etree._Element, etree._ElementTree], x_parent: Union[etree._Element, etree._ElementTree, None] = None) -> etree._Element:
    """
    Merges two xml's together. xml_b overwrites xml_a, unless xml_b's element contains a blank value.
    If blank_override is set, blank value will override xml_a's element.

    :param xml_a: xml to merge into
    :param xml_b: xml to merge from
    :param x_parent: xml parent, only use for recursion
    """
    a_root = xml_a.getroot() if isinstance(xml_a, etree._ElementTree) else xml_a
    b_root = xml_b.getroot() if isinstance(xml_b, etree._ElementTree) else xml_b

    for b_child in b_root.findall('./'):
        a_child = a_root.find('./' + b_child.tag)
        if a_child is not None:
            logger.debug(f'Found matching element in original XML for {b_child.tag}')
            x_parent = None
            if a_child.text is not None:
                if b_child.text:
                    logger.debug(f'Updating element: {b_child.tag} with value: {b_child.text}')
                    a_child.text = b_child.text
                elif self.blank_override is True and b_child.tag in self.xnames:
                    logger.debug(f'Blank override enabled, updating element: {b_child.tag} with blank value')
                    a_child.text = None
                else:
                    logger.debug(f'Keeping existing value for element: {b_child.tag} with value: {a_child.text}')
                    a_child.text = a_child.text
            else:
                if b_child.text:
                    logger.debug(f'Updating blank element: {b_child.tag} with value: {b_child.text}')
                    a_child.text = b_child.text
                else:
                    logger.debug(f'Element: {b_child.tag} is blank in both XMLs, keeping as blank')
                    pass
        else:
            logger.debug(f'Element: {b_child.tag} not found in original XML, adding element with value: {b_child.text}')
            x_parent = a_root
            a_child = etree.SubElement(a_root,b_child.tag)
            if b_child.text:
                logger.debug(f'Adding value for new element: {b_child.tag} with value: {b_child.text}')
                a_child.text = b_child.text
            else:
                logger.debug(f'New element: {b_child.tag} is blank, setting as blank')
                a_child.text = None
        if len(b_child) > 0:
            logger.debug(f'Element: {b_child.tag} has children, merging children elements')
            self.xml_merge(a_child,b_child,x_parent)
    logger.debug(f'Merged XML: {etree.tostring(xml_a)}')
    return a_root