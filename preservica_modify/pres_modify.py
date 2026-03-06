"""
Preservica Mass Modification tool.

This tool is utilised to modify existing data on Preservica through the use of spreadsheets.
Allows for modification of XIP and XML data.

Author: Christopher Prince
license: Apache License 2.0"
"""


from pyPreservica import EntityAPI, RetentionAPI, UploadAPI, WorkflowAPI, AdminAPI, Entity, EntityType
import pandas as pd
from lxml import etree
from datetime import datetime
import os, re
from preservica_modify.common import check_nan, check_bool, export_csv, export_json, export_xml, export_xl, export_ods
from typing import Optional, Union, Dict, List, Hashable, Any
import logging
import configparser
from getpass import getpass

try:
    import keyring
except Exception:
    keyring = None
    class KeyringError(Exception):
        pass

logger = logging.getLogger(__name__)

class PreservicaMassMod:
    """
    Mass Modification Class
    """
    def __init__(self,
                 input_file: str,
                 metadata_dir: str = os.path.join(os.getcwd(),"metadata"),
                 blank_override: bool = False,
                 upload_mode: bool = False,
                 metadata: Optional[str] = None,
                 descendants: Optional[set] = None,
                 dummy: bool = False,
                 username: Optional[str] = None,
                 password: Optional[str] = None,
                 manager_username: Optional[str] = None,
                 manager_password: Optional[str] = None,
                 server: Optional[str] = None,
                 tenant: Optional[str] = None,
                 credentials: str = os.path.join(os.getcwd(),"credentials.properties"),
                 delete: bool = False,
                 use_keyring: bool = False,
                 keyring_service: str = "preservica_modify",
                 save_password_to_keyring: bool = False,
                 disable_continue: bool = False,
                 column_sensistivity: bool = False,
                 options_file: str = os.path.join(os.path.dirname(__file__),'options', 'options.properties')):
        
        self.metadata_dir = metadata_dir
        self.metadata_flag = metadata
        self.dummy_flag = dummy
        self.blank_override = blank_override
        self.delete_flag = delete
        self.descendants_flag = descendants
        
        self.upload_flag = upload_mode

        if credentials is not None:
            if os.path.isfile(credentials):
                self.credentials_file = credentials
            else: 
                self.credentials_file = None
        else:
            self.credentials_file = None

        self.input_file = input_file

        self.username = username
        self.password = password
        self.server = server
        self.tenant = tenant
        self.manager_username = manager_username
        self.manager_password = manager_password

        self.use_keyring = use_keyring
        self.keyring_service = keyring_service
        self.save_password_to_keyring = save_password_to_keyring
        
        self.disable_continue = disable_continue

        self.column_sensistivity = column_sensistivity

        if options_file is None:
            options_file = os.path.join(os.path.dirname(__file__),'options','options.properties')
        self.parse_config(options_file=os.path.abspath(options_file), column_sensistivity=self.column_sensistivity)

        self.xnames: list[str] = []

    def parse_config(self, options_file: str, column_sensistivity: bool = False) -> None:
        config = configparser.ConfigParser()
        read_config = config.read(options_file, encoding='utf-8')
        if not read_config:
            logger.warning(f"Options file not found or not readable: {options_file}. Using defaults.")

        section = config['options'] if 'options' in config else {}

        if column_sensistivity:
            section = {k:v.lower() for k,v in section.items()}

        self.ENTITY_REF=section.get('ENTITY_REF', 'Entity Ref')
        self.DOCUMENT_TYPE=section.get('DOCUMENT_TYPE', 'Document type')
        self.UPLOAD_TYPE=section.get('UPLOAD_TYPE', 'Upload type')
        self.TITLE_FIELD=section.get('TITLE_FIELD', 'Title')
        self.DESCRIPTION_FIELD=section.get('DESCRIPTION_FIELD', 'Description')
        self.SECURITY_FIELD=section.get('SECURITY_FIELD', 'Security')
        self.RETENTION_FIELD=section.get('RETENTION_FIELD', 'Retention Policy')
        self.MOVETO_FIELD=section.get('MOVETO_FIELD', 'Move to')
        self.DELETE_FIELD=section.get('DELETE_FIELD', 'Delete')
        self.IDENTIFIER_FIELD=section.get('IDENTIFIER_FIELD', 'Identifier')
        self.IDENTIFIER_DEFAULT=section.get('IDENTIFIER_DEFAULT', 'code')
        self.FILE_PATH=section.get('FILE_PATH', 'FullName')       
        
        self.UPLOAD_TYPE_FIELD=section.get('UPLOAD_TYPE_FIELD', 'Upload type')
        self.PAX_PRES_FIELD=section.get('PAX_PRES_FIELD', 'Preservation')
        self.PAX_ACCESS_FIELD=section.get('PAX_ACCESS_FIELD', 'Access')
        self.PAX_PATH=section.get('PAX_PATH', 'PAX Path')

        self.ARCREF_FIELD=section.get('ARCREF_FIELD', 'Archive_Reference')
        self.ACCREF_FIELD=section.get('ACCREF_FIELD', 'Accession_Reference')
        self.ACCREF_CODE=section.get('ACCREF_CODE', 'accref')
        
        logger.debug(f'Configuration loaded: {section}')

    def _keyring_entry_name(self) -> str:
        tenant = self.tenant or "default"
        server = self.server or "default-server"
        return f"{self.keyring_service}:{server}:{tenant}"

    def _get_password_from_keyring(self, username) -> Optional[str]:
        if not self.use_keyring:
            return None
        if keyring is None:
            log_msg = "keyring package is not installed. Install with: pip install keyring"
            raise RuntimeError(log_msg)
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
            log_msg = "keyring package is not installed. Install with: pip install keyring"
            raise RuntimeError(log_msg)
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
                    log_msg = 'A Username or Server has not been provided... Please try again...'
                    logger.error(log_msg)
                    raise ValueError(log_msg)

                if password is None and self.save_password_to_keyring is False:
                    password = self._get_password_from_keyring(username)
                
                if password is None:
                    password = getpass(prompt=f"Please enter your password for Preservica for {username}: ")
                    if self.save_password_to_keyring is True:
                        self._set_password_in_keyring(username, password)
                
                if password is not None:
                    return password
                else:
                    log_msg = 'Password not provided and could not be retrieved from keyring. Please try again...'
                    logger.error(log_msg)
                    raise ValueError(log_msg)
            
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
            log_msg = 'Failed to login to Preservica...'
            logger.exception(log_msg)
            raise

    def test_login(self):
        """
        Test Login function, to ensure credentials are correct before running main.
        """
        try:
            self.login_preservica()
            logger.info(f'Login successful: {self.server} as {self.username}')
        except Exception:
            log_msg = 'Login failed'
            logger.exception(log_msg)
            raise

    def _save_continue_token(self, token_file: str, token: Optional[int]) -> None:
        try:
            token_file = token_file + "_continue.txt"
            if token is not None:
                with open(token_file, 'w') as f:
                    f.write(str(token))
                logger.info(f'Continue token saved to {token_file}')
        except Exception as e:
            log_msg = f'Failed to save continue token to file {token_file}: {e}'
            logger.exception(log_msg)
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
                        log_msg = f'Invalid continue token value in {token_file}: {token_str}'
                        logger.error(log_msg)
                        raise ValueError(log_msg)
        except FileNotFoundError as e:
            log_msg = f'Continue token file not found: {token_file}, starting from beginning...'
            logger.error(log_msg)
            return 0
        except Exception as e:
            log_msg = f'Invalid continue token value in {token_file}, starting from beginning...'
            logger.exception(log_msg)
            raise
                        
    def _remove_continue_token(self, token_file: str) -> None:
        token_file = token_file + "_continue.txt"
        if os.path.isfile(token_file):
            os.remove(token_file)
            logger.info(f'Continue token file {token_file} removed.')

    def _set_input_flags(self) -> None:
        """
        Sets the input flags
        """
        self.title_flag = False
        self.description_flag = False
        self.security_flag = False
        self.retention_flag = False
        self.dest_flag = False
        self.move_flag = False
        if self.TITLE_FIELD in self.column_headers:
            self.title_flag = True
        if self.DESCRIPTION_FIELD in self.column_headers:
            self.description_flag = True
        if self.SECURITY_FIELD in self.column_headers:
            self.security_flag = True
        if self.RETENTION_FIELD in self.column_headers:
            self.retention_flag = True
        if self.MOVETO_FIELD in self.column_headers:
            self.move_flag = True
        logger.debug(f'Input Flags - Title: {self.title_flag}, Description: {self.description_flag}, Security: {self.security_flag}, Retention: {self.retention_flag}, Move: {self.move_flag}')

    def init_df(self) -> None:

        from .cli import fmthelper

        input_fmt = fmthelper(os.path.splitext(self.input_file)[-1].replace('.',''))
        
        logger.info(f'Initializing dataframe from input file: {self.input_file} with detected format: {input_fmt}. May take time to load.')

        if input_fmt.endswith("xlsx"):
            self.df: pd.DataFrame = pd.read_excel(self.input_file)
        elif input_fmt.endswith("csv"):
            self.df: pd.DataFrame = pd.read_csv(self.input_file)
        elif input_fmt.endswith("ods"):
            self.df: pd.DataFrame = pd.read_excel(self.input_file, engine='odf')
        elif input_fmt.endswith("json"):
            self.df: pd.DataFrame = pd.read_json(self.input_file, orient='index')
        elif input_fmt.endswith("xml"):
            self.df: pd.DataFrame = pd.read_xml(self.input_file)
        else:
            log_msg = "Unsupported file type for input. Please use .xlsx, .csv, .json or .xml"
            logger.error(log_msg)
            raise ValueError(log_msg)
       
        self.column_headers = list(self.df.columns.values)
        if self.column_sensistivity is True:
            self.column_headers = [str(header).lower() for header in self.column_headers]
            self.df.columns = self.column_headers
        date_headers = [header for header in self.column_headers if "date" in str(header).lower()]
        self.df[date_headers] = self.df[date_headers].apply(lambda x: pd.to_datetime(x,format='mixed'))


    def print_local_xmls(self) -> None:
        try:
            for file in os.scandir(self.metadata_dir):
                path = os.path.join(self.metadata_dir, file.name)
                print(path)
                xml_file = etree.parse(path)
                root_element = etree.QName(xml_file.find('.'))
                root_element_ln = root_element.localname
                for elem in xml_file.findall(".//"):
                    if len(elem) > 0:
                        pass
                    else:
                        elem_path = xml_file.getelementpath(elem)
                        elem = etree.QName(elem)
                        elem_lnpath = elem_path.replace(f"{{{elem.namespace}}}", root_element_ln + ":")
                        print(elem_lnpath)
        except Exception as e:
            log_msg = f'Failed to print Descriptive metadta files, ensure correct path {e}'
            logger.exception(log_msg)
            raise

    def print_remote_xmls(self) -> None:
        try:
            self.login_preservica()
            xml_files = self.admin.xml_documents()
            for xml_dict in [x_d for x_d in xml_files if x_d.get('DocumentType') == 'MetadataTemplate']:
                print(xml_dict.get('Name'))
                xml = self.admin.xml_document(xml_dict.get('SchemaUri'))
                if xml is None:
                    logger.warning(f'No XML content found for {xml_dict.get("Name")}, skipping...')
                    continue
                xml_file = etree.fromstring(xml.encode('utf-8')).getroottree()
                root_element = etree.QName(xml_file.find('.'))
                root_element_ln = root_element.localname
                for elem in xml_file.findall(".//"):
                    if len(elem) > 0:
                        pass
                    else:
                        elem_path = xml_file.getelementpath(elem)
                        elem = etree.QName(elem)
                        elem_lnpath = elem_path.replace(f"{{{elem.namespace}}}", root_element_ln + ":")
                        print(elem_lnpath)
        except Exception as e:
            log_msg = f'Failed to print Descriptive metadta files, ensure correct path {e}'
            logger.exception(log_msg)
            raise

    def convert_local_xmls(self, output_format: str) -> None:
        try:
            for file in os.scandir(self.metadata_dir):
                path = os.path.join(self.metadata_dir, file.name)
                xml_file = etree.parse(path)
                root_element = etree.QName(xml_file.find('.'))
                root_element_ln = root_element.localname
                column_list = []
                for elem in xml_file.findall(".//"):
                    if len(elem) > 0:
                        pass
                    else:
                        elem_path = xml_file.getelementpath(elem)
                        elem = etree.QName(elem)
                        elem_lnpath = elem_path.replace(f"{{{elem.namespace}}}", root_element_ln + ":")
                        column_list.append(elem_lnpath)
                xml_df = pd.DataFrame(columns=column_list,index=None)
                if output_format == 'xlsx':
                    export_xl(xml_df,file.name.replace('.xml','.xlsx'))
                elif output_format == 'ods':
                    export_ods(xml_df,file.name.replace('.xml','.ods'))
                elif output_format == 'csv':
                    export_csv(xml_df,file.name.replace('.xml','.csv'))
                elif output_format == 'json':
                    export_json(xml_df,file.name.replace('.xml','.json'))
                elif output_format == 'dict':
                    xml_dict = {col: None for col in column_list}
                    export_json(pd.DataFrame([xml_dict]), file.name.replace('.xml','.json'), orient='records')
                else:
                    export_xl(xml_df, file.name.replace('.xml','.xlsx'))
        except Exception as e:
            log_msg = f'Failed to print Descriptive metadta files, ensure correct path {e}'
            logger.exception(log_msg)
            raise

    def convert_remote_xmls(self, output_format: str) -> None:
        try:
            self.login_preservica()
            xml_files = self.admin.xml_documents()
            for xml_dict in [x_d for x_d in xml_files if x_d.get('DocumentType') == 'MetadataTemplate']:
                xml = self.admin.xml_document(xml_dict.get('SchemaUri'))
                xml_name = xml_dict.get('Name')
                if xml is None:
                    logger.warning(f'No XML content found for {xml_dict.get("Name")}, skipping...')
                    continue
                xml_file = etree.fromstring(xml.encode('utf-8')).getroottree()
                root_element = etree.QName(xml_file.find('.'))
                root_element_ln = root_element.localname
                column_list = []
                for elem in xml_file.findall(".//"):
                    if len(elem) > 0:
                        pass
                    else:
                        elem_path = xml_file.getelementpath(elem)
                        elem = etree.QName(elem)
                        elem_lnpath = elem_path.replace(f"{{{elem.namespace}}}", root_element_ln + ":")
                        column_list.append(elem_lnpath)
                xml_df = pd.DataFrame(columns=column_list,index=None)
                if output_format == 'xlsx':
                    export_xl(xml_df, xml_name + '.xlsx')
                elif output_format == 'ods':
                    export_ods(xml_df, xml_name +'.ods')
                elif output_format == 'csv':
                    export_csv(xml_df, xml_name +'.csv')
                elif output_format == 'json':
                    export_json(xml_df, xml_name + '.json')
                elif output_format == 'xml':
                    export_xml(xml_df, xml_name + '.xml')
                elif output_format == 'dict':
                    xml_dict = {col: None for col in column_list}
                    export_json(pd.DataFrame([xml_dict]), xml_name + '.json', orient='records')
                else:
                    export_xl(xml_df, xml_name + '.xlsx')
        except Exception as e:
            log_msg = f'Failed to print Descriptive metadta files, ensure correct path {e}'
            logger.exception(log_msg)
            raise

    def get_retentions(self) -> list[dict]:
        """
        Retrieves retention policies from Preservica and parses them into a dict
        """
        self.policies = self.retention.policies()
        self.policy_dict = [{"Name": p.name, "Reference": p.reference} for p in self.policies.get_results()]
        logger.info(f'Retention Policies retrieved')
        logger.debug(f'Retention Policies obtained: {self.policy_dict}')
        return self.policy_dict

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
    
    def _cell(self, idx: Any, column: str) -> Any:
        return self.df.at[idx, column]

    def xip_lookup(self, idx: Hashable) -> tuple[Optional[str], Optional[str], Optional[str]]:
        """
        Uses the pandas index to retreieve data from the "Title, Description and Security" columns. Sets data in Entity.
        :param idx: Pandas Index to lookup
        """

        if getattr(self, 'df', None) is None:
            log_msg = 'Dataframe not initialised, cannot perform lookup'
            logger.error(log_msg)
            raise RuntimeError(log_msg)
        try:
            if self.title_flag:
                title = check_nan(self._cell(idx, self.TITLE_FIELD))
                logger.debug(f'XIP Lookup Title: {title}')
            else:
                title = None
            if self.description_flag:
                description = check_nan(self._cell(idx, self.DESCRIPTION_FIELD))
                if description is None and self.blank_override is True:
                    description = None
                logger.debug(f'XIP Lookup Description: {description}')
            else:
                description = None
            if self.security_flag:
                security = check_nan(self._cell(idx, self.SECURITY_FIELD))
                logger.debug(f'XIP Lookup Security: {security}')
            else:
                security = None
            return title, description, security
        except KeyError as e:
            log_msg = f'Key Error XIP Lookup, missing Column: {e} please ensure column header\'s are an exact match...'
            logger.exception(log_msg)
            raise KeyError(log_msg) from e
        except IndexError as e:
            logger.warning(f"Index Error for XIP Lookup: {e} it is likely you have removed or added a file/folder to the directory"
                         "after generating the spreadsheet. An opex will still be generated but with no identifiers. To ensure identifiers match up please ensure match up...")
            return None, None, None
        except Exception:
            log_msg = f'Retention XIP failed: for {idx}'
            logger.exception(log_msg)
            raise
    
    def ident_lookup(self, idx: Hashable, default_key: Optional[str] = None) -> Optional[Dict[str, Optional[str]]]:
        """
        Uses the pandas index to retreieve data from the "Identifer","Archive_Reference", columns. Sets identifers in Entity.
        "Archive_Reference" & "Accession_Reference" are hard-set.

        :param idx: Pandas Index to lookup
        :param e: Entity to act upon
        :param key_default: Set the default key value  
        """
        try:
            ident_dict = {}
            for header in self.column_headers:
                header = str(header)
                if any(s in header for s in {self.IDENTIFIER_FIELD,'Archive_Reference','Accession_Reference'}):
                    if f"{self.IDENTIFIER_FIELD}:" in header:
                        key_name = str(header).rsplit(':',1)[-1]
                    else:
                        if self.ARCREF_FIELD in header:
                            key_name = default_key if default_key else self.IDENTIFIER_DEFAULT
                        elif self.ACCREF_FIELD in header:
                            key_name = self.ACCREF_CODE
                        elif self.IDENTIFIER_FIELD in header:
                            key_name = default_key if default_key else self.IDENTIFIER_DEFAULT
                        else:
                            key_name = default_key if default_key else self.IDENTIFIER_DEFAULT
                    ident = check_nan(self._cell(idx, header))
                    logger.debug(f'Identifier Lookup for {key_name}: {ident}')
                    ident_dict.update({key_name:ident})
            if len(ident_dict) == 0:
                ident_dict = None
            return ident_dict
        except KeyError as e:
            log_msg = f'Key Error for Identifier Lookup, missing Column: {e} please ensure column header\'s are an exact match...'
            logger.exception(log_msg)
            raise KeyError(log_msg) from e
        except IndexError as e:
            logger.warning(f"Index Error for Identifier Lookup: {e} it is likely you have removed or added a file/folder to the directory"
                         "after generating the spreadsheet. An opex will still be generated but with no identifiers. To ensure identifiers match up please ensure match up...")
        except Exception:
            log_msg = f'Identifier Lookup failed: for {idx}'
            logger.exception(log_msg)
            raise
        
    def retention_lookup(self, idx: Hashable):
        """
        Uses the pandas index to retreieve data from the "Retention Policy" column

        Matched agasint the policies obtained in the obtain_retentions function.

        :param idx: Pandas Index to lookup
        :param e: Entity to act upon
        """
        try:
            if self.retention_flag:
                retention_policy = check_nan(self._cell(idx,self.RETENTION_FIELD))
            else:
                retention_policy = None
            logger.debug(f'Retention Lookup for {idx}: {retention_policy}')    
            return retention_policy
        except KeyError as e:
            log_msg = f'Key Error Retention Lookup, missing Column: {e} please ensure column header\'s are an exact match...'
            logger.exception(log_msg)
            raise KeyError(log_msg) from e
        except IndexError as e:
            logger.warning(f"Index Error for Retention Lookup: {e} it is likely you have removed or added a file/folder to the directory"
                         "after generating the spreadsheet. An opex will still be generated but with no identifiers. To ensure identifiers match up please ensure match up...")
        except Exception:
            log_msg = f'Retention Lookup failed: for {idx}'
            logger.exception(log_msg)
            raise

    def pax_lookup(self, idx: Hashable) -> Optional[tuple[Optional[str], Optional[list], Optional[list]]]:
        try:
            pax_path = check_nan(self._cell(idx, self.PAX_PATH))
            pax_dict = self.df.loc[[self.df[self.PAX_PATH] == pax_path, [self.FILE_PATH,self.PAX_PRES_FIELD,self.PAX_ACCESS_FIELD]]].to_dict(orient='index')
            file_access = check_bool(self._cell(idx, self.PAX_ACCESS_FIELD))
            file_preservation = check_bool(self._cell(idx, self.PAX_ACCESS_FIELD))
            if file_access is False and file_preservation is False:
                logger.info(f'Preservation/Access is not set for {idx, pax_path}, setting Preservation to True by default')
                pax_dict[idx][self.PAX_PRES_FIELD] = True
            if file_access is True and file_preservation is True:
                logger.info(f'Both Preservation and Access are set to True for PAX: {idx, pax_path}, only using Preservation file in PAX')
                pax_dict[idx][self.PAX_ACCESS_FIELD] = False
            pres_list = [v.get(self.FILE_PATH) for v in pax_dict.values() if check_bool(v.get(self.PAX_PRES_FIELD))]
            logger.debug(f'PAX Preservation List for {idx}: {pres_list}')
            access_list = [v.get(self.FILE_PATH) for v in pax_dict.values() if check_bool(v.get(self.PAX_ACCESS_FIELD))]
            logger.debug(f'PAX Access List for {idx}: {access_list}')
            return pax_path, pres_list, access_list
        except KeyError as e:
            log_msg = f'Key Error for Pax Lookup, missing Column: {e} please ensure column header\'s are an exact match...'
            logger.exception(log_msg)
            raise KeyError(log_msg) from e
        except IndexError as e:
            logger.warning(f"Index Error for Pax Lookup: {e} it is likely you have removed or added a file/folder to the directory"
                         "after generating the spreadsheet. An opex will still be generated but with no identifiers. To ensure identifiers match up please ensure match up...")
        except Exception:
            log_msg = f'Pax Lookup failed: for {idx}'
            logger.exception(log_msg)
            raise

    def delete_lookup(self, idx: Hashable):
        """
        Uses the pandas index to retrieve data from the "Delete" column. If True intitaites a Delete.                            self.xml_update(ent, ns, xml_new)
                if ent.entity_type == EntityType.ASSET and self.retention_flag is True:
                    self.retention_update(ent, self.retention_lookup(idx))

        Requires use of a .credentials file.

        Delete Flag must also be set.
        """
        try:
            return check_bool(self._cell(idx, self.DELETE_FIELD))    
        except Exception:
            log_msg = 'Failed to lookup delete flag'
            logger.exception(log_msg)
            raise

    def init_generate_descriptive_metadata(self) -> List[Dict[str, Any]]:
        """
        Initiation for the generate_descriptive_metadata function. Seperated to avoid unecessary repetition.
        First takes xmls files in metadata_dir, generates a list of dicts of the elements in XML file. Then compares the Column headers in the spreadsheet against the XML's in the Metadata Directory.
        """
        try:
            self.xml_files: List[Dict[str, Any]] = []
            for file in os.scandir(self.metadata_dir):
                list_xml: List[Dict[str,Any]] = []
                if file.name.endswith('.xml'):
                    path = os.path.join(self.metadata_dir, file.name)
                    try:
                        xml_file = etree.parse(path)
                    except etree.XMLSyntaxError as e:
                        log_msg = f'XML Syntax Error while parsing {file.name}: {e}'
                        logger.exception(log_msg)
                        raise etree.XMLSyntaxError(log_msg) from e
                    except FileNotFoundError as e:
                        log_msg = f'File not found while parsing {file.name}: {e}'
                        logger.exception(log_msg)
                        raise FileNotFoundError(log_msg) from e
                    root_element = etree.QName(xml_file.find('.'))
                    root_element_ln = root_element.localname
                    root_element_ns = root_element.namespace
                    elements_list = []
                    for elem in xml_file.findall('.//'):
                        elem_path = xml_file.getelementpath(elem)
                        elem = etree.QName(elem)
                        elem_ln = elem.localname
                        elem_ns = elem.namespace
                        elem_lnpath = elem_path.replace(f"{{{elem_ns}}}", root_element_ln + ":")
                        elements_list.append({"Name": root_element_ln + ":" + elem_ln, "XName": f"{{{elem_ns}}}{elem_ln}", "Namespace": elem_ns, "Path": elem_lnpath})
                    try:
                        for elem_dict in elements_list:
                            if elem_dict.get('Name') in self.column_headers or elem_dict.get('Path') in self.column_headers:
                                list_xml.append({"Name": elem_dict.get('Name'), "XName": elem_dict.get('XName'), "Namespace": elem_dict.get('Namespace'), "Path": elem_dict.get('Path')})
                    except Exception:
                        log_msg = f'Error comparing XML elements to column headers for file {file.name}'
                        logger.exception(log_msg)
                        raise
                if len(list_xml) > 0:
                    self.xml_files.append({'data': list_xml, 'localname': root_element_ln, 'localns': root_element_ns, 'xmlfile': path})
                    logger.info(f'Matching columns found in spreadsheet for XML file: {file.name}, added to metadata generation list.')
                else:
                    logger.warning(f'No matching columns found in spreadsheet for XML file: {file.name}, skipping this file for metadata generation.')
            return self.xml_files
        except FileNotFoundError as e:
            log_msg = f'Metadata directory not found: {e}'
            logger.exception(log_msg)
            raise FileNotFoundError(log_msg) from e
        except Exception:
            log_msg = 'Failed to initialize descriptive metadata generation'
            logger.exception(log_msg)
            raise
    
    def generate_descriptive_metadata(self, idx: Hashable, xml_files: List[Dict[str, Any]]) -> Optional[List[Dict[str, Union[etree._ElementTree, list[Optional[str]]]]]]:
        """
        Generates the xml file based on the returned list of xml_files from the init_generate_descriptive_metadata function.

        :param idx: Pandas Index to lookup
        :param xml_file: Dictionary of XML files created as part of init 
        """
        try:
            xml_list = []
            for xml_file in xml_files:
                xml_file: Dict[str, Any]
                xml_data = xml_file.get('data')
                xnames: list[Optional[str]] = []
                xnames = [x.get('XName') for x in xml_data if isinstance(x, Dict)] if xml_data is not None else []                
                localname = xml_file.get('localname', None)
                localns = xml_file.get('localns', None)
                if localname is None or localns is None:
                    logger.warning(f'Missing localname or localns for XML file: {xml_file.get("xmlfile")}, skipping XML generation for this file.')
                    continue
                if xml_data is None or len(xml_data) == 0:
                    logger.warning(f'No XML elements found for {xml_file.get("xmlfile")}, skipping XML generation for this file.')
                    continue
                else:
                    xml_new = etree.parse(str(xml_file.get('xmlfile')))
                    for elem_dict in xml_data:
                        if not isinstance(elem_dict, Dict):
                            logger.warning(f'Invalid element data for {elem_dict} in file {xml_file.get("xmlfile")}, skipping this element.')
                            continue
                        name = elem_dict.get('Name')
                        path = elem_dict.get('Path')
                        elmns = elem_dict.get('Namespace')
                        if not isinstance(name, str) or not isinstance(path, str) or not isinstance(elmns, str):
                            logger.warning(f'Missing Name or Path for element {elem_dict} in file {xml_file.get("xmlfile")}, skipping this element.')
                            continue
                        if self.metadata_flag in {'exact'}:
                            val = check_nan(self._cell(idx, path))
                        elif self.metadata_flag in {'flat'}:
                            val = check_nan(self._cell(idx, name))
                        if pd.isnull(val) or val is None:
                            continue
                        else:
                            if pd.api.types.is_datetime64_dtype(val):
                                val = pd.to_datetime(val)
                                val = datetime.strftime(val, "%Y-%m-%dT%H:%M:%S.000Z")
                        if self.metadata_flag in {'exact'}:
                            n = path.replace(localname + ":", f"{{{elmns}}}")
                            elem = xml_new.find(f'./{n}')
                            if elem is None:
                                logger.warning(f'XML element not found for path: {n, path} in {xml_file}.')
                                continue
                        elif self.metadata_flag in {'flat'}:
                            n = name.split(':')[-1]
                            elem = xml_new.find(f'.//{{{elmns}}}{n}')
                            if elem is None:
                                logger.warning(f'Element not found in XML for {name, path} in {xml_file}.')
                                continue
                        if elem is not None:
                            elem.text = str(val)
                    xml_list.append({localns: xml_new, 'xnames': xnames})
            return xml_list
        except KeyError as e:
            log_msg = f'Key Error, missing Column: {e} please ensure column header\'s are an exact match...'
            logger.exception(log_msg)
            raise KeyError(log_msg)
        except IndexError as e:
            logger.warning(f"Index Error: {e} it is likely you have removed or added a file/folder to the directory" \
                "after generating the spreadsheet. An opex will still be generated but with no xml metadata." \
                "To ensure metadata match up please ensure match up...")
        except Exception:
            log_msg = f'Error generating descriptive metadata for {idx, path}'
            logger.exception(log_msg)
            raise

    def xip_update(self, ent: Entity, title: Optional[str] = None, description: Optional[str] = None, security: Optional[str] = None):
        try:
            if title:
                logger.info(f"Updating {ent.reference} Title from {ent.title} to {title}")
                if self.dummy_flag is False:
                    ent.title = title
            if description:
                logger.info(f"Updating {ent.reference} Description from {ent.description} to {description}")
                if self.dummy_flag is False:
                    ent.description = description
            if security:
                logger.info(f"Updating {ent.reference} Security Tag from {ent.security_tag} to {security}")
                if self.dummy_flag is False:
                    self.entity.security_tag_async(ent, security)
            if any([title,description]):
                self.entity.save(ent)
        except Exception:
            log_msg = 'Error updating XIP metadata'
            logger.exception(log_msg)
            raise
    
    def ident_update(self, ent: Entity, ident_dict: Union[dict,None]):
        if ident_dict is None:
            return
        try:
            xip_idents = self.entity.identifiers_for_entity(ent)
            for items in ident_dict.items():
                key_name = items[0]
                ident = items[1]
                if ident is not None:
                    if any(x[0] for x in xip_idents if x[0] == key_name):
                        oldident = [x[1] for x in xip_idents if x[0] == key_name][0]
                        logger.info(f'Updating {ent.reference} Updating identifier {key_name, oldident} to: {key_name, ident}')
                        if self.dummy_flag is False:
                            self.entity.update_identifiers(ent,key_name,str(ident))
                    else: 
                        logger.info(f'Updating {ent.reference} Adding identifier: {key_name, ident}')
                        if self.dummy_flag is False:
                            self.entity.add_identifier(ent,key_name,str(ident))
                else:
                    if self.blank_override is True:
                        if any(x[0] for x in xip_idents if x[0] == key_name):
                            oldident = [x[1] for x in xip_idents if x[0] == key_name][0]
                            logger.info(f'Updating {ent.reference} Deleting identifier {key_name, oldident}')
                            if self.dummy_flag is False:
                                self.entity.delete_identifiers(ent,key_name,str(oldident))
                        else:
                            pass
        except Exception:
            log_msg = 'Error updating identifiers'
            logger.exception(log_msg)
            raise
                    
    def retention_update(self, ent: Entity, retention_policy: Optional[str] = None):
        try:
            if retention_policy is not None:
                assignments = self.retention.assignments(ent)
                policies = [policy for policy in self.policy_dict if policy.get('Name') == retention_policy]
                if len(policies) > 1:
                    logger.warning(f'Multiple Retention Policies found for reference: {ent.reference}, taking no action.')
                elif len(policies) == 1:
                    policy = policies[0].get('Reference', None)
                    policy_name = policies[0].get('Name', None)
                    if policy is None:
                        log_msg = f'Retention policy reference not found for name: {retention_policy}'
                        logger.error(log_msg)
                        raise LookupError(log_msg)
                    if any(assignments):
                        for ass in assignments:
                            logger.info(f"Updating {ent.reference} Removing retention policy: {ass.policy_reference}")
                            if self.dummy_flag is False:
                                self.retention.remove_assignments(ass)
                    logger.info(f"Updating {ent.reference} Adding retention policy: {policy, policy_name}")
                    if self.dummy_flag is False:
                        self.retention.add_assignments(ent,self.retention.policy(policy))
                elif len(policies) == 0:
                    log_msg = f'Retention policy not found for name: {retention_policy}'
                    logger.error(log_msg)
                    raise LookupError(log_msg)

            elif retention_policy is None and self.blank_override is True:
                assignments = self.retention.assignments(ent)
                if any(assignments):
                    for ass in assignments:
                        logger.info(f"Updating {ent.reference} Removing Retention Policy: {ass.policy_reference}")
                        if self.dummy_flag is False:
                            self.retention.remove_assignments(ass)
            else:
                pass                    
        except Exception:
            log_msg = f'Error updating retention: {ent.reference}'
            logger.exception(log_msg)
            raise
                    
    def xml_update(self, ent: Entity, ns: str, xml_new: etree._ElementTree):
        """
        Makes the call on Preservica's API using pyPreservica to update, remove or add metadata from given entity.

        The namespace must also be passed - this is retrieved automatically. 

        :param e: Entity to act upon
        :param ns: Namespace of XML being updated
        """
        try:
            #Change so it's dynamic - not only self.upload_flag - also indent_update needs same treatment
            if self.upload_flag:
                emeta = None
            else:
                emeta = self.entity.metadata_for_entity(ent, ns)
            # Check if metadata exists for the entity
            if emeta is None:
                xml_to_upload = etree.tostring(xml_new)
                logger.info(f"Updating {ent.reference} Adding Metadata for: {ns}")
                logger.debug(f'New XML Metadata: {xml_to_upload}')
                if self.dummy_flag is False:
                    self.entity.add_metadata(ent, ns, xml_to_upload.decode('utf-8'))
            # Metadata exists, merge and update
            else:
                xml_to_upload = etree.tostring(self.xml_merge(etree.fromstring(emeta), xml_new))
                logger.info(f"Updating {ent.reference} Updating Metadata for: {ns}")
                logger.debug(f'Updated XML Metadata: {xml_to_upload}')
                if self.dummy_flag is False:
                    self.entity.update_metadata(ent, ns, xml_to_upload.decode('utf-8'))
        except Exception:
            log_msg = 'Error updating XML metadata'
            logger.exception(log_msg)
            raise

    def move_update(self, idx: int, ent: Entity):
        """
        Uses the pandas index to retreieve data from the "Move To" column. Intitaites a move. 

        :param idx: Pandas Index to lookup
        :param Enitty: Entity to act upon
        """
        if self.move_flag is True:
            dest = check_nan(self.df[self.MOVETO_FIELD].loc[idx])
            if dest is not None:
                if re.search("^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$", dest):
                    dest_folder = self.entity.folder(dest)
                    logger.info(f'Moving Entity: {ent.reference}, {ent.title}, to: {dest_folder.reference}, {dest_folder.title}')
                    if self.dummy_flag is False:
                        self.entity.move_async(entity=ent, dest_folder=dest_folder)
                else:
                    log_msg = f'Reference: {ent.reference} in "Move To" is formatted incorrectly: {dest}'
                    logger.error(log_msg)
                    raise ValueError(log_msg)

    def delete_update(self, idx: Hashable, ent: Entity):
        """
        Uses the pandas index to retrieve data from the "Delete" column. If True intitaites a Delete.
        Requires use of a .credentials file.

        Delete Flag must also be set.
        """
        try:
            if self.delete_flag is True:
                delete_conf = self.delete_lookup(idx)
                if delete_conf is True and (self.manager_username or self.credentials_file):
                    if ent.entity_type == EntityType.ASSET:
                        logger.info(f'Deleting Asset: {ent.reference}')
                        if self.dummy_flag is False:
                            self.entity.delete_asset(self.entity.asset(ent.reference),"Deleted by Preservica Mass Modify","Deleted by Preservica Mass Modify", self.credentials_file, self.manager_username, self.manager_password)
                        return True
                    elif ent.entity_type == EntityType.FOLDER:
                        logger.info(f'Deleting Folder: {ent.reference}')
                        if self.dummy_flag is False:
                            self.entity.delete_folder(self.entity.folder(ent.reference),"Deleted by Preservica Mass Modify","Deleted by Preservica Mass Modify", self.credentials_file,  self.manager_username, self.manager_password)
                        return True
                if delete_conf is True:
                    log_msg = 'Delete requested but no manager username or credentials file provided.'
                    logger.error(log_msg)
                    raise PermissionError(log_msg)
            else:
                return False
        except KeyError as e:
            log_msg = f'Failed to delete entity due to missing key: {e}'
            logger.exception(log_msg)
            raise KeyError(log_msg) from e
        except Exception:
            log_msg = 'Failed to delete entity'
            logger.exception(log_msg)
            raise

    def _process_descent(self,idx: int, descendant_ent: Entity, entity_type: EntityType):
        """
        Process function for descendants, seperated to avoid repetition.
        """
        if self.descendants_flag is None:
            log_msg = 'Descendants flag not set, cannot process descendants. Ensure you have selected at least 1 option for descendants processing.'
            logger.error(log_msg)
            raise ValueError(log_msg)
        if not any(x in ["include-xml","include-retention","include-description","include-security","include-title","include-identifiers"] for x in self.descendants_flag):
            log_msg = 'No data to process. Ensure you select 1 option of data to edit'
            logger.error(log_msg)
            raise ValueError(log_msg)
        if any(x in ["include-xml","include-all"] for x in self.descendants_flag) and self.metadata_flag is not None:
            xmls = self.generate_descriptive_metadata(idx, self.xml_files)
            if xmls is not None:                                
                for x in xmls:
                    rawxnames = x.get('xnames')
                    if isinstance(rawxnames, list):
                        self.xnames = [x for x in rawxnames if isinstance(x, str)]
                    ns = list(x.keys())[0]
                    xml_new = x.get(ns)
                    if isinstance(xml_new, etree._ElementTree):
                        self.xml_update(descendant_ent, ns, xml_new)
        if any(x in ["include-identifiers","include-all"] for x in self.descendants_flag):
            self.ident_update(descendant_ent, self.ident_lookup(idx, self.IDENTIFIER_DEFAULT))
        if any(x in ["include-all","include-title","include-description","include-security"] for x in self.descendants_flag):
            if not "include-title" in self.descendants_flag:
                title = None
            if not any(x in ["include-all","include-description"] for x in self.descendants_flag):
                description = None
            if not any(x in ["include-all","include-security"] for x in self.descendants_flag):
                security = None
            self.xip_update(descendant_ent,title=title,description=description,security=security)
        if entity_type == EntityType.ASSET and self.retention_flag is True and any(x in ["include-retention","include-all"] for x in self.descendants_flag):
            self.retention_update(descendant_ent, self.retention_lookup(idx))

    def _process_descendants(self, idx: int, ent: Entity):            
        """
        Descendants handling.
        """
        if self.descendants_flag:
            if ent.entity_type == EntityType.FOLDER:
                for ent_dir in self.entity.all_descendants(ent):
                    if ent_dir.entity_type is None:
                        log_msg = f'No descendants found for entity: {ent.reference}'
                        logger.info(log_msg)
                        continue
                    logger.info(f"Processing Descendant: {ent_dir.reference}")
                    descendant_ent = self.entity.entity(ent_dir.entity_type, ent_dir.reference)
                    if "include-assets" in self.descendants_flag and descendant_ent.entity_type == EntityType.ASSET:
                        self._process_descent(idx, descendant_ent, EntityType.ASSET)
                    elif "include-folders" in self.descendants_flag and descendant_ent.entity_type == EntityType.FOLDER:
                        self._process_descent(idx, descendant_ent, EntityType.FOLDER)

    def _process_upload_mode(self) -> None:
        try:
            from preservica_modify.pres_upload import PreservicaMassUpload
        except ImportError:
            log_msg = 'Upload mode enabled but PreservicaMassUpload class not found. Please ensure you have the pres_upload module in your project and it contains the PreservicaMassUpload class.'
            logger.error(log_msg)
            raise ImportError(log_msg)
        try:
            if self.UPLOAD_TYPE in self.column_headers and self.DOCUMENT_TYPE in self.column_headers:
                logger.warning(f'Both Document Type and Upload Type columns found, defaulting to Upload Type column for reference. Using {self.UPLOAD_TYPE} as basis.')
                data_dict = self.df[[self.ENTITY_REF, self.UPLOAD_TYPE]].to_dict(orient='index')
            elif self.UPLOAD_TYPE in self.column_headers:
                logger.debug(f'Upload Type column found, using {self.UPLOAD_TYPE} as basis for upload mode.')
                data_dict = self.df[[self.ENTITY_REF, self.UPLOAD_TYPE]].to_dict(orient='index')
            elif self.DOCUMENT_TYPE in self.column_headers:
                logger.debug(f'Document Type column found, using {self.DOCUMENT_TYPE} as basis for upload mode.')
                data_dict = self.df[[self.ENTITY_REF, self.DOCUMENT_TYPE]].to_dict(orient='index')
            else:
                log_msg = 'Upload mode enabled but no Document Type or Upload Type column found. Please ensure you have a column with either Document Type or Upload Type as the header to use upload mode.'
                logger.error(log_msg)
                raise ValueError(log_msg)
        except KeyError as e:
            log_msg = f'Key Error: {e} Please ensure that the "Document Type" and "Upload Type" columns are in your spreadsheet when using upload mode.'
            logger.exception(log_msg)
            raise KeyError(log_msg) from e
        except Exception:
            log_msg = 'Error setting up upload mode'
            logger.exception(log_msg)
            raise

        try:
            last_ref = None
            keys, start_pos = self._process_continue_token(data_dict)

            for idx in keys[start_pos:]:
                reference_dict = data_dict.get(idx)
                if reference_dict is not None:
                    ref = check_nan(reference_dict.get(self.ENTITY_REF))
                else:
                    log_msg = f'No data found for Index: {idx}, skipping to next row.'
                    logger.error(log_msg)
                    continue
                if self.UPLOAD_TYPE in self.column_headers:
                    upload_type = reference_dict.get(self.UPLOAD_TYPE)
                elif self.DOCUMENT_TYPE in self.column_headers:
                    upload_type = reference_dict.get(self.DOCUMENT_TYPE)
                #Ref is the upload folder reference.
                if ref is None or ref == "Use Parent" and upload_type is not None:
                    if last_ref is None:
                        log_msg = 'No previous reference found. Please provide a reference in at least the first row.'
                        logger.error(log_msg)
                        raise ValueError(log_msg)
                    PreservicaMassUpload('placeholder', spreadsheet_path=self.input_file).main(idx, str(last_ref), str(upload_type))
                else:
                    last_ref = PreservicaMassUpload('placeholder',spreadsheet_path=self.input_file).main(idx, ref, str(upload_type))
        except KeyboardInterrupt:
            logger.warning('Process interrupted by user during upload mode, exiting...')
            if self.disable_continue is False:
                self._save_continue_token(self.input_file, idx)
            raise KeyboardInterrupt('Process interrupted by user, exiting...') from None
        except Exception:
            log_msg = 'Error in upload mode'
            logger.exception(log_msg)
            raise


    def _process_continue_token(self, data_dict: dict) -> tuple[list[int], int]:
        if self.disable_continue is True:
            start_idx = 0
        else:
            start_idx = self._load_continue_token(self.input_file)
            if not isinstance(start_idx, int):
                log_msg = f'Invalid continue token: {start_idx}, must be an integer index. Please ensure the continue token file contains a valid integer index.'
                logger.error(log_msg)
                raise ValueError(log_msg)
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

    def _process_rows(self, data_dict: dict) -> None:
        try:
            keys, start_pos = self._process_continue_token(data_dict)
            for idx in keys[start_pos:]:
                reference_dict = data_dict.get(idx)
                if reference_dict is not None:
                    ref = check_nan(reference_dict.get(self.ENTITY_REF))
                    if ref is None:
                        logger.warning(f'No reference found for index: {idx}, skipping to next row.')
                        continue
                    doc_type = check_nan(reference_dict.get(self.DOCUMENT_TYPE))
                    if doc_type is None:
                        logger.warning(f'No document type found for index: {idx}, attempting to retrieve entity without document type.')
                else:
                    log_msg = f'No data found for index: {idx}'
                    logger.error(log_msg)
                    raise ValueError(log_msg)
                logger.info(f"Processing Row Index: {idx}, Reference: {ref}")
                ent = self._process_fetch_ent(ref, doc_type)
                if ent is not None:
                    self._process_row_ent(ent, idx, reference_dict)
                else:
                    logger.warning(f'Entity not found for reference {ref}, skipping to next row.')
                    continue
        except KeyboardInterrupt:
            log_msg = 'Process interrupted by user, exiting...'
            logger.warning(log_msg)
            if self.disable_continue is False:
                self._save_continue_token(self.input_file, idx)
            raise KeyboardInterrupt(log_msg)
        except Exception:
            log_msg = f'Error processing row with index: {idx}.'
            logger.exception(log_msg)
            raise

     # Setup for Local Definition of Entity?
    def _process_fetch_ent(self, ref: str, doc_type: Optional[str]) -> Optional[Entity]:
        try:
            if doc_type is not None:
                if doc_type == "SO":
                    ent = self.entity.folder(ref)
                elif doc_type == "IO":
                    ent = self.entity.asset(ref)
                else:
                    log_msg = f'Unsupported document type for reference {ref}: {doc_type}'
                    raise ValueError(log_msg)
                return ent
            else:
                #This is a lazy lookup solution to the issue of not being able to determine if the entity is a folder or asset.
                try:
                    ent = self.entity.asset(ref)
                except Exception:
                    ent = self.entity.folder(ref)
                return ent
        except Exception as e:
            log_msg = f'Error retrieving entity with reference {ref}: {e}, skipping to next row.'
            logger.warning(log_msg)
            return None

    # Instead of using lookups, can reference_dict be used directly?
    def _process_row_ent(self, ent: Entity, idx: int, reference_dict: Optional[dict] = None) -> None:
        if self.delete_flag is True:
            delete_check = self.delete_update(idx, ent)
            if delete_check is True:
                return
        if any([self.title_flag, self.description_flag, self.security_flag]) is True:
            title, description, security = self.xip_lookup(idx)
            self.xip_update(ent,title,description,security)
        self.ident_update(ent, self.ident_lookup(idx, self.IDENTIFIER_DEFAULT))
        if self.metadata_flag is not None:
            xmls = self.generate_descriptive_metadata(idx, self.xml_files)
            if xmls is not None:                                
                for x in xmls:
                    rawxnames = x.get('xnames')
                    if isinstance(rawxnames, list):
                        self.xnames = [x for x in rawxnames if isinstance(x, str)]
                    ns = list(x.keys())[0]
                    if not isinstance(ns, str):
                        log_msg = f'Invalid namespace retrieved for index {idx}, expected string but got {type(ns)}. Skipping XML update for this file.'
                        logger.warning(log_msg)
                        continue
                    xml_new = x.get(ns)
                    if not isinstance(xml_new, etree._ElementTree):
                        log_msg = f'Invalid XML data retrieved for index {idx}, expected etree._ElementTree but got {type(xml_new)}. Skipping XML update for this file.'
                        logger.warning(log_msg)
                        continue
                    self.xml_update(ent, ns, xml_new)
        if ent.entity_type == EntityType.ASSET and self.retention_flag is True:
            self.retention_update(ent, self.retention_lookup(idx))
        self.move_update(idx, ent)
        self._process_descendants(idx, ent)

    def main(self):
        """
        Main loop.
        """
        try:
            self.init_df()
            self._set_input_flags()
            self.login_preservica()
            if self.metadata_flag is not None:
                self.init_generate_descriptive_metadata()
            if self.retention_flag is True:
                self.get_retentions()
            if self.upload_flag is True:
                self._process_upload_mode()
                self._remove_continue_token(self.input_file)
                return
            if self.DOCUMENT_TYPE in self.column_headers:
                data_dict = self.df[[self.ENTITY_REF, self.DOCUMENT_TYPE]].to_dict(orient='index')
            else:
                data_dict = self.df[[self.ENTITY_REF]].to_dict(orient='index')
            self._process_rows(data_dict)
            self._remove_continue_token(self.input_file)
            logger.info('Process completed.')
        except KeyError as e:
            log_msg = f'Key Error: {e}.'
            logger.exception(log_msg)
            raise
        except ValueError as e:
            log_msg = f'Value Error: {e}.'
            logger.exception(log_msg)
            raise
        except Exception:
            log_msg = 'Error in main loop'
            logger.exception(log_msg)
            raise