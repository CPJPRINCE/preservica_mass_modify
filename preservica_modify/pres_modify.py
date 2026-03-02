"""
Preservica Mass Modification tool.

This tool is utilised to modify existing data on Preservica through the use of spreadsheets.
Allows for modification of XIP and XML data.

Author: Christopher Prince
license: Apache License 2.0"
"""


from pyPreservica import EntityAPI, RetentionAPI, UploadAPI, WorkflowAPI, Entity, EntityType
import pandas as pd
from lxml import etree
from datetime import datetime
import time, os, re
from preservica_modify.common import check_nan, check_bool
from typing import Optional, Union, Dict, List, Hashable, Any, Mapping
import logging
import configparser

logger = logging.getLogger(__name__)

class PreservicaMassMod:
    """
    Mass Modification Class
    """
    def __init__(self,
                 input_file: str,
                 metadata_dir: str,
                 blank_override: bool = False,
                 upload_mode: bool = False,
                 xml_method: str = "flat",
                 descendants: Optional[set] = None,
                 dummy: bool = False,
                 username: Optional[str] = None,
                 password: Optional[str] = None,
                 server: Optional[str] = None,
                 tenant: Optional[str] = None,
                 credentials: str = os.path.join(os.getcwd(),"credentials.properties"),
                 delete: bool = False,
                 options_file: str = os.path.join(os.path.dirname(__file__),'options.properties')):
        
        self.metadata_dir = metadata_dir
        self.metadata_flag = xml_method
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
        self.username = username
        self.password = password
        self.server = server
        self.tenant = tenant
        self.login_preservica()
        self.input_file = input_file
        if input_file.endswith(".xlsx"):
            self.df: pd.DataFrame = pd.read_excel(input_file)
        elif input_file.endswith(".csv"):
            self.df: pd.DataFrame = pd.read_csv(input_file)
        self.column_headers = list(self.df.columns.values)
        date_headers = [header for header in self.column_headers if "date" in str(header).lower()]
        self.df[date_headers] = self.df[date_headers].apply(lambda x: pd.to_datetime(x,format='mixed'))

        if options_file is None:
            options_file = os.path.join(os.path.dirname(__file__),'options','options.properties')
        self.parse_config(options_file=os.path.abspath(options_file))

    def parse_config(self, options_file: str) -> None:
        config = configparser.ConfigParser()
        read_config = config.read(options_file, encoding='utf-8')
        if not read_config:
            logger.warning(f"Options file not found or not readable: {options_file}. Using defaults.")

        section = config['options'] if 'options' in config else {}

        self.ENTITY_REF=section.get('ENTITY_REF', 'Entity Ref')
        self.DOCUMENT_TYPE=section.get('DOCUMENT_TYPE', 'Document type')
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
        self.ACCREF_FIELD=section.get('ACCREFF_FIELD', 'Accession_Reference')
        self.ACCREF_CODE=section.get('ACCREFF_CODE', 'accref')

        logger.debug(f'Configuration loaded: {section}')

    def login_preservica(self):
        """
        Logs into Preservica. Either through manually logging in.
        """
        try:
            if any(v is None for v in (self.username,self.password,self.server)):
                logger.exception('A Username, Password or Server has not been provided... Please try again...')
                raise Exception('A Username, Password or Server has not been provided... Please try again...')
            if self.credentials_file:
                logger.info('Using credentials file.')
                self.entity = EntityAPI(credentials_path=self.credentials_file)
                self.retention = RetentionAPI(credentials_path=self.credentials_file)
                self.upload = UploadAPI(credentials_path=self.credentials_file)
                self.workflow = WorkflowAPI(credentials_path=self.credentials_file)
                logger.info(f'Successfully logged into Preservica, as user: {self.username}')
            elif None in (self.username,self.password,self.server):
                logger.exception('A Username, Password or Server has not been provided... Please try again...')
                raise Exception('A Username, Password or Server has not been provided... Please try again...')
            else:
                self.entity = EntityAPI(username=str(self.username), password=str(self.password),server=str(self.server), tenant=str(self.tenant))
                self.retention = RetentionAPI(username=str(self.username), password=str(self.password),server=str(self.server), tenant=str(self.tenant))
                self.upload = UploadAPI(username=str(self.username), password=str(self.password),server=str(self.server), tenant=str(self.tenant))
                self.workflow = WorkflowAPI(username=str(self.username), password=str(self.password),server=str(self.server), tenant=str(self.tenant))
                logger.info(f'Successfully logged into Preservica, as user {self.username}')
        except Exception as e: 
            logger.exception('Failed to login to Preservica...')
            raise Exception(f'Failed to login to Preservica, error: {e}')

    def set_input_flags(self) -> None:
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

    def get_retentions(self) -> list[dict]:
        """
        Retrieves retention policies from Preservica and parses them into a dict
        """
        self.policies = self.retention.policies()
        self.policy_dict = [{"Name": p.name, "Reference": p.reference} for p in self.policies.get_results()]
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
    
    def ident_lookup(self, idx: Hashable, default_key: Optional[str] = None) -> Optional[dict]:
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
            logger.exception(f'Key Error for Identifier Lookup, missing Column: {e} please ensure column header\'s are an exact match...')
            raise KeyError(f'Key Error for Identifier Lookup, missing Column please ensure column header\'s are an exact match...') from e
        except IndexError as e:
            logger.warning(f"Index Error for Identifier Lookup: {e} it is likely you have removed or added a file/folder to the directory"
                         "after generating the spreadsheet. An opex will still be generated but with no identifiers. To ensure identifiers match up please ensure match up...")
        except Exception as e:
            logger.exception(f'Identifier Lookup failed: for {idx}, error: {e}')
            raise Exception(f'Identifier Lookup failed: for {idx}') from e

    def xip_lookup(self, idx: Hashable) -> tuple[Optional[str], Optional[str], Optional[str]]:
        """
        Uses the pandas index to retreieve data from the "Title, Description and Security" columns. Sets data in Entity.

        Has Override variables to use an override, mainly for applying to descdendants.

        :param idx: Pandas Index to lookup
        :param e: Entity to act upon
        :param title_override: Set Override for title - main use for descendants  
        :param description_override: Set Override for description - main use for descendants  
        :param title_override: Set Override for security - main use for descendants  
        """
        title: Optional[str] = None
        description: Optional[str] = None
        security: Optional[str] = None
    
        try:
            if self.title_flag:
                title = check_nan(self._cell(idx, self.TITLE_FIELD))
                logger.debug(f'XIP Lookup Title: {title}')
            if self.description_flag:
                description = check_nan(self._cell(idx, self.DESCRIPTION_FIELD))
                if description is None and self.blank_override is True:
                    description = None
                logger.debug(f'XIP Lookup Description: {description}')
            if self.security_flag:
                security = check_nan(self._cell(idx, self.SECURITY_FIELD))
                logger.debug(f'XIP Lookup Security: {security}')
            return title, description, security
        except KeyError as e:
            logger.exception(f'Key Error XIP Lookup, missing Column: {e} please ensure column header\'s are an exact match...')
            raise KeyError(f'Key Error XIP Lookup, missing Column please ensure column header\'s are an exact match...') from e
        except IndexError as e:
            logger.warning(f"Index Error for XIP Lookup: {e} it is likely you have removed or added a file/folder to the directory"
                         "after generating the spreadsheet. An opex will still be generated but with no identifiers. To ensure identifiers match up please ensure match up...")
        except Exception as e:
            logger.exception(f'Retention XIP failed: for {idx}, error: {e}')
            raise Exception(f'Retention XIP failed: for {idx}') from e
        return title, description, security

    def pax_lookup(self, idx: Hashable) -> Optional[tuple[Optional[str], Optional[list], Optional[list]]]:
        try:
            pax_path = check_nan(self._cell(idx, self.PAX_PATH))
            pax_dict: Dict[Hashable, Dict[str,Any]] = self.df.loc[self.df[self.PAX_PATH] == pax_path, [self.FILE_PATH,self.PAX_PRES_FIELD,self.PAX_ACCESS_FIELD]].to_dict(orient='index')
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
            logger.exception(f'Key Error for Pax Lookup, missing Column: {e} please ensure column header\'s are an exact match...')
            raise KeyError(f'Key Error for Pax Lookup, missing Column please ensure column header\'s are an exact match...') from e
        except IndexError as e:
            logger.warning(f"Index Error for Pax Lookup: {e} it is likely you have removed or added a file/folder to the directory"
                         "after generating the spreadsheet. An opex will still be generated but with no identifiers. To ensure identifiers match up please ensure match up...")
        except Exception as e:
            logger.exception(f'Pax Lookup failed: for {idx}, error: {e}')
            raise Exception(f'Pax Lookup failed: for {idx}') from e
        
    def retention_lookup(self, idx: Hashable):
        """
        Uses the pandas index to retreieve data from the "Retention Policy" column

        Matched agasint the policies obtained in the obtain_retentions function.

        :param idx: Pandas Index to lookup
        :param e: Entity to act upon
        """
        try:
            if self.retention_flag:
                rp = check_nan(self._cell(idx,self.RETENTION_FIELD))
            else:
                rp = None
            logger.debug(f'Retention Lookup for {idx}: {rp}')    
            return rp
        except KeyError as e:
            logger.exception(f'Key Error Retention Lookup, missing Column: {e} please ensure column header\'s are an exact match...')
            raise KeyError(f'Key Error Retention Lookup, missing Column please ensure column header\'s are an exact match...') from e
        except IndexError as e:
            logger.warning(f"Index Error for Retention Lookup: {e} it is likely you have removed or added a file/folder to the directory"
                         "after generating the spreadsheet. An opex will still be generated but with no identifiers. To ensure identifiers match up please ensure match up...")
        except Exception as e:
            logger.exception(f'Retention Lookup failed: for {idx}, error: {e}')
            raise Exception(f'Retention Lookup failed: for {idx}') from e

    def delete_lookup(self, idx: Hashable):
        """
        Uses the pandas index to retrieve data from the "Delete" column. If True intitaites a Delete.
        Requires use of a .credentials file.

        Delete Flag must also be set.
        """
        try:
            if self.delete_flag is True:
                delete_conf = self._cell(idx, self.DELETE_FIELD)
                return bool(check_nan(delete_conf))
            else:
                return False
        except Exception as e:
            logger.exception(f'Failed to Lookup Delete, Error: {e}')
            raise Exception(f'Failed to Lookup Delete, Error: {e}')

    def init_generate_descriptive_metadata(self) -> None:
        """
        Initiation for the generate_descriptive_metadata function. Seperated to avoid unecessary repetition.

        First takes xmls files in metadata_dir, generates a list of dicts of the elements in XML file. Then compares the Column headers in the spreadsheet against the XML's in the Metadata Directory.
        """
        self.xml_files: List[Dict[str, Any]] = []
        for file in os.scandir(self.metadata_dir):
            list_xml: List[Dict[str,Any]] = []
            if file.name.endswith('.xml'):
                path = os.path.join(self.metadata_dir, file)
                xml_file = etree.parse(path)
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
                for elem_dict in elements_list:
                    if elem_dict.get('Name') in self.column_headers or elem_dict.get('Path') in self.column_headers:
                        list_xml.append({"Name": elem_dict.get('Name'), "XName": elem_dict.get('XName'), "Namespace": elem_dict.get('Namespace'), "Path": elem_dict.get('Path')})
            if len(list_xml) > 0:
                self.xml_files.append({'data': list_xml, 'localname': root_element_ln, 'localns': root_element_ns, 'xmlfile': path})
 
    def generate_descriptive_metadata(self, idx: Hashable, xml_file: Mapping[str,Any]) -> Optional[etree._ElementOrTree]:
        """
        Generates the xml file based on the returned list of xml_files from the init_generate_descriptive_metadata function.

        :param idx: Pandas Index to lookup
        :param xml_file: Dictionary of XML files created as part of init 
        """
        list_xml = xml_file.get('data')
        localname = xml_file.get('localname')
        assert isinstance(list_xml, list)
        assert isinstance(localname, str)
        if list_xml is None:
            logger.warning(f'No XML elements found for {xml_file.get("xmlfile")}, skipping XML generation for this file.')
            return None
        if len(list_xml) > 0:
            xml_new = etree.parse(str(xml_file.get('xmlfile')))
            for elem_dict in list_xml:
                assert isinstance(elem_dict, dict)
                name = elem_dict.get('Name')
                path = elem_dict.get('Path')
                assert isinstance(path, str)
                assert isinstance(name, str)
                ns = elem_dict.get('Namespace')
                try:
                    if self.metadata_flag in {'e', 'exact'}:
                        val = check_nan(self._cell(idx, path))
                    elif self.metadata_flag in {'f', 'flat'}:
                        val = check_nan(self._cell(idx, name))
                    if pd.isnull(val) or val is None:
                        continue
                    else:
                        if pd.api.types.is_datetime64_dtype(val):
                            val = pd.to_datetime(val)
                            val = datetime.strftime(val, "%Y-%m-%dT%H:%M:%S.000Z")
                    if self.metadata_flag in {'e','exact'}:
                        n = path.replace(localname + ":", f"{{{ns}}}")
                        elem = xml_new.find(f'./{n}')
                    elif self.metadata_flag in {'f', 'flat'}:
                        n = name.split(':')[-1]
                        elem = xml_new.find(f'.//{{{ns}}}{n}')
                    if elem is not None:
                        elem.text = str(val)
                    else:
                        logger.warning(f'Element not found in XML for {name, path}')
                except KeyError as e:
                    logger.exception(f'Key Error, missing Column: {e} please ensure column header\'s are an exact match...')
                    raise KeyError(f'Key Error, missing Column: {e} please ensure column header\'s are an exact match...')
                except IndexError as e:
                    logger.warning(f"Index Error: {e} it is likely you have removed or added a file/folder to the directory" \
                        "after generating the spreadsheet. An opex will still be generated but with no xml metadata." \
                        "To ensure metadata match up please ensure match up...")
                    break
                except Exception as e:
                    logger.exception(f'Error generating descriptive metadata for {name, path}: {e}')
                    raise Exception(f'Error generating descriptive metadata for {name, path}') from e
            self.xml_new = xml_new
            return xml_new

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
        except Exception as e:
            logger.exception(f'Error updating XIP Metadata: {e}')
            raise Exception(f'Error updating XIP Metadata: {e}')
        
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
        except Exception as e:
            logger.exception(f'Error updating Identifiers: {e}')
            raise Exception(f'Error updating Identifiers')
                    
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
                    assert policy
                    if any(assignments):
                        for ass in assignments:
                            logger.info(f"Updating {ent.reference} Removing retention policy: {ass.policy_reference}")
                            if self.dummy_flag is False:
                                self.retention.remove_assignments(ass)
                    logger.info(f"Updating {ent.reference} Adding retention policy: {policy, policy_name}")
                    if self.dummy_flag is False:
                        self.retention.add_assignments(ent,self.retention.policy(policy))
                elif len(policies) == 0:
                    policy = policies[0].get('Reference', None)
                    policy_name = policies[0].get('Name', None)
                    assert policy
                    logger.info(f'Updating {ent.reference} Adding retention policy: {policy, policy_name}')
                    if self.dummy_flag is False:
                        self.retention.add_assignments(ent, self.retention.policy(policy))

            elif retention_policy is None and self.blank_override is True:
                assignments = self.retention.assignments(ent)
                if any(assignments):
                    for ass in assignments:
                        logger.info(f"Updating {ent.reference} Removing Retention Policy: {ass.policy_reference}")
                        if self.dummy_flag is False:
                            self.retention.remove_assignments(ass)
            else:
                pass                    
        except Exception as e:
            logger.exception(f'Error updating Retention: {ent.reference} Error: {e}')
            raise Exception(f'Error updating Retention: {ent.reference}') from e
                    
    def xml_update(self, ent: Entity, ns: str, xml_new: etree._ElementOrTree):
        """
        Makes the call on Preservica's API using pyPreservica to update, remove or add metadata from given entity.

        The namespace must also be passed - this is retrieved automatically. 

        :param e: Entity to act upon
        :param ns: Namespace of XML being updated
        """
        try:
            if self.upload_flag:
                emeta = None
            else:
                emeta = self.entity.metadata_for_entity(ent, ns)
            # Check if metadata exists for the entity
            if emeta is None:
                xml_to_upload = etree.tostring(xml_new)
                logger.info(f"Updating {ent.reference} Adding Metadata")
                logger.debug(f'New XML Metadata: {xml_to_upload}')
                if self.dummy_flag is False:
                    self.entity.add_metadata(ent, ns, xml_to_upload.decode('utf-8'))
            # Metadata exists, merge and update
            else:
                xml_to_upload = etree.tostring(self.xml_merge(etree.fromstring(emeta), xml_new))
                logger.info(f"Updating {ent.reference} Updating Metadata")
                logger.debug(f'Updated XML Metadata: {xml_to_upload}')
                if self.dummy_flag is False:
                    self.entity.update_metadata(ent, ns, xml_to_upload.decode('utf-8'))
        except Exception as e:
            logger.exception(f'Error updating XML Metadata: {e}')
            raise Exception(f'Error updating XML Metadata: {e}')

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
                    logger.exception(f' Reference: {ent.reference} in "Move To" is formatted incorrectly: {dest}')
                    raise Exception(f' Invalid UUID format for Move To: {dest}')


    def delete_update(self, idx: Hashable, ent: Entity):
        """
        Uses the pandas index to retrieve data from the "Delete" column. If True intitaites a Delete.
        Requires use of a .credentials file.

        Delete Flag must also be set.
        """
        try:
            if self.delete_flag is True:
                delete_conf = self.delete_lookup(idx)
                if delete_conf is True and self.credentials_file is not None:
                    if ent.entity_type == EntityType.ASSET:
                        logger.info(f'Deleting Asset: {ent.reference}')
                        if self.dummy_flag is False:
                            self.entity.delete_asset(self.entity.asset(ent.reference),"Deleted by Preservica Mass Modify","Deleted by Preservica Mass Modify", self.credentials_file)
                    elif ent.entity_type == EntityType.FOLDER:
                        logger.info(f'Deleting Folder: {ent.reference}')
                        if self.dummy_flag is False:
                            self.entity.delete_folder(self.entity.folder(ent.reference),"Deleted by Preservica Mass Modify","Deleted by Preservica Mass Modify", self.credentials_file)
        except Exception as e:
            logger.exception(f'Failed to Delete, Error: {e}')
            raise Exception(f'Failed to Delete, Error: {e}')

    def descendant_process(self, idx: int, ent: Entity):            
        """
        Descendants handling
        """
        if self.descendants_flag:
            if ent.entity_type == EntityType.FOLDER:
                for ent_dir in self.entity.all_descendants(ent):
                    assert ent_dir is Entity
                    logger.info(f"Processing Descendant: {ent_dir.reference}")
                    descendant_ent = self.entity.entity(ent_dir.entity_type, ent_dir.reference)
                    if "include-assets" in self.descendants_flag and descendant_ent.entity_type == EntityType.ASSET:
                        if not any(x in ["include-xml","include-retention","include-description","include-security","include-title","include-identifiers"] for x in self.descendants_flag):
                            logger.info('No data to process. Ensure you select 1 option of data to edit')
                        if any(x in ["include-xml","include-all"] for x in self.descendants_flag):
                            for xml in self.xml_files:
                                xml_new = self.generate_descriptive_metadata(idx, xml)
                                assert isinstance(xml_new, etree._ElementTree)
                                ns = xml.get('localns')
                                assert isinstance(ns, str)
                                self.xml_update(descendant_ent, ns, xml_new)
                        if any(x in ["include-identifiers","include-all"] for x in self.descendants_flag):
                            self.ident_update(descendant_ent, self.ident_lookup(idx, self.IDENTIFIER_DEFAULT))
                        if any(x in ["include-retention","include-all"] for x in self.descendants_flag):
                            self.retention_update(descendant_ent, self.retention_lookup(idx))
                        if any(x in ["include-all","include-title","include-description","include-security"] for x in self.descendants_flag):
                            if not "include-title" in self.descendants_flag:
                                title = None
                            if not any(x in ["include-all","include-description"] for x in self.descendants_flag):
                                description = None
                            if not any(x in ["include-all","include-security"] for x in self.descendants_flag):
                                security = None
                            self.xip_update(descendant_ent,title=title,description=description,security=security)
                    elif "include-folders" in self.descendants_flag and descendant_ent.entity_type == EntityType.FOLDER:
                        if not any(x in ["include-xml","include-retention","include-description","include-security","include-title","include-identifiers"] for x in self.descendants_flag):
                            logger.exception('No data to process. Ensure you select 1 option of data to edit')
                            raise Exception('No data to process. Ensure you select 1 option of data to edit')
                        if any(x in ["include-xml","include-all"] for x in self.descendants_flag):
                            for xml in self.xml_files:
                                xml_new = self.generate_descriptive_metadata(idx, xml)
                                assert isinstance(xml_new, etree._ElementTree)
                                ns = xml.get('localns')
                                assert isinstance(ns, str)
                                self.xml_update(descendant_ent, ns, xml_new)
                        if any(x in ["include-identifiers","include-all"] for x in self.descendants_flag):
                            self.ident_update(descendant_ent, self.ident_lookup(idx,self.IDENTIFIER_DEFAULT))
                        if any(x in ["include-title","include-description","include-security"] for x in self.descendants_flag):
                            if not "include-title" in self.descendants_flag:
                                title = None
                            if not any(x in ["include-all","include-description"] for x in self.descendants_flag):
                                description = None
                            if not any(x in ["include-all","include-security"] for x in self.descendants_flag):
                                security = None
                            self.xip_update(descendant_ent,title=title,description=description,security=security)

    def main(self):
        """
        Main loop.
        """
        self.set_input_flags()
        self.init_generate_descriptive_metadata()
        if self.retention_flag is True:
            self.get_retentions()

        # Expand out the formatting of columns to allow Entity to be determined locally.
        if self.DOCUMENT_TYPE in self.column_headers or self.upload_flag is True:
            try:
                data_dict = self.df[[self.ENTITY_REF, self.DOCUMENT_TYPE]].to_dict(orient='index')
                last_ref = None                
            except KeyError as e:
                logger.exception(f'Key Error: {e} Please ensure that the "Document Type" column is in your spreadsheet.')
                raise KeyError(f'Key Error: {e} Please ensure that the "Document Type" column is in your spreadsheet.')
        else:
            data_dict = self.df.to_dict(oreint='index')

        for idx in data_dict:
            assert isinstance(idx, int)
            reference_dict = data_dict.get(idx)
            if reference_dict is not None:
                ref = check_nan(reference_dict.get(self.ENTITY_REF))
            else:
                logger.exception(f'No data found for index: {idx}')
                raise Exception(f'No data found for index: {idx}')
            
            logger.info(f"Processing: {ref}")
            if self.upload_flag is True:
                from preservica_modify.upload_mode import PreservicaUploadMode
                doc_type = reference_dict.get(self.DOCUMENT_TYPE)

                #Ref is the upload folder reference.
                if ref is None or ref == "Use Parent" and doc_type is not None:
                    if last_ref is None:
                        logger.warning('No previous reference found. Please provide a reference in atleast the first row.')
                        raise Exception('No previous reference found. Please provide a reference in atleast the first row.')
                    PreservicaUploadMode('placeholder', spreadsheet_path=self.input_file).upload_mode(idx, str(last_ref), str(doc_type))
                    time.sleep(1)
                else:
                    last_ref = PreservicaUploadMode('placeholder',spreadsheet_path=self.input_file).upload_mode(idx, ref, str(doc_type))
                continue            
            elif self.DOCUMENT_TYPE in reference_dict:
                doc_type = reference_dict.get(self.DOCUMENT_TYPE)
                try:
                    if doc_type == "SO":
                        ent = self.entity.folder(str(ref))
                    elif doc_type == "IO":
                        ent = self.entity.asset(str(ref))
                except Exception as e:
                    logger.warning(f'Error retrieving entity with reference {ref}: {e}, skipping to next row.')
                    continue
            else:
                try: 
                    #This is a lazy solution to the issue of not being able to determine if the entity is a folder or asset.
                    #It will try to get the entity as an asset, if it fails it will try to get it as a folder.
                    try:
                        ent = self.entity.asset(str(ref))
                    except:
                        ent = self.entity.folder(str(ref))
                except Exception as e:
                    logger.warning(f'Error retrieving entity with reference {ref}: {e}, skipping to next row.')
                    continue
            if self.delete_flag is True:
                self.delete_update(idx, ent)
                continue
            if any([self.title_flag, self.description_flag, self.security_flag]) is True:
                title, description, security = self.xip_lookup(idx)
                self.xip_update(ent,title,description,security)
            self.ident_update(ent, self.ident_lookup(idx, self.IDENTIFIER_DEFAULT))
            for xml in self.xml_files:
                xml_new = self.generate_descriptive_metadata(idx, xml)
                assert isinstance(xml_new, etree._ElementTree)
                ns = xml.get('localns')
                assert isinstance(ns, str)
                xml_tmp = xml.get('data')
                self.xnames = [x.get('XName') for x in xml_tmp if isinstance(x, Dict)] if xml_tmp is not None else []
                self.xml_update(ent, ns, xml_new)
            if ent.entity_type == EntityType.ASSET and self.retention_flag is True:
                self.retention_update(ent, self.retention_lookup(idx))
            self.move_update(idx, ent)
            self.descendant_process(idx, ent)