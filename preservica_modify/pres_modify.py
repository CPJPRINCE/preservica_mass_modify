"""
Preservica Mass Modify Tool

This tool is utilised to modify existing data on Preservica through the use of spreadsheets.
Allows for modification of XIP and XML data.

Author: Christopher Prince
license: Apache License 2.0"
"""

from pyPreservica import EntityAPI, RetentionAPI, UploadAPI, WorkflowAPI, AdminAPI, Entity, EntityType
import pandas as pd
from pandas.api.types import is_datetime64_dtype
from lxml import etree
from datetime import datetime
import os, re
from .common import AuthenticatedPreservicaClient, check_nan, check_bool, export_csv, export_json, export_xml, export_xl, export_ods
from typing import Optional, Union, Dict, List, Hashable, Any
import logging
import configparser

logger = logging.getLogger(__name__)

class PreservicaMassMod(AuthenticatedPreservicaClient):
    """
    Mass Modification Class
    """
    def __init__(self,
                 input_file: str,
                 metadata_dir: str = os.path.join(os.path.dirname(__file__), "metadata"),
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
                 column_sensitivity: bool = False,
                 options_file: str = os.path.join(os.path.dirname(__file__),'options', 'options.properties')):
        super().__init__(username=username,
                         password=password,
                         manager_username=manager_username,
                         manager_password=manager_password,
                         server=server,
                         tenant=tenant,
                         credentials=credentials,
                         use_keyring=use_keyring,
                         keyring_service=keyring_service,
                         save_password_to_keyring=save_password_to_keyring)
        
        self.metadata_dir = metadata_dir
        self.metadata_flag = metadata
        self.dummy_flag = dummy
        self.blank_override = blank_override
        self.delete_flag = delete
        self.descendants_flag = descendants
        
        self.input_file = input_file
        
        self.column_sensitivity = column_sensitivity

        if options_file is None:
            options_file = os.path.join(os.path.dirname(__file__),'options','options.properties')
        self.parse_config(options_file=os.path.abspath(options_file), column_sensitivity=self.column_sensitivity)

        self.xnames: list[str] = []

    def parse_config(self, options_file: str, column_sensitivity: bool = False) -> None:
        config = configparser.ConfigParser()
        read_config = config.read(options_file, encoding='utf-8')
        if not read_config:
            logger.warning(f"Options file not found or not readable: {options_file}. Using defaults.")

        section = config['options'] if 'options' in config else {}

        if column_sensitivity:
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
                        elem_ln_path = elem_path.replace(f"{{{elem.namespace}}}", root_element_ln + ":")
                        print(elem_ln_path)
        except Exception:
            logger.exception('Failed to print Descriptive metadata files, ensure correct path')
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
        except Exception:
            logger.exception('Failed to print Descriptive metadta files, ensure correct path')
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
                        elem_ln_path = elem_path.replace(f"{{{elem.namespace}}}", root_element_ln + ":")
                        column_list.append(elem_ln_path)
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
        except Exception:
            logger.exception('Failed to print Descriptive metadata files, ensure correct path')
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
                        elem_ln_path = elem_path.replace(f"{{{elem.namespace}}}", root_element_ln + ":")
                        column_list.append(elem_ln_path)
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
        except Exception:
            logger.exception('Failed to print Descriptive metadta files, ensure correct path')
            raise

    def get_retentions(self) -> list[dict]:
        """
        Retrieves retention policies from Preservica and parses them into a dict
        """
        self.policies = self.retention.policies()
        self.policy_dict = [{"Name": p.name, "Reference": p.reference} for p in self.policies]
        logger.info(f'Retention Policies retrieved')
        logger.debug(f'Retention Policies obtained: {self.policy_dict}')
        return self.policy_dict

    def init_generate_descriptive_metadata(self) -> List[Dict[str, Any]]:
        """
        Initialises the descriptive metadata by parsing the XML files in the metadata directory, generating a list of the elements and their namespaces, and comparing them against the column headers in the spreadsheet to filter out non-matching data.
        The resulting list of matching elements is stored in self.xml_files for use in generating the descriptive metadata in the Opex manifest.
        """
        try:
            self.xml_files: List[Dict[str, Any]] = []
            for file in os.scandir(self.metadata_dir):
                list_xml: List[Dict[str,Any]] = []
                if file.name.endswith('.xml'):
                    path = os.path.join(self.metadata_dir, file.name)
                    """
                    Generates info on the elements of the XML Files placed in the Metadata directory.
                    Composed as a list of dictionaries.
                    """
                    try:
                        xml_file = etree.parse(path)
                    except etree.XMLSyntaxError as e:
                        logger.exception(f'XML Syntax Error while parsing {file.name}: {e}')
                        raise
                    except FileNotFoundError as e:
                        logger.exception(f'XML file not found {file.name}: {e}')
                        raise
                    root_element = etree.QName(xml_file.find('.'))
                    root_element_ln = root_element.localname
                    root_element_ns = root_element.namespace
                    elements_list = []
                    for elem in xml_file.findall('.//'):
                        elem_path = xml_file.getelementpath(elem)
                        elem = etree.QName(elem)
                        elem_ln = elem.localname
                        elem_ns = elem.namespace
                        elem_ln_path = elem_path.replace(f"{{{elem_ns}}}", root_element_ln + ":")
                        elements_list.append({"Name": root_element_ln + ":" + elem_ln, "XName": f"{{{elem_ns}}}{elem_ln}", "Namespace": elem_ns, "Path": elem_ln_path})
                    
                    """
                    Compares the column headers in the Spreadsheet against the headers. Filters out non-matching data.
                    """
                    try:
                        for elem_dict in elements_list:
                            if elem_dict.get('Name') in self.column_headers or elem_dict.get('Path') in self.column_headers:
                                list_xml.append({"Name": elem_dict.get('Name'), "XName": elem_dict.get('XName'), "Namespace": elem_dict.get('Namespace'), "Path": elem_dict.get('Path')})
                    except KeyError as e:
                        logger.exception(f'Key Error comparing XML elements to column headers for file {file.name}: {e}')
                        raise
                    except IndexError as e:
                        logger.warning(f'Index Error comparing XML elements to column headers for file {file.name}: {e}')
                    except Exception:
                        logger.exception(f'Error comparing XML elements to column headers for file {file.name}')
                        raise
                if len(list_xml) > 0:
                    self.xml_files.append({'data': list_xml, 'local_name': root_element_ln, 'local_ns': root_element_ns, 'xml_file': path})
                    logger.info(f'Matching columns found in spreadsheet for XML file: {file.name}, added to metadata generation list.')
                else:
                    logger.debug(f'No matching columns found in spreadsheet for XML file: {file.name}, skipping this file for metadata generation.')
            return self.xml_files
        except FileNotFoundError:
            logger.exception(f'Metadata directory not found at path: {self.metadata_dir}')
            raise
        except Exception:
            logger.exception('Failed to intialise XML Metadata')
            raise

    def _process_descent(self, idx: int, descendant_ent: Entity, entity_type: EntityType):
        """
        Process function for descendants, separated to avoid repetition.
        """
        if self.descendants_flag is None:
            logger.error('Descendants flag not set, cannot process descendants. Ensure you have selected at least 1 option for descendants processing.')
            raise ValueError('Descendants flag not set, cannot process descendants. Ensure you have selected at least 1 option for descendants processing.')
        if not any(x in ["include-xml","include-retention","include-description","include-security","include-title","include-identifiers"] for x in self.descendants_flag):
            logger.error('No data to process. Ensure you select 1 option of data to edit')
            raise ValueError('No data to process. Ensure you select 1 option of data to edit')
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
                        logger.info(f'No descendants found for entity: {ent.reference}')
                        continue
                    logger.info(f"Processing Descendant: {ent_dir.reference}")
                    descendant_ent = self.entity.entity(ent_dir.entity_type, ent_dir.reference)
                    if "include-assets" in self.descendants_flag and descendant_ent.entity_type == EntityType.ASSET:
                        self._process_descent(idx, descendant_ent, EntityType.ASSET)
                    elif "include-folders" in self.descendants_flag and descendant_ent.entity_type == EntityType.FOLDER:
                        self._process_descent(idx, descendant_ent, EntityType.FOLDER)

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
                    logger.error(f'No data found for index: {idx}')
                    raise ValueError(f'No data found for index: {idx}')
                logger.info(f"Processing Row Index: {idx}, Reference: {ref}")
                ent = self._process_fetch_ent(ref, doc_type)
                if ent is not None:
                    self._process_row_ent(ent, idx, reference_dict)
                else:
                    logger.warning(f'Entity not found for reference {ref}, skipping to next row.')
                    continue
        except KeyboardInterrupt:
            logger.warning('Process interrupted by user, exiting...')
            if self.disable_continue is False:
                self._save_continue_token(self.input_file, idx)
            raise KeyboardInterrupt('Process interrupted by user, exiting...')
        except Exception:
            logger.exception('Error processing rows.')
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
                    raise ValueError(f'Unsupported document type for reference {ref}: {doc_type}')
                return ent
            else:
                #This is a lazy lookup solution to the issue of not being able to determine if the entity is a folder or asset.
                try:
                    ent = self.entity.asset(ref)
                except Exception:
                    ent = self.entity.folder(ref)
                return ent
        except Exception as e:
            logger.warning(f'Error retrieving entity with reference {ref}: {e}, skipping to next row.')
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
                        logger.warning(f'Invalid namespace retrieved for index {idx}, expected string but got {type(ns)}. Skipping XML update for this file.')
                        continue
                    xml_new = x.get(ns)
                    if not isinstance(xml_new, etree._ElementTree):
                        logger.warning(f'Invalid XML data retrieved for index {idx}, expected etree._ElementTree but got {type(xml_new)}. Skipping XML update for this file.')
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
            if self.DOCUMENT_TYPE in self.column_headers:
                data_dict = self.df[[self.ENTITY_REF, self.DOCUMENT_TYPE]].to_dict(orient='index')
            else:
                data_dict = self.df[[self.ENTITY_REF]].to_dict(orient='index')
            self._process_rows(data_dict)
            self._remove_continue_token(self.input_file)
            logger.info('Process completed.')
        except KeyError as e:
            logger.exception(f'Key Error: {e}.')
            raise
        except ValueError as e:
            logger.exception(f'Value Error: {e}.')
            raise
        except Exception:
            logger.exception('Error in main loop')
            raise