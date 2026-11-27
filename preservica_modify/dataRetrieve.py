import os
import pandas as pd
from lxml import etree
import logging
from typing import Optional
from preservica_modify.common import export_csv, export_json, export_xl, export_ods, export_xml
from preservica_modify.auth import PreservicaAuth

logger = logging.getLogger(__name__)

class DataRetrieve:
    def __init__(self, metadata_dir: str):
        self.metadata_dir = metadata_dir
    
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


class StandaloneDataRetrieve(PreservicaAuth, DataRetrieve):
    def __init__(self, metadata_dir: str, server: Optional[str], username: Optional[str],
                 tenant: Optional[str] = None, credentials_file: Optional[str] = None,
                 use_keyring: bool = False, save_password_to_keyring: bool = False,
                 keyring_service: str = "preservica_modify",
                 manager_username: Optional[str] = None):
        DataRetrieve.__init__(self, metadata_dir)
        PreservicaAuth.__init__(self, server=server, username=username, tenant=tenant,
                                credentials_file=credentials_file, use_keyring=use_keyring,
                                save_password_to_keyring=save_password_to_keyring,
                                keyring_service=keyring_service, manager_username=manager_username)
