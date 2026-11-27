
from copy import deepcopy
from importlib.resources import path
from typing import Any, Dict, List, Hashable, Optional, Union
import os
from lxml import etree
import logging
from preservica_modify.common import check_nan, _cell
from pandas.api.types import is_datetime64_dtype
import datetime
import pandas as pd

logger = logging.getLogger(__name__)

class DescriptiveMetadataHandler:

    def __init__(self, metadata_dir: str, column_headers: List[str]):
        self.metadata_dir = metadata_dir
        self.column_headers = column_headers

    def init_schema_list(self) -> List[Dict[str, Any]]:
        """
        Retrieves the schema locations from the XML files in the metadata directory.
        Returns a list of schema locations.
        """
        try:
            self.schema_list: List[str] = []
            for file in os.scandir(self.metadata_dir):
                if file.name.endswith('.xml'):
                    path = os.path.join(self.metadata_dir, file.name)
                    try:
                        xml_file = etree.parse(path)
                    except etree.XMLSyntaxError as e:
                        logger.exception(f'XML Syntax Error while parsing {file.name}: {e}')
                        raise
                    except FileNotFoundError as e:
                        logger.exception(f'XML file not found {file.name}: {e}')
                        raise
                    schema = xml_file.getroot().attrib.get('{http://www.w3.org/2001/XMLSchema-instance}schemaLocation')
                    self.schema_list.append(schema)
            return self.schema_list
        except Exception:
            logger.exception('Failed to initialise delete metadata')
            raise

    def init_generate_descriptive_metadata(self, remote_xmls: bool = False) -> List[Dict[str, Any]]:
        """
        Initialises the descriptive metadata by parsing the XML files in the metadata directory, generating a list of the elements and their namespaces, and comparing them against the column headers in the spreadsheet to filter out non-matching data.
        The resulting list of matching elements is stored in self.xml_files for use in generating the descriptive metadata in the Opex manifest.
        """
        try:
            xml_templates = []
            if remote_xmls:
                for xml_doc in self.admin.xml_documents():
                    if xml_doc.get("DocumentType") != "MetadataTemplate":
                        continue
                    name = xml_doc.get("Name")
                    schema_uri = xml_doc.get("SchemaUri")
                    if not schema_uri:
                        logger.exception(f'SchemaUri not found for XML document {name}')
                        raise ValueError(f'SchemaUri not found for XML document {name}')
                    xml = self.admin.xml_document(schema_uri)
                    if not xml:
                        logger.exception(f'Failed to retrieve XML document for SchemaUri {schema_uri}')
                        raise ValueError(f'Failed to retrieve XML document for SchemaUri {schema_uri}')
                    tree = etree.fromstring(xml.encode('utf-8')).getroottree()
                    xml_templates.append((name, tree))
            else:
                for file in os.scandir(self.metadata_dir):
                    if file.name.endswith('.xml'):
                        try:
                            tree = etree.parse(file.path)
                            xml_templates.append((file.path, tree))
                        except etree.XMLSyntaxError as e:
                            logger.exception(f'XML Syntax Error while parsing {file.name}: {e}')
                            raise
                        except FileNotFoundError as e:
                            logger.exception(f'XML file not found {file.name}: {e}')
                            raise

            self.xml_files: List[Dict[str, Any]] = []
            for name, xml_file in xml_templates:
                list_elms_xml: List[Dict[str,Any]] = []
                """
                Generates info on the elements of the XML Files placed in the Metadata directory.
                Composed as a list of dictionaries.
                """
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
                            list_elms_xml.append({"Name": elem_dict.get('Name'), "XName": elem_dict.get('XName'), "Namespace": elem_dict.get('Namespace'), "Path": elem_dict.get('Path')})
                except KeyError as e:
                    logger.exception(f'Key Error comparing XML elements to column headers for file {name}: {e}')
                    raise
                except IndexError as e:
                    logger.warning(f'Index Error comparing XML elements to column headers for file {name}: {e}')
                except Exception:
                    logger.exception(f'Error comparing XML elements to column headers for file {name}')
                    raise
                if len(list_elms_xml) > 0:
                    self.xml_files.append({'data': list_elms_xml, 'local_name': root_element_ln, 'local_ns': root_element_ns, 'xml_file': name, 'xml_tree': xml_file})
                    logger.info(f'Matching columns found in spreadsheet for XML file: {name}, added to metadata generation list.')
                else:
                    logger.debug(f'No matching columns found in spreadsheet for XML file: {name}, skipping this file for metadata generation.')
            return self.xml_files
        except FileNotFoundError:
            logger.exception(f'Metadata directory not found at path: {self.metadata_dir}')
            raise
        except Exception:
            logger.exception('Failed to intialise XML Metadata')
            raise
    
    def generate_descriptive_metadata(self, idx: Hashable, xml_files: List[Dict[str, Any]]) -> Optional[List[Dict[str, Union[etree._ElementTree, list[Optional[str]]]]]]:
        """
        Generates the xml file based on the returned list of xml_files from the init_generate_descriptive_metadata function.

        :param idx: Pandas Index to lookup
        :param xml_file: Dictionary of XML files created as part of init 
        
        This version returns a list of dictionaries containing the XML tree and the XNames for each file, which can be used in the xml_merge function to merge the generated metadata with the original XML template. This allows for more flexible handling of multiple XML files and ensures that all relevant metadata is included in the final Opex manifest.
        """
        try:
            xml_list = []
            for xml_file in xml_files:
                xml_file: Dict[str, Any]
                xml_data = xml_file.get('data')
                xnames: list[Optional[str]] = []
                xnames = [x.get('XName') for x in xml_data if isinstance(x, Dict)] if xml_data is not None else []                
                local_name = xml_file.get('local_name', None)
                local_ns = xml_file.get('local_ns', None)
                if local_name is None or local_ns is None:
                    logger.warning(f'Missing local_name or local_ns for XML file: {xml_file.get("xml_file")}, skipping XML generation for this file.')
                    continue
                if xml_data is None or len(xml_data) == 0:
                    logger.warning(f'No XML elements found for {xml_file.get("xml_file")}, skipping XML generation for this file.')
                    continue
                else:
                    xml_new = deepcopy(xml_file.get('xml_tree'))
                    #xml_new = etree.parse(str(xml_file.get('xml_file')))
                    for elem_dict in xml_data:
                        if not isinstance(elem_dict, Dict):
                            logger.warning(f'Invalid element data for {elem_dict} in file {xml_file.get("xml_file")}, skipping this element.')
                            continue
                        name = elem_dict.get('Name')
                        path = elem_dict.get('Path')
                        elm_ns = elem_dict.get('Namespace')
                        if not isinstance(name, str) or not isinstance(path, str) or not isinstance(elm_ns, str):
                            logger.warning(f'Missing Name or Path or Namespace for element {elem_dict} in file {xml_file.get("xml_file")}, skipping this element.')
                            continue
                        if self.metadata_flag in {'exact'}:
                            val = check_nan(_cell(self.df, idx, path))
                        elif self.metadata_flag in {'flat'}:
                            val = check_nan(_cell(self.df, idx, name))
                        if pd.isnull(val) or val is None:
                            continue
                        else:
                            if is_datetime64_dtype(val):
                                val = pd.to_datetime(val)
                                val = datetime.datetime.strftime(val, "%Y-%m-%dT%H:%M:%S.000Z")
                        if self.metadata_flag in {'exact'}:
                            n = path.replace(local_name + ":", f"{{{elm_ns}}}")
                            elem = xml_new.find(f'./{n}')
                            if elem is None:
                                logger.warning(f'XML element not found for path: {n, path} in {xml_file}.')
                                continue
                        elif self.metadata_flag in {'flat'}:
                            n = name.split(':')[-1]
                            elem = xml_new.find(f'.//{{{elm_ns}}}{n}')
                            if elem is None:
                                logger.warning(f'Element not found in XML for {name, path} in {xml_file}.')
                                continue
                        if elem is not None:
                            elem.text = str(val)
                    xml_list.append({local_ns: xml_new, 'xnames': xnames})
            return xml_list
        except KeyError as e:
            logger.exception(f'Key Error in XML Lookup: {e}' \
            '\n Please ensure column header\'s are an exact match.')
            raise
        except IndexError as e:
            logger.warning(f'Index Error: {e}' \
            '\nIt is likely you have removed or added a file/folder to the directory' \
            'after generating your input spreadsheetree. An opex will still be generated but with no xml metadata.' \
            '\nTo ensure metadata match up please regenerate the spreadsheetree.')
            return None
        except Exception:
            logger.exception('General Error in XML Lookup')
            raise

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