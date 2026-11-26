from datetime import datetime
from pandas.api.types import is_datetime64_dtype
from lxml import etree
import logging
import pandas as pd
from typing import Hashable, List, Optional, Dict, Any
from .common import check_nan, check_bool

logger = logging.getLogger(__name__)

class LookupHandler():
    def __init__(self, df: pd.DataFrame, title_flag: bool, description_flag: bool, security_flag: bool, retention_flag: bool, blank_override: bool):
        self.df = df
        self.column_headers = self.df.columns.tolist()
        
        self.title_flag = title_flag
        self.description_flag = description_flag
        self.security_flag = security_flag
        self.retention_flag = retention_flag
        self.blank_override = blank_override


    def _cell(self, idx: Any, column: str) -> Any:
        return self.df.at[idx, column]

    def xip_lookup(self, idx: Hashable) -> tuple[Optional[str], Optional[str], Optional[str]]:
        """
        Uses the pandas index to retrieve data from the "Title, Description and Security" columns. Sets data in Entity.
        :param idx: Pandas Index to lookup
        """
        
        if getattr(self, 'df', None) is None:
            logger.error('Dataframe not initialised, cannot perform lookup')
            raise RuntimeError('Dataframe not initialised, cannot perform lookup')
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
            message = f'Key Error XIP Lookup, missing Column: {e} please ensure column header\'s are an exact match...'
            logger.exception(message)
            raise KeyError(message) from e
        except IndexError as e:
            logger.warning(f"Index Error for XIP Lookup: {e} it is likely you have removed or added a file/folder to the directory"
                            "after generating the spreadsheet. An opex will still be generated but with no identifiers. To ensure identifiers match up please ensure match up...")
            return None, None, None
        except Exception:
            logger.exception(f'Retention XIP failed: for {idx}')
            raise

    def ident_lookup(self, idx: Hashable, default_key: Optional[str] = None) -> Optional[Dict[str, Optional[str]]]:
        """
        Uses the pandas index to retrieve data from the "Identifier","Archive_Reference", columns. Sets identifiers in Entity.
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
            message = f'Key Error for Identifier Lookup, missing Column: {e} please ensure column header\'s are an exact match...'
            logger.exception(message)
            raise KeyError(message) from e
        except IndexError as e:
            logger.warning(f"Index Error for Identifier Lookup: {e} it is likely you have removed or added a file/folder to the directory"
                            "after generating the spreadsheet. An opex will still be generated but with no identifiers. To ensure identifiers match up please ensure match up...")
        except Exception:
            logger.exception(f'Identifier Lookup failed: for {idx}')
            raise
        
    def retention_lookup(self, idx: Hashable) -> Optional[str]:
        """
        Uses the pandas index to retrieve data from the "Retention Policy" column

        Matched against the policies obtained in the obtain_retentions function.

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
            message = f'Key Error Retention Lookup, missing Column: {e} please ensure column header\'s are an exact match...'
            logger.exception(message)
            raise KeyError(message) from e
        except IndexError as e:
            logger.warning(f"Index Error for Retention Lookup: {e} it is likely you have removed or added a file/folder to the directory"
                            "after generating the spreadsheet. An opex will still be generated but with no identifiers. To ensure identifiers match up please ensure match up...")
        except Exception:
            logger.exception(f'Retention Lookup failed: for {idx}')
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
            message = f'Key Error for Pax Lookup, missing Column: {e} please ensure column header\'s are an exact match...'
            logger.exception(message)
            raise KeyError(message) from e
        except IndexError as e:
            logger.warning(f"Index Error for Pax Lookup: {e} it is likely you have removed or added a file/folder to the directory"
                            "after generating the spreadsheet. An opex will still be generated but with no identifiers. To ensure identifiers match up please ensure match up...")
        except Exception:
            logger.exception(f'Pax Lookup failed: for {idx}')
            raise

    def delete_lookup(self, idx: Hashable):
        """
        Uses the pandas index to retrieve data from the "Delete" column. If True initiates a Delete.                            self.xml_update(ent, ns, xml_new)
                if ent.entity_type == EntityType.ASSET and self.retention_flag is True:
                    self.retention_update(ent, self.retention_lookup(idx))

        Requires use of a .credentials file.

        Delete Flag must also be set.
        """
        try:
            return check_bool(self._cell(idx, self.DELETE_FIELD))    
        except Exception:
            logger.exception('Failed to lookup delete flag')
            raise
    
    def xml_metadata_lookup(self, idx: Hashable, xml_files: List[Dict[str, Any]], metadata_mode: str = 'exact') -> Optional[List[Dict[str, Union[etree._ElementTree, list[Optional[str]]]]]]:
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
                    xml_new = etree.parse(str(xml_file.get('xml_file')))
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
                        if metadata_mode in {'exact'}:
                            val = check_nan(self._cell(idx, path))
                        elif metadata_mode in {'flat'}:
                            val = check_nan(self._cell(idx, name))
                        if pd.isnull(val) or val is None:
                            continue
                        else:
                            if is_datetime64_dtype(val):
                                val = pd.to_datetime(val)
                                val = datetime.strftime(val, "%Y-%m-%dT%H:%M:%S.000Z")
                        if metadata_mode in {'exact'}:
                            n = path.replace(local_name + ":", f"{{{elm_ns}}}")
                            elem = xml_new.find(f'./{n}')
                            if elem is None:
                                logger.warning(f'XML element not found for path: {n, path} in {xml_file}.')
                                continue
                        elif metadata_mode in {'flat'}:
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
