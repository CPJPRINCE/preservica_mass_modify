import logging
from typing import Dict, Hashable, Optional, Any

from preservica_modify.common import check_bool, check_nan, _cell

logger = logging.getLogger(__name__)

class LookupMixin:

    def xip_lookup(self, idx: Hashable) -> tuple[Optional[str], Optional[str], Optional[str]]:
        """
        Uses the pandas index to retrieve data from the "Title, Description and Security" columns. Sets data in Entity.
        :param idx: Pandas Index to lookup
        :param df: Pandas DataFrame containing the data
        :return: Tuple containing the title, description, and security values, or None if not found
        """

        if getattr(self, 'df', None) is None:
            logger.error('Dataframe not initialised, cannot perform lookup')
            raise RuntimeError('Dataframe not initialised, cannot perform lookup')
        try:
            if self.title_flag:
                title = check_nan(_cell(self.df, idx, self.TITLE_FIELD))
                logger.debug(f'XIP Lookup Title: {title}')
            else:
                title = None
            if self.description_flag:
                description = check_nan(_cell(self.df, idx, self.DESCRIPTION_FIELD))
                if description is None and self.blank_override is True:
                    description = None
                logger.debug(f'XIP Lookup Description: {description}')
            else:
                description = None
            if self.security_flag:
                security = check_nan(_cell(self.df, idx, self.SECURITY_FIELD))
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
                            key_name = self.ARCREF_CODE
                        elif self.ACCREF_FIELD in header:
                            key_name = self.ACCREF_CODE
                        elif self.IDENTIFIER_FIELD in header:
                            key_name = default_key if default_key else self.IDENTIFIER_DEFAULT
                        else:
                            key_name = default_key if default_key else self.IDENTIFIER_DEFAULT
                    ident = check_nan(_cell(self.df, idx, header))
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
        
    def retention_lookup(self, idx: Hashable):
        """
        Uses the pandas index to retrieve data from the "Retention Policy" column

        Matched against the policies obtained in the obtain_retentions function.

        :param idx: Pandas Index to lookup
        :param e: Entity to act upon
        """
        try:
            if self.retention_flag:
                retention_policy = check_nan(_cell(self.df, idx,self.RETENTION_FIELD))
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

    # Needed for Modify?
    def pax_lookup(self, idx: Hashable) -> Optional[tuple[Optional[str], Optional[list], Optional[list]]]:
        try:
            pax_path = check_nan(_cell(self.df, idx, self.PAX_PATH))
            pax_dict = self.df.loc[[self.df[self.PAX_PATH] == pax_path, [self.FILE_PATH,self.PAX_PRES_FIELD,self.PAX_ACCESS_FIELD]]].to_dict(orient='index')
            file_access = check_bool(_cell(self.df, idx, self.PAX_ACCESS_FIELD))
            file_preservation = check_bool(_cell(self.df, idx, self.PAX_ACCESS_FIELD))
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
        Requires use of a .credentials file.

        Delete Flag must also be set.
        """
        try:
            return check_bool(_cell(self.df,idx, self.DELETE_FIELD))    
        except Exception:
            logger.exception('Failed to lookup delete flag')
            raise


    def delete_metadata_lookup(self, idx: Hashable):
        """
        Uses the pandas index to retrieve data from the "Delete Metadata" column. If True initiates a Delete All Metadata action.

        Delete Metadata Flag must also be set.
        """
        try:
            return check_bool(_cell(self.df, idx, self.DELETE_METADATA_FIELD))
        except Exception:
            logger.exception('Failed to lookup delete metadata flag')
            raise

    def delete_all_metadata_lookup(self, idx: Hashable):
        """
        Uses the pandas index to retrieve data from the "Delete All Metadata" column. If True initiates a Delete All Metadata action.

        Delete All Metadata Flag must also be set.
        """
        try:
            return check_bool(_cell(self.df, idx, self.DELETE_ALL_METADATA_FIELD))
        except Exception:
            logger.exception('Failed to lookup delete all metadata flag')
            raise