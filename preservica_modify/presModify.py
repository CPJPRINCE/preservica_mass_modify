"""
Preservica Mass Modify Tool

This tool is utilised to modify existing data on Preservica through the use of spreadsheets.
Allows for modification of XIP and XML data.

Author: Christopher Prince
license: Apache License 2.0"
"""

from pyPreservica import Entity, EntityType
from lxml import etree
import os
from typing import Optional
import logging
import configparser

from preservica_modify.common import check_nan
from preservica_modify.auth import PreservicaAuth
from preservica_modify.lookups import LookupMixin
from preservica_modify.updates import UpdatesMixin
from preservica_modify.descMetadata import DescriptiveMetadataHandler
from preservica_modify.continuetokens import ContinueTokenHandler
from preservica_modify.dataframes import DataFrameHandler
from preservica_modify.dataRetrieve import DataRetrieve
from preservica_modify.progress import row_progress

logger = logging.getLogger(__name__)

class PreservicaMassMod(PreservicaAuth,
                        LookupMixin,
                        UpdatesMixin,
                        DescriptiveMetadataHandler,
                        ContinueTokenHandler,
                        DataFrameHandler,
                        DataRetrieve):
    """
    Mass Modification Class
    """
    def __init__(self,
                 input_file: str,
                 metadata_dir: str = os.path.join(os.path.dirname(__file__), "metadata"),
                 blank_override: bool = False,
                 metadata: Optional[str] = None,
                 delete_metadata_flag: bool = False,
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
                 options_file: str = os.path.join(os.path.dirname(__file__),'options', 'options.properties'),
                 use_remote_xmls: bool = False,
                 checkpoint_hash: bool = True,
                 show_progress: bool = False):
        
        self.metadata_dir = metadata_dir
        self.metadata_flag = metadata
        self.delete_metadata_flag = delete_metadata_flag
        self.dummy_flag = dummy
        self.blank_override = blank_override
        self.delete_flag = delete
        self.descendants_flag = descendants
        
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
        self.checkpoint_hash = checkpoint_hash
        self.show_progress = show_progress

        self.column_sensitivity = column_sensitivity
        self.use_remote_xmls = use_remote_xmls

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
        self.TITLE_FIELD=section.get('TITLE_FIELD', 'Title')
        self.DESCRIPTION_FIELD=section.get('DESCRIPTION_FIELD', 'Description')
        self.SECURITY_FIELD=section.get('SECURITY_FIELD', 'Security')
        self.RETENTION_FIELD=section.get('RETENTION_FIELD', 'Retention Policy')
        self.MOVETO_FIELD=section.get('MOVETO_FIELD', 'Move to')
        self.DELETE_FIELD=section.get('DELETE_FIELD', 'Delete')
        self.IDENTIFIER_FIELD=section.get('IDENTIFIER_FIELD', 'Identifier')
        self.IDENTIFIER_DEFAULT=section.get('IDENTIFIER_DEFAULT', 'code')
        self.FILE_PATH=section.get('FILE_PATH', 'FullName')       
        
        self.ARCREF_FIELD=section.get('ARCREF_FIELD', 'Archive_Reference')
        self.ARCREF_CODE=section.get('ARCREF_CODE', 'code')
        self.ACCREF_FIELD=section.get('ACCREF_FIELD', 'Accession_Reference')
        self.ACCREF_CODE=section.get('ACCREF_CODE', 'accref')

        self.DELETE_METADATA_FIELD=section.get('DELETE_METADATA_FIELD', 'Delete Metadata')
        self.DELETE_ALL_METADATA_FIELD=section.get('DELETE_ALL_METADATA_FIELD', 'Delete All Metadata')
        
        logger.debug(f'Configuration loaded: {section}')

    def _process_descent(self,idx: int, descendant_ent: Entity, entity_type: EntityType):
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
        """
        Method to process rows from the data dictionary, handling entity references and document types.
        """
        try:
            keys = list(data_dict.keys())
            input_hash: Optional[str] = None
            if self.disable_continue:
                start_pos = 0
            else:
                input_hash = self._input_hash()
                start_pos, _ = self._load_checkpoint("modify", keys, input_hash)
            with row_progress(len(keys), start_pos, "Modify rows", self.show_progress) as progress:
                for position in range(start_pos, len(keys)):
                    idx = keys[position]
                    reference_dict = data_dict.get(idx)
                    if reference_dict is not None:
                        ref = check_nan(reference_dict.get(self.ENTITY_REF))
                        if ref is None:
                            logger.warning(f'No reference found for index: {idx}, skipping to next row.')
                            if not self.disable_continue:
                                self._save_checkpoint("modify", position + 1, False, input_hash)
                            progress.update(1)
                            continue
                        doc_type = check_nan(reference_dict.get(self.DOCUMENT_TYPE))
                        if doc_type is None:
                            logger.warning(f'No document type found for index: {idx}, attempting to retrieve entity without document type.')
                    else:
                        logger.error(f'No data found for index: {idx}')
                        raise ValueError(f'No data found for index: {idx}')
                    logger.info(f"Processing Row Index: {idx}, Reference: {ref}")
                    if not self.disable_continue:
                        self._save_checkpoint("modify", position, True, input_hash)
                    ent = self._process_fetch_ent(ref, doc_type)
                    if ent is not None:
                        self._process_row_ent(ent, idx, reference_dict)
                    else:
                        logger.warning(f'Entity not found for reference {ref}, skipping to next row.')
                    if not self.disable_continue:
                        self._save_checkpoint("modify", position + 1, False, input_hash)
                    progress.update(1)
        except KeyboardInterrupt:
            logger.warning('Modification interrupted. Check Preservica before resuming an in-progress row.')
            raise KeyboardInterrupt('Process interrupted by user, exiting...')
        except Exception:
            logger.exception('Error processing rows.')
            raise

     # Setup for Local Definition of Entity?
    def _process_fetch_ent(self, ref: str, doc_type: Optional[str]) -> Optional[Entity]:
        """
        Fetches the entity based on the reference and document type.

        Args:
            ref (str): The reference identifier for the entity.
            doc_type (Optional[str]): The document type, either "SO" for folder or "IO" for asset.

        Returns:
            Optional[Entity]: The retrieved entity if found, otherwise None.
        """
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
        """
        Process the entity for the given row index, applying updates based on the configured flags and lookup values.
        """
        if self.delete_flag is True:
            delete_check = self.delete_update(idx, ent)
            if delete_check is True:
                return
        if any([self.title_flag, self.description_flag, self.security_flag]) is True:
            title, description, security = self.xip_lookup(idx)
            self.xip_update(ent,title,description,security)
        self.ident_update(ent, self.ident_lookup(idx, self.IDENTIFIER_DEFAULT))

        if self.delete_metadata_flag is True:
            self.init_schema_list()
            if self.delete_individual_metadata_flag is True:
                delete_meta_check = self.delete_metadata_lookup(idx)
                if delete_meta_check is True:
                    for schema in self.schema_list:
                        self.delete_metadata_update(ent, schema)
            elif self.delete_all_metadata_flag is True:
                delete_all_meta_check = self.delete_all_metadata_lookup(idx)
                if delete_all_meta_check is True:
                    self.delete_all_metadata_update(ent)
            else:
                logger.info("No delete metadata flags are set for this row.")

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
        Main loop for processing the spreadsheet and applying updates to Preservica entities.

        This method initializes the dataframe, sets input flags, logs into Preservica,
        and processes rows based on the configured flags for metadata, retention, and upload.
        It also handles exceptions and logs the progress.
        """
        try:
            self.init_df()
            self._set_input_flags()
            self.login_preservica()
            if self.metadata_flag is not None:
                self.init_generate_descriptive_metadata(remote_xmls=self.use_remote_xmls)
            if self.retention_flag is True:
                self.get_retentions()
            if self.DOCUMENT_TYPE in self.column_headers:
                data_dict = self.df[[self.ENTITY_REF, self.DOCUMENT_TYPE]].to_dict(orient='index')
            else:
                data_dict = self.df[[self.ENTITY_REF]].to_dict(orient='index')
            self._process_rows(data_dict)
            if not self.disable_continue:
                self._remove_checkpoint("modify")
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