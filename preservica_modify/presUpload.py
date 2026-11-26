"""
Upload Module for Preservica Mass Modify

Author: Christopher Prince
license: Apache License 2.0"
"""

import time, os, sys
from preservica_modify.common import check_nan
from pyPreservica.uploadAPI import UploadProgressCallback, complex_asset_package, simple_asset_package
from typing import Optional, Hashable
from lxml import etree
import configparser
import logging

from preservica_modify.dataRetrieve import DataRetrieve
from preservica_modify.dataframes import DataFrameHandler
from preservica_modify.auth import PreservicaAuth
from preservica_modify.continuetokens import ContinueTokenHandler
from preservica_modify.dataframes import DataFrameHandler
from preservica_modify.descMetadata import DescriptiveMetadataHandler
from preservica_modify.updates import UpdatesMixin
from preservica_modify.lookups import LookupMixin

logger = logging.getLogger(__name__)

class PreservicaMassUpload(PreservicaAuth, ContinueTokenHandler, DataFrameHandler, DescriptiveMetadataHandler, UpdatesMixin, LookupMixin, DataRetrieve):
    """
    Mass Uploader for Preservica
    """
    def __init__(self,
                 input_file: str,
                 metadata_dir: str = os.path.join(os.path.dirname(__file__), "metadata"),
                 upload_mode: bool = False,
                 metadata: Optional[str] = None,
                 dummy: bool = False,
                 username: Optional[str] = None,
                 password: Optional[str] = None,
                 manager_username: Optional[str] = None,
                 manager_password: Optional[str] = None,
                 server: Optional[str] = None,
                 tenant: Optional[str] = None,
                 credentials: str = os.path.join(os.getcwd(),"credentials.properties"),
                 use_keyring: bool = False,
                 keyring_service: str = "preservica_modify",
                 save_password_to_keyring: bool = False,
                 disable_continue: bool = False,
                 column_sensitivity: bool = False,
                 options_file: str = os.path.join(os.path.dirname(__file__),'options', 'options.properties'),
                 check_parents: bool = False,
                 use_parent_assets: bool = False,
                 timeout: int = 60):
        
        self.metadata_dir = metadata_dir
        self.metadata_flag = metadata
        self.dummy_flag = dummy

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
        self.column_sensitivity = column_sensitivity

        if options_file is None:
            options_file = os.path.join(os.path.dirname(__file__),'options','uploadopts.properties')
        self.parse_config(options_file=os.path.abspath(options_file), column_sensitivity=self.column_sensitivity)

        self.xnames: list[str] = []

        self.use_parent_assets = use_parent_assets
        self.check_parents = check_parents
        self.TIMEOUT = timeout

        self.blank_override = False # not used in uploads

    def parse_config(self, options_file: str, column_sensitivity: bool = False) -> None:
        config = configparser.ConfigParser()
        read_config = config.read(options_file, encoding='utf-8')
        if not read_config:
            logger.warning(f"Options file not found or not readable: {options_file}. Using defaults.")

        section = config['options'] if 'options' in config else {}

        if column_sensitivity:
            section = {k:v.lower() for k,v in section.items()}

#        self.ENTITY_REF=section.get('ENTITY_REF', 'Entity Ref')
#        self.DOCUMENT_TYPE=section.get('DOCUMENT_TYPE', 'Document type')

        self.UPLOAD_REF=section.get('UPLOAD_REF', 'Upload to' if 'Upload to' in section else 'Entity Ref')
        self.UPLOAD_TYPE=section.get('UPLOAD_TYPE', 'Upload type' if 'Upload type' in section else 'Document type')
        self.TITLE_FIELD=section.get('TITLE_FIELD', 'Title')
        self.DESCRIPTION_FIELD=section.get('DESCRIPTION_FIELD', 'Description')
        self.SECURITY_FIELD=section.get('SECURITY_FIELD', 'Security')
        self.RETENTION_FIELD=section.get('RETENTION_FIELD', 'Retention Policy')
        self.IDENTIFIER_FIELD=section.get('IDENTIFIER_FIELD', 'Identifier')
        self.IDENTIFIER_DEFAULT=section.get('IDENTIFIER_DEFAULT', 'code')
        self.FILE_PATH=section.get('FILE_PATH', 'FullName')       

        self.MOVETO_FIELD=section.get('MOVETO_FIELD', None)
        
        self.UPLOAD_TYPE_FIELD=section.get('UPLOAD_TYPE_FIELD', 'Upload type')
        self.PAX_PRES_FIELD=section.get('PAX_PRES_FIELD', 'Preservation')
        self.PAX_ACCESS_FIELD=section.get('PAX_ACCESS_FIELD', 'Access')
        self.PAX_PATH=section.get('PAX_PATH', 'PAX Path')

        self.ARCREF_FIELD=section.get('ARCREF_FIELD', 'Archive_Reference')
        self.ACCREF_FIELD=section.get('ACCREF_FIELD', 'Accession_Reference')
        self.ACCREF_CODE=section.get('ACCREF_CODE', 'accref')
        
        logger.debug(f'Configuration loaded: {section}')


    def pax_create(self, idx: int, title: str, description: Optional[str] = None, security: Optional[str] = None, ident_dict: Optional[dict] = None, xml_data: Optional[dict] = None) -> Optional[complex_asset_package]:
        if idx is not None:
            pax_path, pax_preservation, pax_access = self.pax_lookup(idx)
        if pax_preservation is None:
            logger.warning(f'No preservation or access files found for PAX at index {idx}, skipping creation...')
            return None
        file_name = os.path.basename(str(pax_path)).split(".")[0] + ".pax"
        logger.info(f'Creating new PAX at index {idx} with file name {file_name}')
        logger.debug(f'PAX creation parameters - idx: {idx}, title: {title}, description: {description}, security: {security}, ident_dict: {ident_dict}, xml_data: {xml_data}')
        if self.dummy_flag is False:
            new_pax = complex_asset_package(title=title,
                                            description=description,
                                            security_tag=security,
                                            preservation_files_list= pax_preservation if pax_preservation else None,
                                            access_file=pax_path if pax_access else None,
                                            identifers=ident_dict,
                                            io_name=file_name,
                                            export_folder='.',
                                            Asset_Metadata=xml_data)
            logger.info(f'Created new PAX: {file_name}')
        return new_pax
    
    def so_create(self, upload_folder: str, title: str, description: Optional[str] = None, security: Optional[str] = None, ident_dict: Optional[dict] = None, xml_data: Optional[dict] = None, retention_policy: Optional[str]=None):
        if description is None:
            description = ""
        if security is None:
            security = ""
        logger.info(f'Creating new SO in folder {upload_folder} with title {title}')
        logger.debug(f'SO creation parameters - title: {title}, description: {description}, security: {security}, upload_folder: {upload_folder}')
        if self.dummy_flag is False:
            time.sleep(self.TIMEOUT)
            new_folder = self.entity.create_folder(title=title,
                                                description=description,
                                                security_tag=security,
                                                parent=upload_folder)
            self.ident_update(new_folder,ident_dict)
            if retention_policy is not None:
                self.retention_update(new_folder,retention_policy)
            if xml_data is not None:
                for ns,new_xml in xml_data.items():
                    self.xml_update(new_folder, ns, new_xml)
        return new_folder

    def pa_create(self, upload_folder: str, title: str, description: Optional[str] = None, security: Optional[str] = None, ident_dict: Optional[dict] = None, xml_data: Optional[dict] = None, retention_policy: Optional[str]=None, ):
        if description is None:
            description = ""
        if security is None:
            security = ""
        logger.info(f'Creating new PA in folder {upload_folder} with title {title}')
        logger.debug(f'PA creation parameters - title: {title}, description: {description}, security: {security}, upload_folder: {upload_folder}')
        if self.dummy_flag is False:
            time.sleep(self.TIMEOUT)
            new_pa = self.entity.add_physical_asset(title=title, description=description, security_tag=security, parent=upload_folder)
            if ident_dict is not None:
                self.ident_update(new_pa, ident_dict)
            if retention_policy is not None:
                self.retention_update(new_pa,retention_policy)
            if xml_data is not None:
                for ns,new_xml in xml_data.items():
                    self.xml_update(new_pa, ns, new_xml)
        return new_pa

    def so_upload(self, idx: int, upload_folder: str, title: str, description: Optional[str] = None, security: Optional[str] = None, ident_dict: Optional[dict] = None, xml_data: Optional[dict] = None, retention_policy: Optional[str]=None):
        if self.FILE_PATH in self.column_headers:
            logger.info(f'Creating new SO at index {idx} in folder {upload_folder} with title {title}')
            logger.debug(f'SO creation parameters - idx: {idx}, upload_folder: {upload_folder}, title: {title}, description: {description}, security: {security}, ident_dict: {ident_dict}, xml_data: {xml_data}, retention_policy: {retention_policy}')
            if self.dummy_flag is False:    
                time.sleep(self.TIMEOUT)
                folder_path = check_nan(self.df[self.FILE_PATH].loc[idx])
                if folder_path is None:
                    logger.exception(f'The upload path for {idx} is set to blank, please ensure a valid path is given')
                    raise Exception(f'The upload path for {idx} is set to blank, please ensure a valid path is given')
                if os.path.isfile(folder_path):
                    logger.warning(f'Folder marked as SO-Upload. Ignoring file...')
                    return None
                elif os.path.isdir(folder_path):
                    try:
                        self.entity.folder(upload_folder)
                    except Exception as e:
                        logger.exception(f'The reference given {upload_folder} is not a valid folder on your Preservica Server')
                        raise Exception(f'The reference given {upload_folder} is not a valid folder on your Preservica Server') from e
                    file_list = [p.path for p in os.scandir(folder_path) if os.path.isfile(p)]
                    sip = complex_asset_package(preservation_files_list=file_list,
                                                parent_folder=upload_folder,
                                                title=title,
                                                description=description,
                                                security_tag=security,
                                                identifiers=ident_dict,
                                                AssetMetadata=xml_data)
                    callback = UploadProgressCallback(folder_path)
                    self.upload.upload_zip_package(sip, folder=upload_folder, callback=callback)
                    if self.retention_flag is True:
                        time.sleep(self.TIMEOUT)
                        self.retention_update(sip, retention_policy)
                    return upload_folder
            else:
                logger.warning(f'Path marked as SO-Upload is not a valid file or folder path. Ignoring entry...')
                return None
        else:
            logger.exception(f'The upload path column: {self.FILE_PATH} is not present in the spreadsheet; please ensure it is with a valid path to folder')
            raise Exception(f'The upload path column: {self.FILE_PATH} is not present in the spreadsheet; please ensure it is with a valid path to folder')
        
    def so_crawl_alt(self, idx: int, upload_folder: str, title: str, description: Optional[str] = None, security: Optional[str] = None, ident_dict: Optional[dict] = None, xml_data: Optional[dict] = None, retention_policy: Optional[str]=None):
        if self.FILE_PATH in self.column_headers:
            folder_path = check_nan(self.df[self.FILE_PATH].loc[idx])
            if folder_path is None:
                logger.exception(f'The upload path for {idx} is set to blank, please ensure a valid path is given')
                raise Exception(f'The upload path for {idx} is set to blank, please ensure a valid path is given')
            if os.path.isdir(folder_path):

                def upload_loop(self, path_list, parent_folder = None):
                    for p in path_list:
                        logger.info(f'Processing path: {p}')
                        logger.debug(f'Parent folder: {parent_folder}, Current path: {p}')
                        if self.dummy_flag is False:
                            time.sleep(self.TIMEOUT)
                            f_list = [f.path for f in os.scandir(p) if os.path.isfile(f)]
                            d_list = [d.path for d in os.scandir(p) if os.path.isdir(d)]
                            sip = complex_asset_package(f_list, parent_folder=parent_folder)
                            upload = self.upload.upload_zip_package(sip, folder=parent_folder, callback=UploadProgressCallback(sip))
                            self.upload_loop(d_list, upload)
                
                logger.info(f'Processing folder path: {folder_path}')
                logger.debug(f'Upload folder: {upload_folder}, Folder path: {folder_path}')
                if self.dummy_flag is False:
                    time.sleep(self.TIMEOUT)
                    upload_loop([folder_path],upload_folder)
            else:
                logger.warning(f'Folder marked as SO-Crawl. Ignoring file...')
                pass
        else:
            logger.exception(f'The upload path column: {self.FILE_PATH} is not present in the spreadsheet; please ensure it is with a valid path to folder')
            raise Exception(f'The upload path column: {self.FILE_PATH} is not present in the spreadsheet; please ensure it is with a valid path to folder')

    def so_crawl(self, idx: int, upload_folder: str, title: str, description: Optional[str] = None, security: Optional[str] = None, ident_dict: Optional[dict] = None, xml_data: Optional[dict] = None, retention_policy: Optional[str]=None):
        time.sleep(self.TIMEOUT)
        if self.FILE_PATH in self.column_headers:
            folder_path = check_nan(self.df[self.FILE_PATH].loc[idx])
            if folder_path is None:
                logger.exception(f'The file path for {idx} is set to blank, please ensure a valid path is given')
                raise Exception(f'The file path for {idx} is set to blank, please ensure a valid path is given')
            if os.path.isdir(folder_path):
                logger.info(f'Uploading folder: {folder_path} to Preservica folder: {upload_folder}')
                logger.debug(f'Folder path: {folder_path}, Upload folder: {upload_folder}')
                if self.dummy_flag is False:
                    self.upload.crawl_filesystem(folder_path,preservica_parent=upload_folder,callback=UploadProgressCallback(folder_path))
            else:
                logger.warning(f'Folder marked as SO-Crawl. Ignoring file...')
                pass
        else:
            logger.exception(f'The upload path column: {self.FILE_PATH} is not present in the spreadsheet; please ensure it is with a valid path to folder')
            raise Exception(f'The upload path column: {self.FILE_PATH} is not present in the spreadsheet; please ensure it is with a valid path to folder')
        
    def io_upload(self, idx: int, upload_folder: str, title: str, description: Optional[str] = None, security: Optional[str] = None, ident_dict: Optional[dict] = None, xml_data: Optional[dict] = None, retention_policy: Optional[str]=None):
        
        if self.FILE_PATH in self.column_headers:
            file_path = str(self.df[self.FILE_PATH].loc[idx])
            if check_nan(file_path):
                logger.exception(f'The upload path for {idx} is set to blank, please ensure a valid path is given')
                raise Exception(f'The upload path for {idx} is set to blank, please ensure a valid path is given')
            if os.path.isfile(file_path):
                if not file_path.endswith('.zip'):
                    sip = simple_asset_package(preservation_file=file_path,parent_folder=upload_folder,
                                               title=title,
                                               description=description,
                                               security_tag=security,
                                               identifiers=ident_dict,
                                               AssetMetadata=xml_data)
                else:
                    sip = file_path
                new_io = self.upload.upload_zip_package(sip, folder=upload_folder, callback=UploadProgressCallback(sip))
                if self.retention_flag is True:
                    time.sleep(self.TIMEOUT)
                    self.retention_update(new_io, retention_policy)
                return upload_folder
            else:
                logger.warning(f'File marked as IO-Upload. Ignoring folder...')
                pass
        else:
            logger.exception(f'The upload path column: {self.FILE_PATH} is not present in the spreadsheet; please ensure it is with a valid path to folder')
            raise Exception(f'The upload path column: {self.FILE_PATH} is not present in the spreadsheet; please ensure it is with a valid path to folder')

    def io_pax_upload(self, idx: int, upload_folder: str, title: str, description: Optional[str] = None, security: Optional[str] = None, ident_dict: Optional[dict] = None, xml_data: Optional[dict] = None, retention_policy: Optional[str]=None):
        
        if self.FILE_PATH in self.column_headers:
            file_path = str(self.df[self.FILE_PATH].loc[idx])
            if check_nan(file_path):
                logger.exception(f'The upload path for {idx} is set to blank, please ensure a valid path is given')
                raise Exception(f'The upload path for {idx} is set to blank, please ensure a valid path is given')
            if os.path.isfile(file_path):
                if not file_path.endswith('.zip'):
                    sip = self.pax_create(idx, title, description, security, ident_dict, xml_data)
                else:
                    sip = file_path
                new_io = self.upload.upload_zip_package(sip, folder=upload_folder, callback=UploadProgressCallback(sip))
                if self.retention_flag is True:
                    time.sleep(self.TIMEOUT)
                    self.retention_update(new_io, retention_policy)
                return upload_folder
            else:
                logger.warning(f'File marked as IO-Upload. Ignoring folder...')
                pass
        else:
            logger.exception(f'The upload path column: {self.FILE_PATH} is not present in the spreadsheet; please ensure it is with a valid path to folder')
            raise Exception(f'The upload path column: {self.FILE_PATH} is not present in the spreadsheet; please ensure it is with a valid path to folder')


    """
    # Needs a rework.
    def so_pax_upload(self, idx: int, upload_folder: str, title: str, description: Optional[str] = None, security: Optional[str] = None, ident_dict: Optional[dict] = None):
        
        if description is None:
            description = ""
        if security is None:
            security = ""
        if all({self.PATH_FIELD and self.PAX_PRES_FIELD and self.PAX_PRES_FIELD} in self.column_headers):
            pres_folder_path = check_nan(self.df[self.PRES_UPLOAD_FIELD].loc[idx].item())
            access_folder_path = check_nan(self.df[self.ACCESS_UPLOAD_FIELD].loc[idx].item())
            if pres_folder_path is None:
                print(f'Either Access or Upload path for {idx} is set to blank, please ensure a valid path is given for both')
                time.sleep(self.TIMEOUT)
                raise SystemExit()
            if all(os.path.isdir(pres_folder_path), os.path.isdir(access_folder_path)):
                acc_file_list = [pth.path for pth in os.scandir(access_folder_path)]
                pres_file_list = [pth.path for pth in os.scandir(pres_folder_path)]
                sip = complex_asset_package(pres_file_list,acc_file_list,parent_folder=upload_folder,
                                            Title=title,Description=description,SecurityTag=security,
                                            Asset_Metadata=xml_dict,Identifiers=ident_dict,)
                callback = UploadProgressCallback(sip)
                self.upload.upload_zip_package(sip, folder=upload_folder, callback=callback)
                return upload_folder
        else:
            print(f'The upload path columns: {self.PRES_UPLOAD_FIELD, self.ACCESS_UPLOAD_FIELD} are not present in the spreadsheet; please ensure they are with a valid path to folder')
            time.sleep(5)
            raise SystemExit()
    """

    def _process_rows(self, data_dict: dict) -> None:
        """
        Method to process rows from the data dictionary, handling entity references and document types.
        """
        try:
            last_ref = None
            keys, start_pos = self._process_continue_token(data_dict)
            for idx in keys[start_pos:]:
                reference_dict = data_dict.get(idx)
                if reference_dict is not None:
                    upload_ref = check_nan(reference_dict.get(self.UPLOAD_REF))
                    if upload_ref is None:
                        logger.warning(f'No reference found for index: {idx}, skipping to next row.')
                        continue
                    upload_type = reference_dict.get(self.UPLOAD_TYPE)
                    if upload_ref is None or str(upload_ref).lower() == "use parent" and upload_type is not None:
                        if last_ref is None:
                            logger.error('No previous reference found. Please provide a reference in at least the first row.')
                            raise ValueError('No previous reference found. Please provide a reference in at least the first row.')
                        self._process_upload_row(idx, str(last_ref), str(upload_type))
                    else:
                        logger.info(f"Processing Row Index: {idx}, Uploading to: {upload_ref}")
                        last_ref = self._process_upload_row(idx, upload_ref, str(upload_type))
                else:
                    logger.error(f'No data found for index: {idx}')
                    raise ValueError(f'No data found for index: {idx}')
        except KeyboardInterrupt:
            logger.warning('Process interrupted by user during upload mode, exiting...')
            if self.disable_continue is False:
                self._save_continue_token(self.input_file, idx)
            raise KeyboardInterrupt('Process interrupted by user, exiting...') from None
        except Exception:
            logger.exception('Error running upload mode')
            raise

    def _process_fetch_parent(self, upload_folder: str):
        """
        Method to fetch the parent entity based on the upload folder and document type.
        """
        try:
            try: 
                parent_ent = self.entity.folder(upload_folder)
                logger.debug(f'Fetched parent entity as folder for upload folder: {upload_folder}')
            except Exception:
                if self.use_parent_assets:
                    parent_ent = self.entity.asset(upload_folder).parent()
                    logger.debug(f'Failed to fetch parent entity as folder for upload folder: {upload_folder}, trying as asset parent.')
                else:
                    logger.error(f'Failed to fetch parent entity as folder for upload folder: {upload_folder} and parent assets not used. Skiping Entry')
                    return None 
            return parent_ent
        except Exception as e:
            logger.error(f'Error fetching parent entity for folder {upload_folder}: {e}')
            return None
        

    def _process_upload_row(self, idx: Hashable, upload_folder: str, upload_type: str):
        """
        Main processing function for upload mode.
        """
        time.sleep(self.TIMEOUT)

        try:
            if self.check_parents:
                self._process_fetch_parent(upload_folder)
            title, description, security = self.xip_lookup(idx)
            if title is None:
                title = self.df[self.FILE_PATH].loc[idx].split(os.sep)[-1]
            ident_dict = self.ident_lookup(idx, self.IDENTIFIER_DEFAULT)
            retention_policy = self.retention_lookup(idx)
            if self.metadata_flag is not None:    
                xml_dict = {}
                xmls = self.generate_descriptive_metadata(idx, self.xml_files)
                if xmls is not None:
                    for x in xmls:
                        ns = list(x.keys())[0]
                        assert isinstance(ns, str)
                        new_xml = x.get(ns)
                        assert isinstance(new_xml, etree._ElementTree)
                        xml_dict.update({ns: new_xml})                
            
            if upload_type == "PAX-Create":
                new_pax = self.pax_create(idx, title, description, security, ident_dict, xml_dict)
                return new_pax
        
            elif upload_type == "SO-Create":
                return self.so_create(upload_folder,title,description,security,ident_dict,xml_dict,retention_policy) 
        
            elif upload_type == "PA-Create": 
                return self.pa_create(upload_folder,title,description,security,ident_dict,xml_dict,retention_policy)

            #elif upload_type == "SO-PAX-Upload":
            #    new_pax = self.so_pax_upload(idx,upload_folder,title,description,security,ident_dict)
            #    return new_pax

            elif upload_type == "SO-Upload":
                return self.so_upload(idx,upload_folder,title,description,security,ident_dict, xml_dict, retention_policy)
            
            elif upload_type == "SO-Crawl":
                return self.so_crawl(idx,upload_folder,title,description,security,ident_dict)

            elif upload_type == "SO-Crawl-Alt":
                return self.so_crawl_alt(idx,upload_folder,title,description,security,ident_dict)

            elif upload_type == "IO-PAX-Upload":
               return self.io_pax_upload(idx,upload_folder,title,description,security,ident_dict,xml_dict,retention_policy)
                                    
            elif upload_type == "IO-Upload":
                return self.io_upload(idx,upload_folder,title,description,security,ident_dict,xml_dict,retention_policy)
            else:
                logger.warning(f'Upload mode for {upload_type} is not currently supported, skipping...')
                return None
        except Exception as e:
            logger.exception(f'An error occurred during upload mode processing for index {idx} with document type {upload_type}'
                             f'Please review the error message and stack trace for more details.'
                             f'Error message: {str(e)}')
            raise Exception(f'An error occurred during upload mode processing for index {idx} with document type {upload_type}'
                            f'Please review the error message and stack trace for more details.') from e

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
                self.init_generate_descriptive_metadata()
            if self.retention_flag is True:
                self.get_retentions()
            try:
                if (self.UPLOAD_REF and self.UPLOAD_TYPE) in self.column_headers:
                    logger.debug(f'{self.UPLOAD_REF} and {self.UPLOAD_TYPE} columns found, proceeding with upload.')
                    data_dict = self.df[[self.UPLOAD_REF, self.UPLOAD_TYPE]].to_dict(orient='index')
                else:
                    logger.error(f'Ensure {self.UPLOAD_REF} and {self.UPLOAD_TYPE} columns are present in the headers.')
                    raise ValueError('Upload mode enabled but no Upload Reference or Upload Type column found. Please ensure you have a column with either Upload Reference or Upload Type as the header to use upload mode.')
            except KeyError as e:
                message = f'Key Error: {e} Please ensure that the "Upload Reference" and "Upload Type" columns are in your spreadsheet when using upload mode.'
                logger.exception(message)
                raise KeyError(message) from e
            except Exception:
                logger.exception('Error setting up upload mode')
                raise
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
