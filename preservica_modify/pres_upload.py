"""
Upload Mode for Preservica Mass Modify

Still needs work and testing...

Author: Christopher Prince
license: Apache License 2.0"
"""

import time, os, sys
from preservica_modify.common import check_nan
from pyPreservica.uploadAPI import UploadProgressCallback, complex_asset_package, simple_asset_package
from typing import Optional, Hashable
from lxml import etree
import logging
from preservica_modify.pres_modify import PreservicaMassMod

logger = logging.getLogger(__name__)

class PreservicaMassUpload(PreservicaMassMod):
    def __init__(self, config_path: str, spreadsheet_path: str, timeout: int = 2):
        super().__init__(config_path, spreadsheet_path)
        self.TIMEOUT = timeout

    def pax_create(self, idx: int, title: str, description: Optional[str] = None, security: Optional[str] = None, ident_dict: Optional[dict] = None, xml_data: Optional[dict] = None) -> Optional[complex_asset_package]:
        if idx is not None:
            pax_path, pax_preservation, pax_access = self.pax_lookup(idx)
        if pax_preservation is None:
            logger.warning(f'No preservation or access files found for PAX at index {idx}, skipping creation...')
            return None
        file_name = os.path.basename(str(pax_path)).split(".")[0] + ".pax"
                
        new_pax = complex_asset_package(title=title,
                                        description=description,
                                        security_tag=security,
                                        preservation_files_list= pax_preservation if pax_preservation else None,
                                        access_file=pax_path if pax_access else None,
                                        identifers=ident_dict,
                                        io_name=file_name,
                                        export_folder='.',
                                        Asset_Metadata=xml_data)
        return new_pax
    
    def so_create(self, upload_folder: str, title: str, description: Optional[str] = None, security: Optional[str] = None, ident_dict: Optional[dict] = None, xml_data: Optional[dict] = None, retention_policy: Optional[str]=None):
        if description is None:
            description = ""
        if security is None:
            security = ""
        new_folder = self.entity.create_folder(title=title,
                                               description=description,
                                               security_tag=security,
                                               parent=upload_folder)
        time.sleep(self.TIMEOUT)
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
        new_pa = self.entity.add_physical_asset(title=title, description=description, security_tag=security, parent=upload_folder)
        time.sleep(self.TIMEOUT)
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
            folder_path = check_nan(self.df[self.FILE_PATH].loc[idx])
            if folder_path is None:
                logger.exception(f'The upload path for {idx} is set to blank, please ensure a valid path is given')
                raise Exception(f'The upload path for {idx} is set to blank, please ensure a valid path is given')
            if os.path.isfile(folder_path):
                logger.warning(f'Folder marked as SO-Upload. Ignoring file...')
                pass
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
                if self.upload_flag is True:    
                    callback = UploadProgressCallback(folder_path)
                    self.upload.upload_zip_package(sip, folder=upload_folder, callback=callback)
                    if self.retention_flag is True:
                        time.sleep(self.TIMEOUT)
                        self.retention_update(sip, retention_policy)
                return upload_folder
            else:
                logger.warning(f'Path marked as SO-Upload is not a valid file or folder path. Ignoring entry...')
                pass
        else:
            logger.exception(f'The upload path column: {self.FILE_PATH} is not present in the spreadsheet; please ensure it is with a valid path to folder')
            raise Exception(f'The upload path column: {self.FILE_PATH} is not present in the spreadsheet; please ensure it is with a valid path to folder')
        
    # Can this be reworked with PyPreservica upload crawl method?
    def so_crawl(self, idx: int, upload_folder: str, title: str, description: Optional[str] = None, security: Optional[str] = None, ident_dict: Optional[dict] = None, xml_data: Optional[dict] = None, retention_policy: Optional[str]=None):
        time.sleep(self.TIMEOUT)
        if self.FILE_PATH in self.column_headers:
            folder_path = check_nan(self.df[self.FILE_PATH].loc[idx])
            if folder_path is None:
                logger.exception(f'The upload path for {idx} is set to blank, please ensure a valid path is given')
                raise Exception(f'The upload path for {idx} is set to blank, please ensure a valid path is given')
            if os.path.isdir(folder_path):
                
                def upload_loop(self, path_list, parent_folder = None):
                    for p in path_list:
                        f_list = [f.path for f in os.scandir(p) if os.path.isfile(f)]
                        d_list = [d.path for d in os.scandir(p) if os.path.isdir(d)]
                        sip = complex_asset_package(f_list, parent_folder=parent_folder)
                        upload = self.upload.upload_zip_package(sip, folder=parent_folder, callback=UploadProgressCallback(sip))
                        time.sleep(self.TIMEOUT)
                        self.upload_loop(d_list, upload)
                
                try:
                    self.entity.folder(upload_folder)
                except:
                    logger.exception(f'The reference given {upload_folder} is not a valid folder on your Preservica Server')
                    raise Exception(f'The reference given {upload_folder} is not a valid folder on your Preservica Server')
                
                upload_loop([folder_path],upload_folder)
            else:
                logger.warning(f'Folder marked as SO-Crawl. Ignoring file...')
                pass
        else:
            logger.exception(f'The upload path column: {self.FILE_PATH} is not present in the spreadsheet; please ensure it is with a valid path to folder')
            raise Exception(f'The upload path column: {self.FILE_PATH} is not present in the spreadsheet; please ensure it is with a valid path to folder')

    def so_crawl_alt(self, idx: int, upload_folder: str, title: str, description: Optional[str] = None, security: Optional[str] = None, ident_dict: Optional[dict] = None, xml_data: Optional[dict] = None, retention_policy: Optional[str]=None):
        time.sleep(self.TIMEOUT)
        if self.FILE_PATH in self.column_headers:
            folder_path = check_nan(self.df[self.FILE_PATH].loc[idx])
            if folder_path is None:
                logger.exception(f'The upload path for {idx} is set to blank, please ensure a valid path is given')
                raise Exception(f'The upload path for {idx} is set to blank, please ensure a valid path is given')
            if os.path.isdir(folder_path):
                try:
                    self.entity.folder(upload_folder)
                except:
                    logger.exception(f'The reference given {upload_folder} is not a valid folder on your Preservica Server')
                    raise Exception(f'The reference given {upload_folder} is not a valid folder on your Preservica Server'  )
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
                try:
                    self.entity.folder(upload_folder)
                except:
                    logger.exception(f'The reference given {upload_folder} is not a valid folder on your Preservica Server')
                    raise Exception(f'The reference given {upload_folder} is not a valid folder on your Preservica Server')
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
                try:
                    self.entity.folder(upload_folder)
                except:
                    logger.exception(f'The reference given {upload_folder} is not a valid folder on your Preservica Server')
                    raise Exception(f'The reference given {upload_folder} is not a valid folder on your Preservica Server')
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

    def process_upload_row(self, idx: Hashable, upload_folder: str, doc_type: str):
        """
        Testing do not use!
        Main processing function for upload mode.
        """
        time.sleep(self.TIMEOUT)
        try:
            title, description, security = self.xip_lookup(idx)
            if title is None:
                title = self.df[self.FILE_PATH].loc[idx].split(os.sep)[-1]
            ident_dict = self.ident_lookup(idx, self.IDENTIFIER_DEFAULT)
            retention_policy = self.retention_lookup(idx)
            xml_dict = {}
            xmls = self.generate_descriptive_metadata(idx, self.xml_files)
            if xmls is not None:
                for x in xmls:
                    ns = x.keys()[0]
                    assert isinstance(ns, str)
                    new_xml = x.get(ns)
                    assert isinstance(new_xml, etree._ElementTree)
                    xml_dict.update({ns: new_xml})                
            
            if doc_type == "PAX-Create":
                new_pax = self.pax_create(idx, title, description, security, ident_dict, xml_dict)
                return new_pax
            if doc_type == "SO-Create":
                return self.so_create(idx,upload_folder,title,description,security,ident_dict,xml_dict,retention_policy) 
                
            elif doc_type == "PA-Create": 
                return self.pa_create(idx,upload_folder,title,description,security,ident_dict,xml_dict,retention_policy)

            #elif doc_type == "SO-PAX-Upload":
            #    new_pax = self.so_pax_upload(idx,upload_folder,title,description,security,ident_dict)
            #    return new_pax

            elif doc_type == "SO-Upload":
                return self.so_upload(idx,upload_folder,title,description,security,ident_dict, xml_dict, retention_policy)
            
            elif doc_type == "SO-Crawl":
                return self.so_crawl(idx,upload_folder,title,description,security,ident_dict)

            elif doc_type == "SO-Crawl-Alt":
                return self.so_crawl_alt(idx,upload_folder,title,description,security,ident_dict)

            elif doc_type == "IO-PAX-Upload":
               return self.io_pax_upload(idx,upload_folder,title,description,security,ident_dict,xml_dict,retention_policy)
                                    
            elif doc_type == "IO-Upload":
                return self.io_upload(idx,upload_folder,title,description,security,ident_dict,xml_dict,retention_policy)
            else:
                logger.warning(f'Upload mode for {doc_type} is not currently supported, skipping...')
                return None
        except Exception as e:
            logger.exception(f'An error occurred during upload mode processing for index {idx} with document type {doc_type}'
                             f'Please review the error message and stack trace for more details.'
                             f'Error message: {str(e)}')
            raise Exception(f'An error occurred during upload mode processing for index {idx} with document type {doc_type}'
                            f'Please review the error message and stack trace for more details.') from e
