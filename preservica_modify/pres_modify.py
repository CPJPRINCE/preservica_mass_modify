"""
Preservica Mass Modification tool.

This tool is utilised to modify existing data on Preservica through the use of spreadsheets.
Allows for modification of XIP and XML data.

Author: Christopher Prince
license: Apache License 2.0"
"""


from pyPreservica import *
import pandas as pd
from lxml import etree as ET
from datetime import datetime
import time, os
from preservica_modify.common import *

class PreservicaMassMod:
    """
    Mass Modification Class
    """
    def __init__(self,
                 excel_file: str,
                 metadata_dir: str,
                 blank_override: bool = False,
                 upload_mode: bool = False,
                 xml_method: str = "flat",
                 descendants: set = None,
                 dummy: bool = False,
                 username: str = None,
                 password: str = None,
                 server: str = None,
                 tenant: str = None,
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

        config = configparser.ConfigParser()
        config.read(options_file,encoding='utf-8')

        global ENTITY_REF
        ENTITY_REF=config['options']['ENTITY_REF']
        global DOCUMENT_TYPE
        DOCUMENT_TYPE=config['options']['DOCUMENT_TYPE']
        global TITLE_FIELD
        TITLE_FIELD=config['options']['TITLE_FIELD']
        global DESCRIPTION_FIELD 
        DESCRIPTION_FIELD=config['options']['DESCRIPTION_FIELD']
        global SECURITY_FIELD
        SECURITY_FIELD=config['options']['SECURITY_FIELD']
        global RETENTION_FIELD
        RETENTION_FIELD=config['options']['RETENTION_FIELD']
        global MOVETO_FIELD
        MOVETO_FIELD=config['options']['MOVETO_FIELD']
        global DELETE_FIELD
        DELETE_FIELD=config['options']['DELETE_FIELD']
        global IDENTIFIER_FIELD
        IDENTIFIER_FIELD=config['options']['IDENTIFIER_FIELD']
        global IDENTIFIER_DEFAULT
        IDENTIFIER_DEFAULT=config['options']['IDENTIFIER_DEFAULT']        
        global PRES_UPLOAD_FIELD
        PRES_UPLOAD_FIELD=config['options']['PRES_UPLOAD_FIELD']
        global ACCESS_UPLOAD_FIELD
        ACCESS_UPLOAD_FIELD=config['options']['ACCESS_UPLOAD_FIELD']
        
        print(credentials)

        if os.path.isfile(credentials):
            self.credentials_file = credentials
        else:
            self.credentials_file = None
        self.username = username
        self.password = password
        self.server = server
        self.tenant = tenant
        self.login_preservica()
        if excel_file.endswith(".xlsx"):
            self.df = pd.read_excel(excel_file)
        elif excel_file.endswith(".csv"):
            self.df = pd.read_csv(excel_file)
            nl = [ch for ch in self.df if "date" in ch]
            self.df[nl] = self.df[nl].apply(lambda x: pd.to_datetime(x,format='mixed'))
        self.column_headers = list(self.df)

    def login_preservica(self):
        """
        Logs into Preservica. Either through manually logging in.
        """
        try:
            if self.credentials_file:
                print('Using credentials file.')
                self.entity = EntityAPI(credentials_path=self.credentials_file)
                self.retention = RetentionAPI(credentials_path=self.credentials_file)
                self.upload = UploadAPI(credentials_path=self.credentials_file)
                self.workflow = WorkflowAPI(credentials_path=self.credentials_file)
                print('Successfully logged into Preservica')
            elif None in (self.username,self.password,self.server):
                print('A Username, Password or Server has not been provided... Please try again...')
                time.sleep(5)
                raise SystemExit()
            else:
                self.entity = EntityAPI(username=self.username, password=self.password,server=self.server, tenant=self.tenant)
                self.retention = RetentionAPI(username=self.username, password=self.password,server=self.server, tenant=self.tenant)
                self.upload = UploadAPI(username=self.username, password=self.password,server=self.server, tenant=self.tenant)
                self.workflow = WorkflowAPI(username=self.username, password=self.password,server=self.server, tenant=self.tenant)
                print('Successfully logged into Preservica')
        except Exception as e: 
            print('Failed to login to Preservica...')
            print(f'Exception: {e}')
            time.sleep(5)
            raise SystemExit()

    def set_input_flags(self):
        """
        Sets the input flags
        """
        self.title_flag = False
        self.description_flag = False
        self.security_flag = False
        self.retention_flag = False
        self.dest_flag = False
        if TITLE_FIELD in self.column_headers:
            self.title_flag = True
        if DESCRIPTION_FIELD in self.column_headers:
            self.description_flag = True
        if SECURITY_FIELD in self.column_headers:
            self.security_flag = True
        if RETENTION_FIELD in self.column_headers:
            self.retention_flag = True
        if MOVETO_FIELD in self.column_headers:
            self.dest_flag = True

    def get_retentions(self) -> dict:
        """
        Retrieves retention policies from Preservica and parses them into a dict
        """
        self.policies = self.retention.policies()
        self.policy_dict = [{"Name": f.name, "Reference": f.reference} for f in self.policies.get_results()]

    def xml_merge(self, xml_a: ET.Element, xml_b: ET.Element, x_parent: ET.Element = None) -> str:
        """
        Merges two xml's together. xml_b overwrites xml_a, unless xml_b's element contains a blank value.
        If blank_override is set, blank value will override xml_a's element.

        :param xml_a: xml to merge into
        :param xml_b: xml to merge from
        :param x_parent: xml parent, only use for recursion
        """
        for b_child in xml_b.findall('./'):
            a_child = xml_a.find('./' + b_child.tag)
            if a_child is not None:
                x_parent = None
                if a_child.text is not None:
                    if b_child.text:
                        a_child.text = b_child.text
                    elif self.blank_override is True and b_child.tag in self.xnames:
                        a_child.text = None
                    else:
                        a_child.text = a_child.text
                else:
                    if b_child.text:
                        a_child.text = b_child.text
                    else:
                        pass
            else:
                x_parent = xml_a
                a_child = ET.SubElement(xml_a,b_child.tag)
                if b_child.text:
                    a_child.text = b_child.text
                else:
                    a_child.text = None                
            if b_child.getchildren():
                self.xml_merge(a_child,b_child,x_parent)
        return ET.tostring(xml_a,pretty_print=True)
    

    def ident_lookup(self, idx: pd.Index, key_default: str = None) -> dict:
        """
        Uses the pandas index to retreieve data from the "Identifer","Archive_Reference", columns. Sets identifers in Entity.
        "Archive_Reference" & "Accession_Reference" are hard-set.

        :param idx: Pandas Index to lookup
        :param e: Entity to act upon
        :param key_default: Set the default key value  
        """
        try:
            # if idx.empty:
            #     ident = None
            # else:
            ident_dict = {}
            for header in self.column_headers:
                if any(s in header for s in {IDENTIFIER_FIELD,'Archive_Reference','Accession_Reference'}):
                    if f'{IDENTIFIER_FIELD}:' in header:
                        key_name = str(header).rsplit(':')[-1]
                    else:
                        if 'Archive_Reference' in header:
                            key_name = key_default
                        elif 'Accession_Reference' in header:
                            key_name = "accref"
                        elif IDENTIFIER_FIELD in header:
                            key_name = key_default
                        else:
                            key_name = key_default
                    ident = check_nan(self.df[header].loc[idx])
                    print(key_name,ident)
                    ident_dict.update({key_name:ident})
                    
            return ident_dict
        except Exception as e:
            print('Error looking up Identifiers')
            raise SystemError()
        
    def ident_update(self, e: Entity, ident_dict: dict):
        try:
            xip_idents = self.entity.identifiers_for_entity(e)
            for items in ident_dict.items():
                key_name = items[0]
                ident = items[1]
                if ident is not None:
                    if any(x[0] for x in xip_idents if x[0] == key_name):
                        oldident = [x[1] for x in xip_idents if x[0] == key_name][0]
                        if self.dummy_flag is True:
                            print(f'Updating {e.reference} Updating identifier {key_name, oldident} to: {key_name, ident}')
                        else:
                            self.entity.update_identifiers(e,key_name,str(ident))
                    else: 
                        if self.dummy_flag is True:
                            print(f'Updating {e.reference} Adding identifier: {key_name, ident}')
                        else:
                            self.entity.add_identifier(e,key_name,str(ident))
                else:
                    if self.blank_override is True:
                        if any(x[0] for x in xip_idents if x[0] == key_name):
                            oldident = [x[1] for x in xip_idents if x[0] == key_name][0]
                            if self.dummy_flag is True:
                                print(f'Updating {e.reference} Deleting identifier {key_name, oldident}')
                            else:
                                self.entity.delete_identifiers(e,key_name,str(oldident))
                        else:
                            pass
        except Exception as e:
            print('Error updating Identifiers')
            raise SystemError()

    def xip_lookup(self, idx: pd.Index):
        """
        Uses the pandas index to retreieve data from the "Title, Description and Security" columns. Sets data in Entity.

        Has Override variables to use an override, mainly for applying to descdendants.

        :param idx: Pandas Index to lookup
        :param e: Entity to act upon
        :param title_override: Set Override for title - main use for descendants  
        :param description_override: Set Override for description - main use for descendants  
        :param title_override: Set Override for security - main use for descendants  
        """
    
        try:
            title = None
            description = None
            security = None
            # if idx.empty:
            #     title = None
            #     description = None
            #     security = None
            # else:
            if self.title_flag:
                title = check_nan(self.df[TITLE_FIELD].loc[idx])
                print(title)
            if self.description_flag:
                description = check_nan(self.df[DESCRIPTION_FIELD].loc[idx].item())
                if description is None and self.blank_override is True:
                    description = ""
            if self.security_flag:
                security = check_nan(self.df[SECURITY_FIELD].loc[idx].item())
            return title,description,security
        except Exception as e:
            print('Error Looking up XIP Metadata')
            raise SystemError()
        
    def xip_update(self, e: Entity, title: str = None, description: str = None, security: str = None):
        try:
            if title:
                if self.dummy_flag is True:
                    print(f"Updating {e.reference} Title from {e.title} to {title}")
                else:
                    e.title = title
            if description:
                if self.dummy_flag is True:
                    print(f"Updating {e.reference} Description from {e.description} to {description}")
                else:
                    e.description = description
            if security:
                if self.dummy_flag is True:
                    print(f"Updating {e.reference} Security Tag from {e.security_tag} to {security}")
                else:
                    e.security_tag = security
            if any([title,description,security]):
                self.entity.save(e)
        except Exception as e:
            print('Error updating XIP Metadata')
            raise SystemError()

    def retention_lookup(self, idx: pd.Index):
        """
        Uses the pandas index to retreieve data from the "Retention Policy" column

        Matched agasint the policies obtained in the obtain_retentions function.

        :param idx: Pandas Index to lookup
        :param e: Entity to act upon
        """
        try:
            # if idx.empty:
            #     rp = None
            # else:
            if self.retention_flag:
                rp = check_nan(self.df[RETENTION_FIELD].loc[idx].item())
            else:
                rp = None    
            return rp
        except Exception as e:
            print('Error updating XIP Metadata')
            raise SystemError()
                    
    def retention_update(self, e: Entity, retention_policy: str = None):
        try:
            if retention_policy is not None:
                assignments = self.retention.assignments(e)
                policies = [policy for policy in self.policy_dict if policy.get('Name') == retention_policy]
                if len(policies) > 1:
                    print('Multiple Retention Policies found, not doing anything...')
                elif len(policies) == 1:
                    if any(assignments):
                        for ass in assignments:
                            if self.dummy_flag is True:
                                print(f"Updating {e.reference} Removing Retention Policy (part of update): {ass.reference, ass.name}")
                            else:
                                self.retention.remove_assignments(ass)
                    if self.dummy_flag is True:
                        print(f"Updating {e.reference} Adding Retention Policy: {policies[0].get('Reference'), policies[0].get('Name')}")
                    else:
                        self.retention.add_assignments(e,self.retention.policy(policies[0].get('Reference')))
                elif len(policies) == 0:
                    print('No Policy Found...')
            elif retention_policy is None and self.blank_override is True:
                    assignments = self.retention.assignments(e)
                    if any(assignments):
                        for ass in assignments:
                            if self.dummy_flag is True:
                                print(f"Updating {e.reference}, removing Retention Policy: {ass.policy_reference}")
                            else:
                                self.retention.remove_assignments(ass)
            else:
                pass                    
        except Exception as e:
            print('Error updating Retention')
            raise SystemError()

    def init_generate_descriptive_metadata(self):
        """
        Initiation for the generate_descriptive_metadata function. Seperated to avoid unecessary repetition.

        First takes xmls files in metadata_dir, generates a list of dicts of the elements in XML file. Then compares the Column headers in the spreadsheet against the XML's in the Metadata Directory.
        """
        self.xml_files = []
        for file in os.scandir(self.metadata_dir):
            list_xml = []
            if file.name.endswith('.xml'):
                path = os.path.join(self.metadata_dir, file)
                xml_file = ET.parse(path)
                root_element = ET.QName(xml_file.find('.'))
                root_element_ln = root_element.localname
                root_element_ns = root_element.namespace
                elements_list = []
                for elem in xml_file.findall('.//'):
                    elem_path = xml_file.getelementpath(elem)
                    elem = ET.QName(elem)
                    elem_ln = elem.localname
                    elem_ns = elem.namespace
                    elem_lnpath = elem_path.replace(f"{{{elem_ns}}}", root_element_ln + ":")
                    elements_list.append({"Name": root_element_ln + ":" + elem_ln, "XName": f"{{{elem_ns}}}{elem_ln}", "Namespace": elem_ns, "Path": elem_lnpath})
                for elem_dict in elements_list:
                    if elem_dict.get('Name') in self.column_headers or elem_dict.get('Path') in self.column_headers:
                        list_xml.append({"Name": elem_dict.get('Name'), "XName": elem_dict.get('XName'), "Namespace": elem_dict.get('Namespace'), "Path": elem_dict.get('Path')})
            if len(list_xml) > 0:
                self.xml_files.append({'data': list_xml, 'localname': root_element_ln, 'localns': root_element_ns, 'xmlfile': path})
 
    def generate_descriptive_metadata(self, idx: pd.Index, xml_file: dict):
        """
        Generates the xml file based on the returned list of xml_files from the init_generate_descriptive_metadata function.

        :param idx: Pandas Index to lookup
        :param xml_file: Dictionary of XML files created as part of init 
        """
        list_xml = xml_file.get('data')
        localname = xml_file.get('localname')
        if len(list_xml):
            if idx.empty:
                pass
            else:
                xml_new = ET.parse(xml_file.get('xmlfile'))
                for elem_dict in list_xml:
                    name = elem_dict.get('Name')
                    path = elem_dict.get('Path')
                    ns = elem_dict.get('Namespace')
                    try:
                        if self.metadata_flag in {'e', 'exact'}:
                            val = check_nan(self.df[path].loc[idx].item())
                        elif self.metadata_flag in {'f', 'flat'}:
                            val = check_nan(self.df[name].loc[idx].item())
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
                        elem.text = str(val)
                    except KeyError as e:
                        print('Key Error: please ensure column header\'s are an exact match...')
                        print(f'Missing Column: {e}')
                        print('Alternatively use flat mode...')
                        time.sleep(3)
                        raise SystemError()
                    except IndexError as e:
                        print("""Index Error; it is likely you have removed or added a file/folder to the directory \
                            after generating the spreadsheet. An opex will still be generated but with no xml metadata. \
                            To ensure metadata match up please regenerate the spreadsheet...""")
                        print(f'Error: {e}')
                        time.sleep(3)
                        break
                self.xml_new = xml_new
                return xml_new
            
    def xml_update(self, e: Entity, ns: str):
        """
        Makes the call on Preservica's API using pyPreservica to update, remove or add metadata from given entity.

        The namespace must also be passed - this is retrieved automatically. 

        :param e: Entity to act upon
        :param ns: Namespace of XML being updated
        """
        if self.upload_flag:
            emeta = None
        else:
            emeta = self.entity.metadata_for_entity(e, ns)
        if emeta is None:
            xml_to_upload = ET.tostring(self.xml_new)
            if self.dummy_flag is True:
                print(f"Updating {e.reference} Adding Metadata")
            else:
                self.entity.add_metadata(e,ns,xml_to_upload.decode('utf-8'))
        else:
            xml_to_upload = self.xml_merge(ET.fromstring(emeta), self.xml_new)
            if self.dummy_flag is True:
                print(f"Updating {e.reference} Updating Metadata")
            else:
                self.entity.update_metadata(e, ns, xml_to_upload.decode('utf-8'))

    def dest_update(self, idx: pd.Index, e: Entity):
        """
        Uses the pandas index to retreieve data from the "Move To" column. Intitaites a move. 

        :param idx: Pandas Index to lookup
        :param Enitty: Entity to act upon
        """
        if self.dest_flag is True:
            if idx.empty:
                pass
            else: 
                dest = check_nan(self.df[MOVETO_FIELD].loc[idx].item())
                if dest is not None:
                    if re.search("^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$",dest):
                        dest_folder = self.entity.folder(dest)
                        if self.dummy_flag is True:
                            print(f'Moving Entity: {e}, to: {dest_folder.reference, dest_folder.title}')
                        else:
                            self.entity.move_async(entity=e, dest_folder=dest_folder)
                    else:
                        print(f'Error: Reference in "Move To" is incorrect: {dest}')
                        raise SystemExit()

    def delete_lookup(self, idx: pd.Index, e:Entity):
        """
        Uses the pandas index to retrieve data from the "Delete" column. If True intitaites a Delete.
        Requires use of a .credentials file.

        Delete Flag must also be set.
        """
        if self.delete_flag is True:
            if idx.empty:
                pass
            else:
                delete_conf = self.df[DELETE_FIELD].loc[idx].item()
                if bool(delete_conf) is True:
                    if e.entity_type == EntityType.ASSET:
                        if self.dummy_flag is True:
                            print(f'Deleting Asset: {e}')
                        else:
                            self.entity.delete_asset(e)
                    elif e.entity_type == EntityType.FOLDER:
                        if self.dummy_flag is True:
                            print(f'Deleting Folder: {e}')
                        else:
                            self.entity.delete_folder(e)
    
    def upload_processing(self, idx: pd.Index, upload_folder: str, doc_type: str):
        """
        Testing do not use!
        """
        time.sleep(1)
        try:
            title, description,security = self.xip_lookup(idx)
            if title is None:
                print('Title is None... Error!!!')
                raise SystemExit 
            ident_dict = self.ident_lookup(idx, IDENTIFIER_DEFAULT)
            retention_policy = self.retention_lookup(idx)
            if doc_type not in {"SO-Create","PA-Create"}:
                for xml in self.xml_files:
                    new_xml = self.generate_descriptive_metadata(idx, xml)
                    ns = xml.get('localns')
                    xml_dict = {ns:new_xml}

            if doc_type == "SO-Create":
                new_folder = self.entity.create_folder(title=title,description=description,security_tag=security,parent=upload_folder)
                time.sleep(1)
                self.ident_update(new_folder,ident_dict)
                self.retention_update(new_pa,retention_policy)
                for xml in self.xml_files:
                    new_xml = self.generate_descriptive_metadata(idx, xml)
                    ns = xml.get('localns')
                    self.xml_update(new_folder,ns)
                return new_folder

            elif doc_type == "PA-Create": 
                new_pa = self.entity.add_physical_asset(title=title,description=description,security_tag=security,parent=upload_folder)
                time.sleep(1)
                self.ident_update(new_pa,ident_dict)
                self.retention_update(new_pa,retention_policy)
                for xml in self.xml_files:
                    new_xml = self.generate_descriptive_metadata(idx, xml)
                    ns = xml.get('localns')
                    self.xml_update(new_folder,ns)
                return new_pa

            elif doc_type == "SO-PAXUpload":
                if all({PRES_UPLOAD_FIELD, ACCESS_UPLOAD_FIELD} in self.column_headers):
                    pres_folder_path = check_nan(self.df[PRES_UPLOAD_FIELD].loc[idx].item())
                    access_folder_path = check_nan(self.df[ACCESS_UPLOAD_FIELD].loc[idx].item())
                    if pres_folder_path is None:
                        print(f'Either Access or Upload path for {idx} is set to blank, please ensure a valid path is given for both')
                        time.sleep(5)
                        raise SystemExit()
                    if all(os.path.isdir(pres_folder_path),os.path.isdir(access_folder_path)):
                        acc_file_list = [pth.path for pth in os.scandir(access_folder_path)]
                        pres_file_list = [pth.path for pth in os.scandir(pres_folder_path)]
                        sip = complex_asset_package(pres_file_list,acc_file_list,parent_folder=upload_folder,
                                                    Title=title,Description=description,SecurityTag=security,
                                                    Asset_Metadata=xml_dict,Identifiers=ident_dict,)
                        callback = UploadProgressCallback(sip)
                        self.upload.upload_zip_package(sip, folder=upload_folder, callback=callback)
                        return upload_folder
                    else:
                        print(f'File marked as {doc_type}. Ignoring...')
                        pass
                else:
                    print(f'The upload path columns: {PRES_UPLOAD_FIELD, ACCESS_UPLOAD_FIELD} are not present in the spreadsheet; please ensure they are with a valid path to folder')
                    time.sleep(5)
                    raise SystemExit()
                
            elif doc_type == "SO-Upload":    
                time.sleep(1)
                if PRES_UPLOAD_FIELD in self.column_headers:
                    folder_path = check_nan(self.df[PRES_UPLOAD_FIELD].loc[idx])
                    if folder_path is None:
                        print(f'The upload path for {idx} is set to blank, please ensure a valid path is given')
                        time.sleep(5)
                        raise SystemExit()
                    if os.path.isdir(folder_path):
                        try:
                            self.entity.folder(upload_folder)
                        except:
                            print(f'The reference given {upload_folder} is not a valid folder on your Preservica Server')
                            time.sleep(5)
                            raise SystemError()
                        file_list = [pth.path for pth in os.scandir(folder_path) if os.path.isfile(pth)]
                        sip = complex_asset_package(file_list,parent_folder=upload_folder)
                        callback = UploadProgressCallback(folder_path)
                        self.upload.upload_zip_package(sip, folder=upload_folder, callback=callback)
                        return upload_folder
                    else:
                        print(f'File marked as {doc_type}. Ignoring...')
                        pass
                else:
                    print(f'The upload path column: {PRES_UPLOAD_FIELD} is not present in the spreadsheet; please ensure it is with a valid path to folder')
                    time.sleep(5)
                    raise SystemExit()

            elif doc_type == "SO-Crawl":    
                time.sleep(1)
                if PRES_UPLOAD_FIELD in self.column_headers:
                    folder_path = check_nan(self.df[PRES_UPLOAD_FIELD].loc[idx])
                    if folder_path is None:
                        print(f'The upload path for {idx} is set to blank, please ensure a valid path is given')
                        time.sleep(5)
                        raise SystemExit()
                    if os.path.isdir(folder_path):
                        try:
                            self.entity.folder(upload_folder)
                        except:
                            print(f'The reference given {upload_folder} is not a valid folder on your Preservica Server')
                            time.sleep(5)
                            raise SystemError()
                        
                        def upload_loop(path_list,parent_folder = None):
                            for p in path_list:
                                f_list = [f.path for f in os.scandir(p) if os.path.isfile(f)]
                                d_list = [d.path for d in os.scandir(p) if os.path.isdir(d)]
                                sip = complex_asset_package(f_list,parent_folder=parent_folder)
                                callback = UploadProgressCallback(sip)
                                upload = self.upload.upload_zip_package(sip, folder=parent_folder, callback=callback)
                                print(upload)
                                time.sleep(2)
                                upload_loop(d_list, upload)

                        upload_loop([folder_path],upload_folder)
                    else:
                        print(f'File marked as {doc_type}. Ignoring...')
                        pass
                else:
                    print(f'The upload path column: {PRES_UPLOAD_FIELD} is not present in the spreadsheet; please ensure it is with a valid path to folder')
                    time.sleep(5)
                    raise SystemExit()

            elif doc_type == "IO-PAXUpload":
                if PRES_UPLOAD_FIELD and ACCESS_UPLOAD_FIELD in self.column_headers:
                    pres_file_path = check_nan(self.df[PRES_UPLOAD_FIELD].loc[idx])
                    access_file_path = check_nan(self.df[ACCESS_UPLOAD_FIELD].loc[idx])
                    if any([pres_file_path, access_file_path] is None):
                        print(f'Either Access or Upload path for {idx} is set to blank, please ensure a valid path is given for both')
                        time.sleep(5)
                        raise SystemExit()
                    if all(os.path.isfile(pres_file_path),os.path.isfile(access_file_path)):
                        sip = complex_asset_package(pres_file_path,acc_file_list,parent_folder=upload_folder)
                        self.upload.upload_zip_package_to_Azure(sip, folder=upload_folder, callback=UploadProgressCallback(sip))
                        return upload_folder
                    else:
                        print(f'Folder marked as {doc_type}. Ignoring...')
                        pass
                else:
                    print(f'The upload path columns: {PRES_UPLOAD_FIELD, ACCESS_UPLOAD_FIELD} are not present in the spreadsheet; please ensure they are with a valid path to a file')
                    time.sleep(5)
                    raise SystemExit()
                
            elif doc_type == "IO-Upload":
                if PRES_UPLOAD_FIELD in self.column_headers:
                    file_path = str(self.df[PRES_UPLOAD_FIELD].loc[idx].item())
                    if check_nan(file_path):
                        print(f'The upload path for {idx} is set to blank, please ensure a valid path is given')
                        time.sleep(5)
                        raise SystemExit()
                    if os.path.isfile(file_path):
                        try:
                            self.entity.folder(upload_folder)
                        except:
                            print(f'The reference given {upload_folder} is not a valid folder on your Preservica Server')
                            time.sleep(5)
                            raise SystemError()
                        if not file_path.endswith('.zip'):
                            sip = simple_asset_package(file_path,parent_folder=upload_folder)
                        else:
                            sip = file_path
                        self.upload.upload_zip_package(sip, folder=upload_folder, callback=UploadProgressCallback(sip))
                        return upload_folder
                    else:
                        print(f'Folder marked as {doc_type}. Ignoring...')
                        pass
                else:
                    print(f'The upload path column: {PRES_UPLOAD_FIELD} is not present in the spreadsheet; please ensure it is with a valid path to folder')
                    time.sleep(5)
                    raise SystemExit()
            else:
                print('Ignoring...')
                pass
        except:
            raise SystemError()

    def main(self):
        """
        Main loop.
        """
        self.set_input_flags()
        self.init_generate_descriptive_metadata()
        if self.retention_flag is True:
            self.get_retentions()
        if DOCUMENT_TYPE in self.df or self.upload_flag is True:
            try:
                data_dict = self.df[[ENTITY_REF, DOCUMENT_TYPE]].to_dict('index')
                last_ref = None                
            except KeyError as e:
                print('Key Error: Please ensure that the "Document Type" column is in your spreadsheet.')
                time.sleep(5)
                raise SystemExit()
        else:
            data_dict = self.df[ENTITY_REF].to_dict('index')
        for idx in data_dict:
            reference_dict = data_dict.get(idx)
            ref = check_nan(reference_dict.get(ENTITY_REF))
            print(f"Processing: {ref}")
            if self.upload_flag is True:
                doc_type = reference_dict.get(DOCUMENT_TYPE)
                #Ref is the upload folder reference.
                if ref is None or ref == "Use Parent":
                    self.upload_processing(idx, last_ref, doc_type)
                    time.sleep(1)               
                else:
                    last_ref = self.upload_processing(idx, ref, doc_type)
                continue            
            elif DOCUMENT_TYPE in reference_dict:
                doc_type = reference_dict.get(DOCUMENT_TYPE)
                if doc_type == "SO":
                    e = self.entity.folder(ref)
                elif doc_type == "IO":
                    e = self.entity.asset(ref)
            else:
                try:
                    e = self.entity.asset(ref)
                except:
                    e = self.entity.folder(ref)
            if self.delete_flag is True:
                self.delete_lookup(idx, e)
                continue
            title, description, security = self.xip_lookup(idx)
            self.xip_update(idx,title,description,security)
            self.ident_update(e, self.ident_lookup(idx, IDENTIFIER_DEFAULT))
            for xml in self.xml_files:
                self.generate_descriptive_metadata(idx, xml)
                ns = xml.get('localns')
                self.xnames = [x.get('XName') for x in xml.get('data')]
                self.xml_update(e,ns=ns)
            if e.entity_type == EntityType.ASSET:
                self.retention_update(e,self.retention_lookup(idx))
            self.dest_update(idx, e)
            """
            Descdenants handling
            """
            if self.descendants_flag:
                if e.entity_type == EntityType.FOLDER:
                    for ed in self.entity.all_descendants(e):
                        print(f"Processing Descendant: {ed.reference}")
                        de = self.entity.entity(ed.entity_type,ed.reference)
                        if "include-assets" in self.descendants_flag and de.entity_type == EntityType.ASSET:
                            if not any(x in ["include-xml","include-retention","include-description","include-security","include-title","include-identifiers"] for x in self.descendants_flag):
                                print('No data to process. Ensure you select 1 option of data to edit')
                            if any(x in ["include-xml","include-all"] for x in self.descendants_flag):
                                for xml in self.xml_files:
                                    ns = xml.get('localns')
                                    self.xml_update(e,ns=ns)
                            if any(x in ["include-identifiers","include-all"] for x in self.descendants_flag):
                                self.ident_update(de, self.ident_lookup(idx, IDENTIFIER_DEFAULT))
                            if any(x in ["include-retention","include-all"] for x in self.descendants_flag):
                                self.retention_update(de, self.retention_lookup(idx))
                            if any(x in ["include-all","include-title","include-description","include-security"] for x in self.descendants_flag):
                                if not "include-title" in self.descendants_flag:
                                    title = None
                                if not any(x in ["include-all","include-description"] for x in self.descendants_flag):
                                    description = None
                                if not any(x in ["include-all","include-security"] for x in self.descendants_flag):
                                    security = None
                                self.xip_update(de,title=title,description=description,security=security)
                        elif "include-folders" in self.descendants_flag and de.entity_type == EntityType.FOLDER:
                            if not any(x in ["include-xml","include-retention","include-description","include-security","include-title","include-identifiers"] for x in self.descendants_flag):
                                print('No data to process. Ensure you select 1 option of data to edit')
                                time.sleep(5)
                                raise SystemExit()
                            if any(x in ["include-xml","include-all"] for x in self.descendants_flag):
                                for xml in self.xml_files:
                                    ns = xml.get('localns')
                                    self.xml_update(e,ns=ns)
                            if any(x in ["include-identifiers","include-all"] for x in self.descendants_flag):
                                self.ident_update(de, self.ident_lookup(idx,IDENTIFIER_DEFAULT))
                            if any(x in ["include-title","include-description","include-security"] for x in self.descendants_flag):
                                if not "include-title" in self.descendants_flag:
                                    title = None
                                if not any(x in ["include-all","include-description"] for x in self.descendants_flag):
                                    description = None
                                if not any(x in ["include-all","include-security"] for x in self.descendants_flag):
                                    security = None
                                self.xip_update(de,title=title,description=description,security=security)
