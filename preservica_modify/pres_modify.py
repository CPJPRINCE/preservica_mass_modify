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
from common import *

class PreservicaMassMod():
    def __init__(self,
                 excel_file: str,
                 metadata_dir: str,
                 blank_override: bool = False,
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
        """
        Class init
        """
        self.metadata_dir = metadata_dir
        self.metadata_flag = xml_method
        self.dummy_flag = dummy
        self.blank_override = blank_override
        self.delete_flag = delete
        self.descendants_flag = descendants
        
        self.upload_flag = True

        config = configparser.ConfigParser()
        config.read(options_file,encoding='utf-8')

        self.ENTITY_REF=config['options']['ENTITY_REF']
        self.DOCUMENT_TYPE=config['options']['DOCUMENT_TYPE']
        self.TITLE_FIELD=config['options']['TITLE_FIELD']
        self.DESCRIPTION_FIELD=config['options']['DESCRIPTION_FIELD']
        self.SECURITY_FIELD=config['options']['SECURITY_FIELD']
        self.RETENTION_FIELD=config['options']['RETENTION_FIELD']
        self.MOVETO_FIELD=config['options']['MOVETO_FIELD']
        self.DELETE_FIELD=config['options']['DELETE_FIELD']
        self.IDENTIFIER_FIELD=config['options']['IDENTIFIER_FIELD']
        self.IDENTIFIER_DEFAULT=config['options']['IDENTIFIER_DEFAULT']        
        self.PRES_UPLOAD_FIELD=config['options']['PRES_UPLOAD_FIELD']
        self.ACCESS_UPLOAD_FIELD=config['options']['ACCESS_UPLOAD_FIELD']

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
                    self.content = ContentAPI(credentials_path=self.credentials_file)
                    self.retention = RetentionAPI(credentials_path=self.credentials_file)
                    self.upload = UploadAPI(credentials_path=self.credentials_file)
                    print('Successfully logged into Preservica')
            elif None in (self.username,self.password,self.server):
                print('A Username, Password or Server has not been provided... Please try again...')
                time.sleep(5)
                raise SystemExit()
            else:
                self.content = ContentAPI(username=self.username, password=self.password,server=self.server, tenant=self.tenant)
                self.entity = EntityAPI(username=self.username, password=self.password,server=self.server, tenant=self.tenant)
                self.retention = RetentionAPI(username=self.username, password=self.password,server=self.server, tenant=self.tenant)
                self.upload = UploadAPI(username=self.username, password=self.password,server=self.server, tenant=self.tenant)
                print('Successfully logged into Preservica')
        except Exception as e: 
            print('Failed to login to Preservica...')
            print(f'Exception: {e}')
            time.sleep(5)
            raise SystemExit()

    def set_input_flags(self):
        """
        Sets the flags.
        """
        self.title_flag = False
        self.description_flag = False
        self.security_flag = False
        self.retention_flag = False
        self.dest_flag = False
        if self.TITLE_FIELD in self.column_headers:
            self.title_flag = True
        if self.DESCRIPTION_FIELD in self.column_headers:
            self.description_flag = True
        if self.SECURITY_FIELD in self.column_headers:
            self.security_flag = True
        if self.RETENTION_FIELD in self.column_headers:
            self.retention_flag = True
        if self.MOVETO_FIELD in self.column_headers:
            self.dest_flag = True

    def get_retentions(self):
        """
        Retrieves retention policies from Preservica and parses them into a dict
        """
        self.policies = self.retention.policies()
        self.policy_dict = [{"Name": f.name, "Reference": f.reference} for f in self.policies.get_results()]

    def xml_merge(self, xml_a: ET.Element, xml_b: ET.Element, x_parent = None):
        """
        Merges an two xml's together. xml_b overwrites xml_a. 
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
    

    def ident_update(self, idx: pd.Index, e: Entity, key_default: str = None):
        try:
            if idx.empty:
                ident = None
            else:
                for header in self.column_headers:
                    if any(s in header for s in {self.IDENTIFIER_FIELD,'Archive_Reference','Accession_Reference'}):
                        ident = self.df[header].loc[idx].item()
                        if f'{self.IDENTIFIER_FIELD}:' in header:
                            key_name = str(header).rsplit(':')[-1]
                        elif key_default is None:
                            if 'Archive_Reference' in header:
                                key_name = key_default
                            elif 'Accession_Reference' in header:
                                key_name = "accref"
                            elif self.IDENTIFIER_FIELD in header:
                                key_name = key_default
                        else:
                            key_name = key_default
                        if str(ident).lower() in {"nan", "nat"} or not ident:
                            ident = None
                        xip_idents = self.entity.identifiers_for_entity(e)
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
                    else:
                        pass
        except Exception as e:
            print('Error looking up Identifiers')
            raise SystemError()

    def xip_update(self, idx: pd.Index, e: Entity, title_override: str = None, description_override: str = None, security_override: str = None):
        """
        Uses the pandas index to retreieve data from the "Title, Description and Security" columns.

        Makes the calls on 

        Has Override variables to use an override, mainly for applying to descdendants. 
        """
        
        try:
            title = None
            description = None
            security = None
            if title_override:
                title = title_override
            if description_override:
                description = description_override
            if security_override:
                security = security_override
            if not any([title_override,description_override,security_override]):
                if idx.empty:
                    title = None
                    description = None
                    security = None
                else:
                        if self.title_flag:
                            title = self.df[self.TITLE_FIELD].loc[idx].item()
                            if str(title).lower() in {"nan","nat"}:
                                title = None
                        else:
                            title = None
                        if self.description_flag:
                            description = self.df[self.DESCRIPTION_FIELD].loc[idx].item()
                            if str(description).lower() in {"nan","nat"}:
                                description = None
                            if str(description).lower() in {"nan","nat"} and self.blank_override is True:
                                description = ""
                        else:
                            description = None
                        if self.security_flag:
                            security = self.df[self.SECURITY_FIELD].loc[idx].item()
                            if str(security).lower() in {"nan","nat"}:
                                security = None
                        else:
                            security = None
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
            return title,description,security
        except Exception as e:
            print('Error Looking up XIP Metadata')
            raise SystemError()

    def retention_update(self, idx: pd.Index, e: Entity):
        """
        Uses the pandas index to retreieve data from the "Retention Policy" column

        Matched agasint the policies obtained in the obtain_retentions function.
        """
        try:
            if idx.empty:
                rp = None
            else:
                if self.retention_flag:
                    rp = self.df[self.RETENTION_FIELD].loc[idx].item()
                    assignments = self.retention.assignments(e)
                    if str(rp).lower() in {"nan","nat"} and self.blank_override:
                        rp = None
                        if any(assignments):
                            for ass in assignments:
                                if self.dummy_flag is True:
                                    print(f"Updating {e.reference}, removing Retention Policy: {ass.policy_reference}")
                                else:
                                    self.retention.remove_assignments(ass)
                    elif str(rp).lower() in {"nan","nat"}:
                        rp = None
                        pass
                    else:
                        d = [d for d in self.policy_dict if d.get('Name') == rp]
                        if len(d) > 1:
                            print('Multiple Retention Policies found, not doing anything...')
                        elif len(d) == 1:
                            if any(assignments):
                                for ass in assignments:
                                    if self.dummy_flag is True:
                                        print(f"Updating {e.reference} Removing Retention Policy (part of update): {ass.reference, ass.name}")
                                    else:
                                        self.retention.remove_assignments(ass)
                            if self.dummy_flag is True:
                                print(f"Updating {e.reference} Adding Retention Policy: {d[0].get('Reference'), d[0].get('Name')}")
                            else:
                                self.retention.add_assignments(e,self.retention.policy(d[0].get('Reference')))
                        elif len(d) == 0:
                            print('No Policy Found...')
        except Exception as e:
            print('Error looking up Retention')
            raise SystemError()

    def init_generate_descriptive_metadata(self):
        """
        Initiation for the generate_descriptive_metadata function. Seperated to avoid unecessary repetition.

        Compares the Column headers in the spreadsheet agasint the XML's in the Metadata Directory.
        """
        self.xml_files = []
        for file in os.scandir(self.metadata_dir):
            if file.name.endswith('.xml'):
                """
                Generates info on the elements of the XML Files placed in the Metadata directory.
                Composed as a list of dictionaries.
                """
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

                """
                Compares the column headers in the Spreadsheet against the headers. Filters out non-matching data.
                """
                list_xml = []
                for elem_dict in elements_list:
                    if elem_dict.get('Name') in self.column_headers or elem_dict.get('Path') in self.column_headers:
                        list_xml.append({"Name": elem_dict.get('Name'), "XName": elem_dict.get('XName'), "Namespace": elem_dict.get('Namespace'), "Path": elem_dict.get('Path')})
            if len(list_xml) > 0:
                self.xml_files.append({'data': list_xml, 'localname': root_element_ln, 'localns': root_element_ns, 'xmlfile': path})
 
    def generate_descriptive_metadata(self, idx: pd.Index, xml_file: dict):
        """
        Generates the xml file based on the returned list of xml_files from the init function.
        """
        list_xml = xml_file.get('data')
        localname = xml_file.get('localname')
        """
        Composes the data into an xml file.
        """
        if len(list_xml):
            if not idx.empty:
                xml_new = ET.parse(xml_file.get('xmlfile'))
                for elem_dict in list_xml:
                    name = elem_dict.get('Name')
                    path = elem_dict.get('Path')
                    ns = elem_dict.get('Namespace')
                    try:
                        if self.metadata_flag in {'e', 'exact'}:
                            val = self.df.loc[idx, path].values[0]
                        elif self.metadata_flag in {'f', 'flat'}:
                            val = self.df.loc[idx, name].values[0]
                        if pd.isnull(val):
                            continue
                        else:
                            if pd.api.types.is_datetime64_dtype(val):
                                val = pd.to_datetime(val)
                                val = datetime.strftime(val, "%Y-%m-%dT%H:%M:%S.000Z")
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
                    if str(val).lower() in {"nan", "nat"}:
                        continue
                    if self.metadata_flag in {'e','exact'}:
                        n = path.replace(localname + ":", f"{{{ns}}}")
                        elem = xml_new.find(f'./{n}')
                    elif self.metadata_flag in {'f', 'flat'}:
                        n = name.split(':')[-1]
                        elem = xml_new.find(f'.//{{{ns}}}{n}')
                    elem.text = str(val)    
                self.xml_new = xml_new
            else:
                pass
        else:
            pass
            
    def xml_update(self, e: Entity, ns: str):
        """
        Makes the call on Preservica's API using pyPreservica to update, remove or add metadata from given entity.

        The namespace must also be passed - this is retrieved automatically. 
        """
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
        """
        if self.dest_flag is True:
            if idx.empty:
                pass
            else: 
                dest = self.df[self.MOVETO_FIELD].loc[idx].item()
                if str(dest).lower() in {"nan","nat"}:
                    pass
                else:
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
                delete_conf = self.df[self.DELETE_FIELD].loc[idx].item()
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

        title = check_nan(self.df[self.TITLE_FIELD].loc[idx].item())
        description = check_nan(self.df[self.DESCRIPTION_FIELD].loc[idx].item())
        security = check_nan(self.df[self.SECURITY_FIELD].loc[idx].item())
        if title is None:
            print('Error...')
            raise SystemExit 
        if doc_type == "SO-Create":
            self.entity.create_folder(title=title,description=description,security_tag=security,parent=upload_folder)
        elif doc_type == "SO-Upload":
            if self.PRES_UPLOAD_FIELD and self.ACCESS_UPLOAD_FIELD in self.column_headers:
                pres_folder_path = str(self.df[self.PRES_UPLOAD_FIELD].loc[idx].item())
                access_folder_path = str(self.df[self.ACCESS_UPLOAD_FIELD].loc[idx].item())
                if any([check_nan(pres_folder_path), check_nan(access_folder_path)] is None):
                    print(f'Either Access or Upload path for {idx} is set to blank, please ensure a valid path is given for both')
                    time.sleep(5)
                    raise SystemExit()
                ###Path...
            if self.PRES_UPLOAD_FIELD in self.column_headers:
                folder_path = str(self.df[self.PRES_UPLOAD_FIELD].loc[idx].item())
                if check_nan(folder_path):
                    print(f'The upload path for {idx} is set to blank, please ensure a valid path is given')
                    time.sleep(5)
                    raise SystemExit()
                if os.path.isfolder(folder_path):
                    try:
                        self.entity.folder(upload_folder)
                    except:
                        print(f'The reference given {upload_folder} is not a valid folder on your Preservica Server')
                        time.sleep(5)
                        raise SystemError()
                    file_list = [pth.path for pth in os.scandir(folder_path)]
                    sip = complex_asset_package(file_list,parent_folder=upload_folder)
                    callback = UploadProgressCallback(folder_path)
                    self.upload.upload_zip_package(sip, folder=upload_folder, callback=callback)
                else:
                    print(f'The upload path for {idx} is not directed to a valid file')
                    time.sleep(5)
                    raise SystemExit()
            else:
                print(f'The upload path column: {self.PRES_UPLOAD_FIELD} is not present in the spreadsheet; please ensure it is with a valid path to folder')
                time.sleep(5)
                raise SystemExit()
        elif doc_type == "IO-SimpleUpload":
            if self.PRES_UPLOAD_FIELD in self.column_headers:
                file_path = str(self.df[self.PRES_UPLOAD_FIELD].loc[idx].item())
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
                        sip = simple_asset_package(file_path)
                    callback = UploadProgressCallback(file_path)
                    self.upload.upload_zip_package(sip, folder=upload_folder, callback=callback)
                else:
                    print(f'The upload path for {idx} is not directed to a valid file')
                    time.sleep(5)
                    raise SystemExit()
            else:
                print(f'The upload path column: {self.PRES_UPLOAD_FIELD} is not present in the spreadsheet; please ensure it is with a valid path to folder')
                time.sleep(5)
                raise SystemExit()
            
        elif doc_type == "PA-Create": 
            self.entity.add_physical_asset(title=title,description=description,security_tag=security,parent=upload_folder)
        else:
            raise SystemError()

    def main(self):
        """
        Main loop.
        """
        self.set_input_flags()
        self.init_generate_descriptive_metadata()
        if self.retention_flag is True:
            self.get_retentions()
        if self.DOCUMENT_TYPE in self.df and self.upload_flag is True:
            data_dict = self.df[[self.ENTITY_REF, self.DOCUMENT_TYPE]].to_dict('index')
        else:
            data_dict = self.df[self.ENTITY_REF].to_dict('index')
        for idx in data_dict:
            reference_dict = data_dict.get(idx)
            ref = reference_dict.get(self.ENTITY_REF)
            print(f"Processing: {ref}")
            if self.DOCUMENT_TYPE in reference_dict:
                doc_type = reference_dict.get(self.DOCUMENT_TYPE)
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
            title,description,security = self.xip_update(idx, e)
            self.ident_update(idx, e, self.IDENTIFIER_DEFAULT)
            for xml in self.xml_files:
                self.generate_descriptive_metadata(idx, xml)
                ns = xml.get('localns')
                self.xnames = [x.get('XName') for x in xml.get('data')]
                self.xml_update(e,ns=ns)
            if e.entity_type == EntityType.ASSET:
                self.retention_update(idx,e)
            self.dest_update(idx, e)
            if self.DOCUMENT_TYPE in reference_dict and self.upload_flag is True:
                self.upload_processing(idx, ref, doc_type)
                continue
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
                                self.ident_update(idx, de, self.IDENTIFIER_DEFAULT)
                            # xml_update reutilises the metadata generated above - it will merge the data from the spreadsheet, with the data from the descendant's Entity...
                            if any(x in ["include-retention","include-all"] for x in self.descendants_flag):
                                self.retention_update(idx, de)
                            if any(x in ["include-all","include-title","include-description","include-security"] for x in self.descendants_flag):
                                if not "include-title" in self.descendants_flag:
                                    title = None
                                if not any(x in ["include-all","include-description"] for x in self.descendants_flag):
                                    description = None
                                if not any(x in ["include-all","include-security"] for x in self.descendants_flag):
                                    security = None
                                #To note idx is not being utilised in the function for descendantsm, as title override; overrides it...
                                self.xip_update(idx,de,title_override=title,description_override=description,security_override=security)
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
                                self.ident_update(idx, de)
                            if any(x in ["include-title","include-description","include-security"] for x in self.descendants_flag):
                                if not "include-title" in self.descendants_flag:
                                    title = None
                                if not any(x in ["include-all","include-description"] for x in self.descendants_flag):
                                    description = None
                                if not any(x in ["include-all","include-security"] for x in self.descendants_flag):
                                    security = None
                                #idx is not doing anything here
                                self.xip_update(idx,de,title_override=title,description_override=description,security_override=security)