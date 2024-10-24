
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
                 server: str = None):
        if excel_file.endswith(".xlsx"):
            self.df = pd.read_excel(excel_file)
        elif excel_file.endswith(".csv"):
            self.df = pd.read_csv(excel_file)
            nl = [ch for ch in self.df if "date" in ch]
            self.df[nl] = self.df[nl].apply(lambda x: pd.to_datetime(x,format='mixed'))
        self.column_headers = list(self.df)
        self.metadata_dir = metadata_dir
        self.metadata_flag = xml_method
        self.merge_add = "merge"
        self.dummy_flag = dummy
        self.blank_override = blank_override
        self.descendants_flag = descendants
        self.username = username
        self.password = password
        self.server = server
        self.login_preservica()

    def login_preservica(self):
        if None in (self.username,self.password,self.server):
            print('A Username, Password or Server has not been provided... Please try again...')
            time.sleep(5)
            raise SystemExit()
        try:
            self.content = ContentAPI(username=self.username, password=self.password,server=self.server)
            self.entity = EntityAPI(username=self.username, password=self.password,server=self.server)
            self.retention = RetentionAPI(username=self.username, password=self.password,server=self.server)
            print('Successfully logged into Preservica')
        except Exception as e: 
            print('Failed to login to Preservica...')
            print(f'Exception: {e}')
            time.sleep(5)
            raise SystemExit()

    def set_input_flags(self):
        self.title_flag = False
        self.description_flag = False
        self.security_flag = False
        self.retention_flag = False
        if 'Title' in self.column_headers:
            self.title_flag = True
        if 'Description' in self.column_headers:
            self.description_flag = True
        if 'Security' in self.column_headers:
            self.security_flag = True
        if 'Retention Policy' in self.column_headers:
            self.retention_flag = True

    def get_retentions(self):
        self.policies = self.retention.policies()
        self.policy_dict = [{"Name": f.name, "Reference": f.reference} for f in self.policies]

    # Possibly defunct Function - incorporated into xml_merge...
    # Test if behaviour is now different.
    def xml_add(self, xml_a,xml_b, x_child=None):
        for b_child in xml_b.findall('./'):
            a_child = xml_a.find('./' + b_child.tag)
            if a_child is None:
                a_child = ET.SubElement(x_child,b_child.tag)
                a_child.text = b_child.text
            else:
                x_child = a_child
                self.xml_add(a_child,b_child,x_child)
        print(ET.tostring(xml_a))
        return ET.tostring(xml_a)

    def xml_merge(self, xml_a: ET.Element, xml_b: ET.Element, x_parent = None):
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
    

    def ident_update(self, idx: pd.Index, e: Entity, key_override: str = None):
        try:
            if idx.empty:
                ident = None
            else:
                for header in self.column_headers:
                    if any(s in header for s in {'Identifier','Archive_Reference','Accession_Reference'}):
                        ident = self.df[header].loc[idx].item()
                        if 'Identifier:' in header:
                            key_name = str(header).rsplit(':')[-1]
                        elif key_override is None:
                            if 'Archive_Reference' in header:
                                key_name = "code"
                            elif 'Accession_Reference' in header:
                                key_name = "accref"
                            elif 'Identifier' in header:
                                key_name = "code"
                        else:
                            key_name = key_override
                    else:
                        ident = None
                    if self.blank_override is True and str(ident).lower() in {"nan","nat"}:
                        ident = False
                    elif str(ident).lower() in {"nan", "nat"} or not ident:
                        ident = None
                    if ident is not None:
                        xip_idents = self.entity.identifiers_for_entity(e)
                        if self.blank_override is True and ident is False:
                            if any(x[0] for x in xip_idents if x[0] == key_name):
                                oldident = [x[1] for x in xip_idents if x[0] == key_name][0]
                                if self.dummy_flag:
                                    print(f'Updating {e.reference} Deleting Identifer {key_name, oldident}')
                                else:
                                    self.entity.update_identifiers(e,key_name,str(ident))
                        else:
                            if any(x[0] for x in xip_idents if x[0] == key_name):
                                oldident = [x[1] for x in xip_idents if x[0] == key_name][0]
                                if self.dummy_flag:
                                    print(f'Updating {e.reference} Updating identifer {key_name, oldident} to: {key_name, ident}')
                                else:
                                    self.entity.update_identifiers(e,key_name,str(ident))
                            else: 
                                if self.dummy_flag:
                                    print(f'Updating {e.reference} Adding identifier: {key_name, ident}')
                                else:
                                    self.entity.add_identifier(e,key_name,str(ident))
                
        except Exception as e:
            print('Error looking up Identifiers')
            raise SystemError()

    def xip_update(self, idx: pd.Index, e: Entity, title_override: str = None, description_override: str = None, security_override: str = None):
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
                            title = self.df['Title'].loc[idx].item()
                            if str(title).lower() in {"nan","nat"}:
                                title = None
                        else:
                            title = None
                        if self.description_flag:
                            description = self.df['Description'].loc[idx].item()
                            if str(description).lower() in {"nan","nat"}:
                                description = None
                            if str(description).lower() in {"nan","nat"} and self.blank_override is True:
                                description = ""
                        else:
                            description = None
                        if self.security_flag:
                            security = self.df['Security'].loc[idx].item()
                            if str(security).lower() in {"nan","nat"}:
                                security = None
                        else:
                            security = None
            if title:
                if self.dummy_flag:
                    print(f"Updating {e.reference} Title from {e.title} to {title}")
                else:
                    e.title = title
            if description:
                if self.dummy_flag:
                    print(f"Updating {e.reference} Description from {e.description} to {description}")
                else:
                    e.description = description
            if security:
                if self.dummy_flag:
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
        try:
            if idx.empty:
                rp = None
            else:
                if self.retention_flag:
                    rp = self.df['Retention Policy'].loc[idx].item()
                    assignments = self.retention.assignments(e)
                    if str(rp).lower() in {"nan","nat"} and self.blank_override:
                        rp = None
                        if any(assignments):
                            for ass in assignments:
                                if self.dummy_flag:
                                    print(f"Updating {e.reference}, removing Retention Policy: {ass.reference, ass.name}")
                                else:
                                    self.retention.remove_assignments(ass)
                    elif str(rp).lower() in {"nan","nat"}:
                        rp = None
                        pass
                    else:
                        d = [d for d in self.policy_dict if d.get('Name') == rp]
                        if len(d) != 1:
                            print('Error Retention Policy was not found')
                            ### Parse Path
                        else:
                            if any(assignments):
                                for ass in assignments:
                                    if self.dummy_flag:
                                        print(f"Updating {e.reference} Removing Retention Policy (part of update): {ass.reference, ass.name}")
                                    else:
                                        self.retention.remove_assignments(ass)
                            if self.dummy_flag:
                                print(f"Updating {e.reference} Adding Retention Policy: {d[0].get('Reference'), d[0].get('Name')}")
                            else:
                                self.retention.add_assignments(e,self.retention.policy(d[0].get('Reference')))
        except Exception as e:
            print('Error looking up Retention')
            raise SystemError()

    def init_generate_descriptive_metadata(self):
        self.xml_files = []
        for file in os.scandir(self.metadata_dir):
            if file.name.endswith('xml'):
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
 
    def generate_descriptive_metadata(self, idx: int, xml_file: dict):
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
                                val = datetime.strftime(val, "%Y-%m-%dT%H-%M-%S.00Z")
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
        emeta = self.entity.metadata_for_entity(e, ns)
        if emeta is None:
            xml_to_upload = ET.tostring(self.xml_new)
            if self.dummy_flag:
                print(f"Updating {e.reference} Adding Metadata")
            else:
                self.entity.add_metadata(e,ns,xml_to_upload.decode('utf-8'))
        else:
            if self.merge_add == "merge":
                xml_to_upload = self.xml_merge(ET.fromstring(emeta), self.xml_new)
                xml_to_upload_b = self.xml_add(ET.fromstring(emeta), self.xml_new)
                print(xml_to_upload)
                print(xml_to_upload_b)
            elif self.merge_add == "add":
                xml_to_upload = self.xml_add(ET.fromstring(emeta), self.xml_new)
            if self.dummy_flag:
                print(f"Updating {e.reference} Updating Metadata")
            else:
                self.entity.update_metadata(e, ns, xml_to_upload.decode('utf-8'))

    def main(self):
        self.set_input_flags()
        self.init_generate_descriptive_metadata()
        if self.retention_flag is True:
            self.get_retentions()
        if "Document type" in self.df:
            reference_list = self.df[['Entity Ref', 'Document type']].to_dict('records')
        else:
            reference_list = self.df['Entity Ref'].to_dict('records')
        for reference in reference_list:
            ref = reference.get('Entity Ref')
            print(f"Processing: {ref}")
            if "Document type" in reference:
                if reference.get('Document type') == "SO":
                    e = self.entity.folder(ref)
                elif reference.get('Document type') == "IO":
                    e = self.entity.asset(ref)
            else:
                try:
                    e = self.entity.asset(ref)
                except:
                    e = self.entity.folder(ref)
            idx = self.df['Entity Ref'].index[self.df['Entity Ref'] == ref]
            if e.entity_type == EntityType.ASSET:
                self.retention_update(idx,e)
            title,description,security = self.xip_update(idx, e)
            self.ident_update(idx, e)
            for xml in self.xml_files:
                self.generate_descriptive_metadata(idx, xml)
                ns = xml.get('localns')
                self.xnames = [x.get('XName') for x in xml.get('data')]
                self.xml_update(e,ns=ns)
                if self.descendants_flag:
                    if e.entity_type == EntityType.FOLDER:
                        for ed in self.entity.all_descendants(e):
                            print(f"Processing Descendant: {ed.reference}")
                            de = self.entity.entity(ed.entity_type,ed.reference)
                            if "include-assets" in self.descendants_flag and de.entity_type == EntityType.ASSET:
                                if not any(x in ["include-xml","include-retention","include-description","include-security","include-title","include-identifiers"] for x in self.descendants_flag):
                                    print('No data to process. Ensure you select 1 option of data to edit')
                                if any(x in ["include-xml","include-all"] for x in self.descendants_flag):
                                    self.xml_update(de,ns=ns)
                                if any(x in ["include-identifiers","include-all"] for x in self.descendants_flag):
                                    self.ident_update(idx, de)
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
                                    #idx is not doing anything here
                                    self.xip_update(idx,de,title_override=title,description_override=description,security_override=security)
                            elif "include-folders" in self.descendants_flag and de.entity_type == EntityType.FOLDER:
                                if "include-xml" in self.descendants_flag:
                                    self.xml_update(de,ns=ns)
                                if "include-identifiers" in self.descendants_flag:
                                    self.ident_update(idx, de)
                                if any(x in ["include-title","include-description","include-security"] for x in self.descendants_flag):
                                    if not "include-title" in self.descendants_flag:
                                        title = None
                                    if not "include-description" in self.descendants_flag:
                                        description = None
                                    if not "include-security" in self.descendants_flag:
                                        security = None
                                    #idx is not doing anything here
                                    self.xip_update(idx,de,title_override=title,description_override=description,security_override=security)