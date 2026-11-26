

import logging
from typing import Hashable, Optional, Union
import pandas as pd
from .common import AuthenticatedPreservicaClient
from pyPreservica import Entity, EntityType, EntityAPI, RetentionAPI, UploadAPI, WorkflowAPI, AdminAPI

logger = logging.getLogger(__name__)

class UpdateHandler(AuthenticatedPreservicaClient):
    def __init__(self, credentials_file: Optional[str] = None, manager_username: Optional[str] = None, manager_password: Optional[str] = None, dummy_flag: bool = False, blank_override: bool = False, move_flag: bool = False, delete_flag: bool = False):
        super().__init__(df, entity_api, retention_api, upload_api, workflow_api, admin_api, credentials_file, manager_username, manager_password)
        self.dummy_flag = dummy_flag
        self.blank_override = blank_override
        self.move_flag = move_flag
        self.delete_flag = delete_flag
    
    def xip_update(self, ent: Entity, title: Optional[str] = None, description: Optional[str] = None, security: Optional[str] = None):
        try:
            if title:
                logger.info(f"Updating {ent.reference} Title from {ent.title} to {title}")
                if self.dummy_flag is False:
                    ent.title = title
            if description:
                logger.info(f"Updating {ent.reference} Description from {ent.description} to {description}")
                if self.dummy_flag is False:
                    ent.description = description
            if security:
                logger.info(f"Updating {ent.reference} Security Tag from {ent.security_tag} to {security}")
                if self.dummy_flag is False:
                    self.entity.security_tag_async(ent, security)
            if any([title,description]):
                self.entity.save(ent)
        except Exception:
            logger.exception('Error updating XIP metadata')
            raise
    
    def ident_update(self, ent: Entity, ident_dict: Union[dict,None]):
        if ident_dict is None:
            return
        try:
            xip_idents = self.entity.identifiers_for_entity(ent)
            for items in ident_dict.items():
                key_name = items[0]
                ident = items[1]
                if ident is not None:
                    if any(x[0] for x in xip_idents if x[0] == key_name):
                        old_ident = [x[1] for x in xip_idents if x[0] == key_name][0]
                        logger.info(f'Updating {ent.reference} Updating identifier {key_name, old_ident} to: {key_name, ident}')
                        if self.dummy_flag is False:
                            self.entity.update_identifiers(ent,key_name,str(ident))
                    else: 
                        logger.info(f'Updating {ent.reference} Adding identifier: {key_name, ident}')
                        if self.dummy_flag is False:
                            self.entity.add_identifier(ent,key_name,str(ident))
                else:
                    if self.blank_override is True:
                        if any(x[0] for x in xip_idents if x[0] == key_name):
                            old_ident = [x[1] for x in xip_idents if x[0] == key_name][0]
                            logger.info(f'Updating {ent.reference} Deleting identifier {key_name, old_ident}')
                            if self.dummy_flag is False:
                                self.entity.delete_identifiers(ent,key_name,str(old_ident))
                        else:
                            pass
        except Exception:
            logger.exception('Error updating identifiers')
            raise
                    
    def retention_update(self, ent: Entity, retention_policy: Optional[str] = None):
        try:
            if retention_policy is not None:
                assignments = self.retention.assignments(ent)
                policies = [policy for policy in self.policy_dict if policy.get('Name') == retention_policy]
                if len(policies) > 1:
                    logger.warning(f'Multiple Retention Policies found for reference: {ent.reference}, taking no action.')
                elif len(policies) == 1:
                    policy = policies[0].get('Reference', None)
                    policy_name = policies[0].get('Name', None)
                    if policy is None:
                        logger.error(f'Retention policy reference not found for name: {retention_policy}')
                        raise LookupError(f'Retention policy reference not found for name: {retention_policy}')
                    if any(assignments):
                        for ass in assignments:
                            logger.info(f"Updating {ent.reference} Removing retention policy: {ass.policy_reference}")
                            if self.dummy_flag is False:
                                self.retention.remove_assignments(ass)
                    logger.info(f"Updating {ent.reference} Adding retention policy: {policy, policy_name}")
                    if self.dummy_flag is False:
                        self.retention.add_assignments(ent,self.retention.policy(policy))
                elif len(policies) == 0:
                    logger.error(f'Retention policy not found for name: {retention_policy}')
                    raise LookupError(f'Retention policy not found for name: {retention_policy}')

            elif retention_policy is None and self.blank_override is True:
                assignments = self.retention.assignments(ent)
                if any(assignments):
                    for ass in assignments:
                        logger.info(f"Updating {ent.reference} Removing Retention Policy: {ass.policy_reference}")
                        if self.dummy_flag is False:
                            self.retention.remove_assignments(ass)
            else:
                pass                    
        except Exception:
            logger.exception(f'Error updating retention: {ent.reference}')
            raise
                    
    def xml_update(self, ent: Entity, ns: str, xml_new: etree._ElementTree):
        """
        Makes the call on Preservica's API using pyPreservica to update, remove or add metadata from given entity.

        The namespace must also be passed - this is retrieved automatically. 

        :param e: Entity to act upon
        :param ns: Namespace of XML being updated
        """
        try:
            #Change so it's dynamic - not only self.upload_flag - also indent_update needs same treatment
            if self.upload_flag:
                ent_meta = None
            else:
                ent_meta = self.entity.metadata_for_entity(ent, ns)
            # Check if metadata exists for the entity
            if ent_meta is None:
                xml_to_upload = etree.tostring(xml_new)
                logger.info(f"Updating {ent.reference} Adding Metadata for: {ns}")
                logger.debug(f'New XML Metadata: {xml_to_upload}')
                if self.dummy_flag is False:
                    self.entity.add_metadata(ent, ns, xml_to_upload.decode('utf-8'))
            # Metadata exists, merge and update
            else:
                xml_to_upload = etree.tostring(self.xml_merge(etree.fromstring(ent_meta), xml_new))
                logger.info(f"Updating {ent.reference} Updating Metadata for: {ns}")
                logger.debug(f'Updated XML Metadata: {xml_to_upload}')
                if self.dummy_flag is False:
                    self.entity.update_metadata(ent, ns, xml_to_upload.decode('utf-8'))
        except Exception:
            logger.exception('Error updating XML metadata')
            raise

    def move_update(self, idx: int, ent: Entity):
        """
        Uses the pandas index to retrieve data from the "Move To" column. Initiates a move. 

        :param idx: Pandas Index to lookup
        :param ent: Entity to act upon
        """
        if self.move_flag is True:
            dest = check_nan(self.df[self.MOVETO_FIELD].loc[idx])
            if dest is not None:
                if re.search("^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$", dest):
                    dest_folder = self.entity.folder(dest)
                    logger.info(f'Moving Entity: {ent.reference}, {ent.title}, to: {dest_folder.reference}, {dest_folder.title}')
                    if self.dummy_flag is False:
                        self.entity.move_async(entity=ent, dest_folder=dest_folder)
                else:
                    logger.error(f'Reference: {ent.reference} in "Move To" is formatted incorrectly: {dest}')
                    raise ValueError(f'Reference: {ent.reference} in "Move To" is formatted incorrectly: {dest}')

    def delete_update(self, idx: Hashable, ent: Entity, delete_confirmation: bool = False, credentials_file: str = None, manager_username: str = None, manager_password: str = None):
        """
        Uses the pandas index to retrieve data from the "Delete" column. If True initiates a Delete.
        Requires use of a .credentials file.

        Delete Flag must also be set.
        """
        try:
            if delete_confirmation is True and (manager_username or credentials_file):
                if ent.entity_type == EntityType.ASSET:
                    logger.info(f'Deleting Asset: {ent.reference}')
                    if self.dummy_flag is False:
                        self.entity.delete_asset(self.entity.asset(ent.reference),"Deleted by Preservica Mass Modify","Deleted by Preservica Mass Modify", credentials_file, manager_username, manager_password)
                    return True
                elif ent.entity_type == EntityType.FOLDER:
                    logger.info(f'Deleting Folder: {ent.reference}')
                    if self.dummy_flag is False:
                        self.entity.delete_folder(self.entity.folder(ent.reference),"Deleted by Preservica Mass Modify","Deleted by Preservica Mass Modify", self.credentials_file,  self.manager_username, self.manager_password)
                    return True
            elif delete_confirmation is True and not (manager_username or credentials_file):
                logger.error('Delete requested but no manager username or credentials file provided.')
                raise PermissionError('Delete requested but no manager username or credentials file provided.')
            else:
                logger.info(f'Delete flag set to True for {ent.reference} but delete confirmation not given, skipping delete.')
                return False
        except KeyError as e:
            message = f'Failed to delete entity due to missing key: {e}'
            logger.exception(message)
            raise KeyError(message) from e
        except Exception:
            logger.exception('Failed to delete entity')
            raise
