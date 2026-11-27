"""
Updates Mixin for Preservica Mass Modify

Author: Christopher Prince
license: Apache License 2.0"
"""

import logging
from typing import Optional, Union, Hashable
from pyPreservica import Entity, EntityType
import re
from lxml import etree
from preservica_modify.common import check_nan

logger = logging.getLogger(__name__)

class UpdatesMixin:

    def xip_update(self, ent: Entity, title: Optional[str] = None, description: Optional[str] = None, security: Optional[str] = None):
        try:
            if title != ent.title:
                if check_nan(title) is None:
                    logger.info(f'Title for {ent.reference} is blank, this cannot be updated')
                else: 
                    logger.info(f"Updating {ent.reference} Title from {ent.title} to {title}")
                    if self.dummy_flag is False:
                        ent.title = title
                        self.entity.save(ent)
            else:
                logger.info(f"No update required for {ent.reference} Title")
            if description != ent.description:
                if check_nan(description) is None and self.blank_override is True:
                    logger.info(f'Updating Description for {ent.reference} to blank')
                    if self.dummy_flag is False:
                        ent.description = ""
                        self.entity.save(ent)
                elif check_nan(description) is None and self.blank_override is False:
                    logger.info(f'Description for {ent.reference} is blank, override is not set - not updating')
                else:
                    logger.info(f"Updating {ent.reference} Description from {ent.description} to {description}")
                    if self.dummy_flag is False:
                        ent.description = str(description)
                        self.entity.save(ent)
            else:
                logger.info(f"No update required for {ent.reference} Description")
            if security != ent.security_tag:
                if check_nan(security) is None:
                    logger.info(f"Security Tag for {ent.reference} is blank, this cannot be updated")
                else:
                    logger.info(f"Updating {ent.reference} Security Tag from {ent.security_tag} to {security}")
                    if self.dummy_flag is False:
                        self.entity.security_tag_async(ent, security)
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
                    policy_ref = policies[0].get('Reference', None)
                    policy_name = policies[0].get('Name', None)
                    if policy_ref is None:
                        logger.error(f'Retention policy reference not found for name: {retention_policy}')
                        raise LookupError(f'Retention policy reference not found for name: {retention_policy}')
                    if any(assignments):
                        for ass in assignments:
                            logger.info(f"Updating {ent.reference} Removing retention policy: {ass.policy_reference}")
                            if self.dummy_flag is False:
                                self.retention.remove_assignments(ass)
                    logger.info(f"Updating {ent.reference} Adding retention policy: {policy_ref, policy_name}")
                    if self.dummy_flag is False:
                        self.retention.add_assignments(ent,self.retention.policy(policy_ref))
                elif len(policies) == 0:
                    logger.error(f'Retention policy not found for name: {retention_policy}')
                    raise LookupError(f'Retention policy not found for name: {retention_policy}')

            elif retention_policy is None and self.blank_override is True:
                assignments = list(self.retention.assignments(ent))
                if len(assignments) > 0:
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

    def delete_update(self, idx: Hashable, ent: Entity):
        """
        Uses the pandas index to retrieve data from the "Delete" column. If True initiates a Delete.
        Requires use of a .credentials file.

        Delete Flag must also be set.
        """
        try:
            if self.delete_flag is True:
                delete_conf = self.delete_lookup(idx)
                if delete_conf is True and (self.manager_username or self.credentials_file):
                    if ent.entity_type == EntityType.ASSET:
                        logger.info(f'Deleting Asset: {ent.reference}')
                        if self.dummy_flag is False:
                            self.entity.delete_asset(self.entity.asset(ent.reference),"Deleted by Preservica Mass Modify","Deleted by Preservica Mass Modify", self.credentials_file, self.manager_username, self.manager_password)
                        return True
                    elif ent.entity_type == EntityType.FOLDER:
                        logger.info(f'Deleting Folder: {ent.reference}')
                        if self.dummy_flag is False:
                            self.entity.delete_folder(self.entity.folder(ent.reference),"Deleted by Preservica Mass Modify","Deleted by Preservica Mass Modify", self.credentials_file,  self.manager_username, self.manager_password)
                        return True
                if delete_conf is True:
                    logger.error('Delete requested but no manager username or credentials file provided.')
                    raise PermissionError('Delete requested but no manager username or credentials file provided.')
            else:
                return False
        except KeyError as e:
            message = f'Failed to delete entity due to missing key: {e}'
            logger.exception(message)
            raise KeyError(message) from e
        except Exception:
            logger.exception('Failed to delete entity')
            raise

    def delete_metadata_update(self, ent: Entity, schema: str):
        """
        Initiates a Delete Metadata action on the given entity for the specified schema.
        """
        try:
            logger.info(f'Deleting metadata for Entity: {ent.reference}, {ent.title}, Schema: {schema}')
            if self.dummy_flag is False:
                self.entity.delete_metadata(ent, schema)
        except Exception:
            logger.exception(f'Failed to delete metadata for Entity: {ent.reference}, {ent.title}, Schema: {schema}')
            raise

    def delete_all_metadata_update(self, ent: Entity):
        """
        Initiates a Delete All Metadata action on the given entity.
        """
        try:
            logger.info(f'Deleting all metadata for Entity: {ent.reference}, {ent.title}')
            if self.dummy_flag is False:
                for schema in set(ent.metadata.values()):
                    self.entity.delete_metadata(ent, schema)
        except Exception:
            logger.exception(f'Failed to clear metadata for Entity: {ent.reference}, {ent.title}')
            raise