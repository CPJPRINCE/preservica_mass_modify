from preservica_modify.pres_modify import EntityType, PreservicaMassMod
import pandas as pd
from lxml import etree


class DummyEntity:
    def __init__(self, reference: str, entity_type):
        self.reference = reference
        self.entity_type = entity_type
        self.title = "old-title"
        self.description = "old-description"
        self.security_tag = "old-security"


class DummyAssignment:
    def __init__(self, policy_reference: str):
        self.policy_reference = policy_reference


class DummyEntityAPI:
    def __init__(self):
        self.deleted_assets = []
        self.deleted_folders = []
        self.saved_entities = []
        self.updated_idents = []
        self.added_idents = []
        self.deleted_idents = []
        self.security_calls = []
        self.added_metadata = []
        self.updated_metadata = []
        self.folder_objects = {}
        self.moved = []
        self.metadata_existing = None

    def asset(self, ref: str):
        return f"asset:{ref}"

    def folder(self, ref: str):
        class FolderObj:
            def __init__(self, reference):
                self.reference = reference
                self.title = f"folder-{reference}"

        folder_obj = FolderObj(ref)
        self.folder_objects[ref] = folder_obj
        return folder_obj

    def delete_asset(self, *args):
        self.deleted_assets.append(args)

    def delete_folder(self, *args):
        self.deleted_folders.append(args)

    def save(self, ent):
        self.saved_entities.append(ent)

    def security_tag_async(self, ent, security):
        self.security_calls.append((ent.reference, security))

    def identifiers_for_entity(self, _ent):
        return [("code", "OLD")]

    def update_identifiers(self, ent, key, value):
        self.updated_idents.append((ent.reference, key, value))

    def add_identifier(self, ent, key, value):
        self.added_idents.append((ent.reference, key, value))

    def delete_identifiers(self, ent, key, value):
        self.deleted_idents.append((ent.reference, key, value))

    def metadata_for_entity(self, _ent, _ns):
        return self.metadata_existing

    def add_metadata(self, ent, ns, xml_text):
        self.added_metadata.append((ent.reference, ns, xml_text))

    def update_metadata(self, ent, ns, xml_text):
        self.updated_metadata.append((ent.reference, ns, xml_text))

    def move_async(self, entity, dest_folder):
        self.moved.append((entity.reference, dest_folder.reference))


class DummyRetentionAPI:
    def __init__(self, assignments):
        self._assignments = assignments
        self.removed = []
        self.added = []
        self.policy_calls = []

    def assignments(self, ent):
        return self._assignments

    def remove_assignments(self, assignment):
        self.removed.append(assignment)

    def add_assignments(self, ent, policy):
        self.added.append((ent, policy))

    def policy(self, ref: str):
        self.policy_calls.append(ref)
        return f"policy:{ref}"


def make_instance() -> PreservicaMassMod:
    instance = PreservicaMassMod.__new__(PreservicaMassMod)
    instance.dummy_flag = False
    instance.blank_override = False
    instance.credentials_file = None
    instance.manager_username = None
    instance.manager_password = None
    instance.delete_flag = True
    instance.entity = DummyEntityAPI()
    instance.retention = DummyRetentionAPI([])
    instance.policy_dict = []
    instance.upload_flag = False
    return instance


def test_delete_update_returns_false_when_flag_disabled() -> None:
    instance = make_instance()
    instance.delete_flag = False

    result = instance.delete_update(0, DummyEntity("ref-1", EntityType.ASSET))

    assert result is False


def test_delete_update_raises_permission_error_without_auth() -> None:
    instance = make_instance()
    instance.delete_lookup = lambda idx: True

    try:
        instance.delete_update(0, DummyEntity("ref-1", EntityType.ASSET))
    except PermissionError:
        pass
    else:
        raise AssertionError("Expected PermissionError when delete requested without manager auth")


def test_delete_update_deletes_asset_with_manager_auth() -> None:
    instance = make_instance()
    instance.delete_lookup = lambda idx: True
    instance.manager_username = "manager@example.com"

    result = instance.delete_update(0, DummyEntity("ref-1", EntityType.ASSET))

    assert result is True
    assert len(instance.entity.deleted_assets) == 1


def test_delete_update_deletes_folder_with_manager_auth() -> None:
    instance = make_instance()
    instance.delete_lookup = lambda idx: True
    instance.manager_username = "manager@example.com"

    result = instance.delete_update(0, DummyEntity("ref-folder", EntityType.FOLDER))

    assert result is True
    assert len(instance.entity.deleted_folders) == 1


def test_retention_update_raises_lookup_for_missing_policy() -> None:
    instance = make_instance()
    instance.policy_dict = []

    try:
        instance.retention_update(DummyEntity("ref-2", EntityType.ASSET), "Unknown")
    except LookupError:
        pass
    else:
        raise AssertionError("Expected LookupError when retention policy is not found")


def test_retention_update_replaces_existing_assignments() -> None:
    instance = make_instance()
    assn = DummyAssignment("old-ref")
    instance.retention = DummyRetentionAPI([assn])
    instance.policy_dict = [{"Name": "Keep7", "Reference": "new-ref"}]

    instance.retention_update(DummyEntity("ref-3", EntityType.ASSET), "Keep7")

    assert instance.retention.removed == [assn]
    assert instance.retention.policy_calls == ["new-ref"]
    assert len(instance.retention.added) == 1


def test_xip_update_updates_fields_and_saves() -> None:
    instance = make_instance()
    ent = DummyEntity("ref-xip", EntityType.ASSET)

    instance.xip_update(ent, title="new-title", description="new-desc", security="new-sec")

    assert ent.title == "new-title"
    assert ent.description == "new-desc"
    assert instance.entity.security_calls == [("ref-xip", "new-sec")]
    assert instance.entity.saved_entities == [ent]


def test_ident_update_deletes_existing_identifier_on_blank_override() -> None:
    instance = make_instance()
    instance.blank_override = True
    ent = DummyEntity("ref-ident", EntityType.ASSET)

    instance.ident_update(ent, {"code": None})

    assert instance.entity.deleted_idents == [("ref-ident", "code", "OLD")]


def test_xml_update_adds_metadata_when_missing() -> None:
    instance = make_instance()
    ent = DummyEntity("ref-xml", EntityType.ASSET)
    xml_new = etree.ElementTree(etree.Element("root"))

    instance.entity.metadata_existing = None
    instance.xml_update(ent, "urn:test", xml_new)

    assert len(instance.entity.added_metadata) == 1
    assert instance.entity.added_metadata[0][0] == "ref-xml"


def test_xml_update_updates_metadata_when_existing() -> None:
    instance = make_instance()
    ent = DummyEntity("ref-xml2", EntityType.ASSET)
    xml_new = etree.ElementTree(etree.Element("root"))

    instance.entity.metadata_existing = "<root><a>old</a></root>"
    instance.xml_update(ent, "urn:test", xml_new)

    assert len(instance.entity.updated_metadata) == 1
    assert instance.entity.updated_metadata[0][0] == "ref-xml2"


def test_xml_update_upload_flag_forces_add_path() -> None:
    instance = make_instance()
    instance.upload_flag = True
    ent = DummyEntity("ref-upload", EntityType.ASSET)
    xml_new = etree.ElementTree(etree.Element("root"))

    instance.entity.metadata_existing = "<root><x>y</x></root>"
    instance.xml_update(ent, "urn:test", xml_new)

    assert len(instance.entity.added_metadata) == 1
    assert instance.entity.updated_metadata == []


def test_move_update_valid_uuid_calls_move_async() -> None:
    instance = make_instance()
    instance.move_flag = True
    instance.MOVETO_FIELD = "Move to"
    valid_uuid = "11111111-1111-1111-1111-111111111111"
    instance.df = pd.DataFrame({"Move to": [valid_uuid]})

    ent = DummyEntity("ref-move", EntityType.ASSET)
    instance.move_update(0, ent)

    assert instance.entity.moved == [("ref-move", valid_uuid)]


def test_move_update_invalid_destination_raises() -> None:
    instance = make_instance()
    instance.move_flag = True
    instance.MOVETO_FIELD = "Move to"
    instance.df = pd.DataFrame({"Move to": ["not-a-guid"]})

    ent = DummyEntity("ref-move", EntityType.ASSET)
    try:
        instance.move_update(0, ent)
    except ValueError:
        pass
    else:
        raise AssertionError("Expected ValueError for invalid move destination")