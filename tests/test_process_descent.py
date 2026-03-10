from lxml import etree

from preservica_modify.pres_modify import EntityType, PreservicaMassMod


class DummyEntity:
    reference = "ref-1"

    def __init__(self, reference: str = "ref-1", entity_type=None):
        self.reference = reference
        self.entity_type = entity_type


class DescendantRef:
    def __init__(self, reference: str, entity_type):
        self.reference = reference
        self.entity_type = entity_type


class DummyEntityAPI:
    def __init__(self, descendants):
        self._descendants = descendants

    def all_descendants(self, _ent):
        return self._descendants

    def entity(self, entity_type, reference):
        return DummyEntity(reference=reference, entity_type=entity_type)


def build_instance() -> PreservicaMassMod:
    instance = PreservicaMassMod.__new__(PreservicaMassMod)
    instance.descendants_flag = {"include-xml"}
    instance.metadata_flag = "exact"
    instance.xml_files = []
    instance.xnames = []
    instance.retention_flag = False
    instance.IDENTIFIER_DEFAULT = "code"
    return instance


def test_process_descent_uses_only_string_xnames_and_xml_trees() -> None:
    instance = build_instance()

    good_tree = etree.ElementTree(etree.Element("root"))
    instance.generate_descriptive_metadata = lambda idx, files: [
        {
            "urn:test": good_tree,
            "xnames": ["{urn:test}a", None, 123],
        },
        {
            "urn:skip": None,
            "xnames": ["{urn:skip}b"],
        },
    ]

    calls: list[tuple[str, etree._ElementTree]] = []

    def fake_xml_update(entity, ns, xml_new):
        calls.append((ns, xml_new))

    instance.xml_update = fake_xml_update
    instance.ident_update = lambda *args, **kwargs: None
    instance.ident_lookup = lambda *args, **kwargs: None
    instance.xip_update = lambda *args, **kwargs: None
    instance.retention_update = lambda *args, **kwargs: None
    instance.retention_lookup = lambda *args, **kwargs: None

    instance._process_descent(0, DummyEntity(), None)

    assert instance.xnames == ["{urn:skip}b"]
    assert len(calls) == 1
    assert calls[0][0] == "urn:test"
    assert calls[0][1] is good_tree


def test_process_descent_raises_when_descendants_flag_missing() -> None:
    instance = build_instance()
    instance.descendants_flag = None

    try:
        instance._process_descent(0, DummyEntity(), EntityType.ASSET)
    except ValueError:
        pass
    else:
        raise AssertionError("Expected ValueError when descendants_flag is None")


def test_process_descent_raises_when_no_edit_options() -> None:
    instance = build_instance()
    instance.descendants_flag = {"include-assets"}

    try:
        instance._process_descent(0, DummyEntity(), EntityType.ASSET)
    except ValueError:
        pass
    else:
        raise AssertionError("Expected ValueError when no editable descendant options are selected")


def test_process_descent_calls_identifier_update() -> None:
    instance = build_instance()
    instance.descendants_flag = {"include-identifiers"}
    instance.metadata_flag = None

    ident_calls = []
    instance.ident_lookup = lambda idx, default: {"code": "ID-1"}
    instance.ident_update = lambda ent, ident: ident_calls.append((ent.reference, ident))
    instance.xip_update = lambda *args, **kwargs: None
    instance.retention_update = lambda *args, **kwargs: None

    descendant = DummyEntity(reference="child-1", entity_type=EntityType.ASSET)
    instance._process_descent(0, descendant, EntityType.ASSET)

    assert ident_calls == [("child-1", {"code": "ID-1"})]


def test_process_descent_calls_retention_for_assets_only() -> None:
    instance = build_instance()
    instance.descendants_flag = {"include-retention"}
    instance.metadata_flag = None
    instance.retention_flag = True

    retention_calls = []
    instance.retention_lookup = lambda idx: "Keep7"
    instance.retention_update = lambda ent, policy: retention_calls.append((ent.reference, policy))
    instance.ident_update = lambda *args, **kwargs: None
    instance.xip_update = lambda *args, **kwargs: None

    asset_desc = DummyEntity(reference="asset-child", entity_type=EntityType.ASSET)
    folder_desc = DummyEntity(reference="folder-child", entity_type=EntityType.FOLDER)

    instance._process_descent(0, asset_desc, EntityType.ASSET)
    instance._process_descent(0, folder_desc, EntityType.FOLDER)

    assert retention_calls == [("asset-child", "Keep7")]


def test_process_descendants_routes_assets_and_folders() -> None:
    instance = build_instance()
    instance.descendants_flag = {"include-assets", "include-folders"}

    descendants = [
        DescendantRef("asset-1", EntityType.ASSET),
        DescendantRef("folder-1", EntityType.FOLDER),
        DescendantRef("skip-1", None),
    ]
    instance.entity = DummyEntityAPI(descendants)

    calls = []
    instance._process_descent = lambda idx, ent, ent_type: calls.append((idx, ent.reference, ent_type))

    parent = DummyEntity(reference="parent-1", entity_type=EntityType.FOLDER)
    instance._process_descendants(10, parent)

    assert calls == [
        (10, "asset-1", EntityType.ASSET),
        (10, "folder-1", EntityType.FOLDER),
    ]


def test_process_descendants_ignores_non_folder_parent() -> None:
    instance = build_instance()
    instance.descendants_flag = {"include-assets", "include-folders"}
    instance.entity = DummyEntityAPI([DescendantRef("asset-1", EntityType.ASSET)])

    calls = []
    instance._process_descent = lambda idx, ent, ent_type: calls.append((idx, ent.reference, ent_type))

    parent_asset = DummyEntity(reference="parent-asset", entity_type=EntityType.ASSET)
    instance._process_descendants(2, parent_asset)

    assert calls == []
