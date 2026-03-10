from preservica_modify.pres_modify import EntityType, PreservicaMassMod
from lxml import etree


class DummyEntity:
    def __init__(self, reference: str, entity_type):
        self.reference = reference
        self.entity_type = entity_type


class DummyEntityAPI:
    def __init__(self):
        self.folder_calls = []
        self.asset_calls = []
        self.fail_asset = False

    def asset(self, ref: str):
        self.asset_calls.append(ref)
        if self.fail_asset:
            raise RuntimeError("asset lookup failed")
        return f"asset:{ref}"

    def folder(self, ref: str):
        self.folder_calls.append(ref)
        return f"folder:{ref}"


def make_instance() -> PreservicaMassMod:
    instance = PreservicaMassMod.__new__(PreservicaMassMod)
    instance.entity = DummyEntityAPI()
    return instance


def test_process_fetch_ent_uses_doc_type_and_fallback() -> None:
    instance = make_instance()

    so_ent = instance._process_fetch_ent("ref-so", "SO")
    io_ent = instance._process_fetch_ent("ref-io", "IO")

    instance.entity.fail_asset = True
    fallback_ent = instance._process_fetch_ent("ref-fallback", None)

    assert so_ent == "folder:ref-so"
    assert io_ent == "asset:ref-io"
    assert fallback_ent == "folder:ref-fallback"


def test_process_fetch_ent_returns_none_on_invalid_doc_type() -> None:
    instance = make_instance()

    result = instance._process_fetch_ent("ref-x", "INVALID")

    assert result is None


def test_process_continue_token_with_disable_true_starts_at_zero() -> None:
    instance = make_instance()
    instance.disable_continue = True

    keys, start_pos = instance._process_continue_token({10: {}, 20: {}})

    assert keys == [10, 20]
    assert start_pos == 0


def test_process_continue_token_uses_loaded_index_when_enabled() -> None:
    instance = make_instance()
    instance.disable_continue = False
    instance.input_file = "dummy.csv"
    instance._load_continue_token = lambda _: 20

    keys, start_pos = instance._process_continue_token({10: {}, 20: {}, 30: {}})

    assert keys == [10, 20, 30]
    assert start_pos == 1


def test_process_continue_token_raises_on_non_int_loaded_token() -> None:
    instance = make_instance()
    instance.disable_continue = False
    instance.input_file = "dummy.csv"
    instance._load_continue_token = lambda _: "bad"

    try:
        instance._process_continue_token({1: {}, 2: {}})
    except ValueError:
        pass
    else:
        raise AssertionError("Expected ValueError for non-integer continue token")


def test_process_row_ent_skips_invalid_xml_and_updates_valid_tree() -> None:
    instance = make_instance()
    instance.delete_flag = False
    instance.title_flag = False
    instance.description_flag = False
    instance.security_flag = False
    instance.IDENTIFIER_DEFAULT = "code"
    instance.metadata_flag = "exact"
    instance.xml_files = []
    instance.retention_flag = False
    instance.xnames = []

    valid_tree = etree.ElementTree(etree.Element("root"))
    instance.generate_descriptive_metadata = lambda idx, files: [
        {"urn:valid": valid_tree, "xnames": ["{urn:valid}a", None, 1]},
        {"urn:invalid": None, "xnames": ["{urn:invalid}b"]},
    ]

    xml_calls = []
    ident_calls = []
    move_calls = []
    descendants_calls = []

    instance.ident_lookup = lambda *args, **kwargs: {"code": "A1"}
    instance.ident_update = lambda ent, ident: ident_calls.append((ent.reference, ident))
    instance.xml_update = lambda ent, ns, xml_new: xml_calls.append((ent.reference, ns, xml_new))
    instance.move_update = lambda idx, ent: move_calls.append((idx, ent.reference))
    instance._process_descendants = lambda idx, ent: descendants_calls.append((idx, ent.reference))

    ent = DummyEntity("ref-row", EntityType.ASSET)
    instance._process_row_ent(ent, 5, {})

    assert ident_calls == [("ref-row", {"code": "A1"})]
    assert len(xml_calls) == 1
    assert xml_calls[0][1] == "urn:valid"
    assert xml_calls[0][2] is valid_tree
    assert move_calls == [(5, "ref-row")]
    assert descendants_calls == [(5, "ref-row")]
    assert instance.xnames == ["{urn:invalid}b"]
