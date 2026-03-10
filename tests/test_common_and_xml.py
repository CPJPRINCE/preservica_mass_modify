from pathlib import Path

from lxml import etree

from preservica_modify.common import check_bool, check_nan
from preservica_modify.pres_modify import PreservicaMassMod


def make_instance() -> PreservicaMassMod:
    instance = PreservicaMassMod.__new__(PreservicaMassMod)
    instance.blank_override = False
    instance.xnames = []
    return instance


def test_check_nan_converts_nan_and_nat() -> None:
    assert check_nan("nan") is None
    assert check_nan("NaT") is None
    assert check_nan("value") == "value"


def test_check_bool_truthy_values() -> None:
    assert check_bool("true") is True
    assert check_bool("YES") is True
    assert check_bool("1") is True


def test_check_bool_falsey_values() -> None:
    assert check_bool(None) is False
    assert check_bool("") is False
    assert check_bool("false") is False
    assert check_bool("0") is False


def test_xml_merge_overwrites_existing_text() -> None:
    instance = make_instance()
    xml_a = etree.fromstring("<root><title>old</title></root>")
    xml_b = etree.fromstring("<root><title>new</title></root>")

    merged = instance.xml_merge(xml_a, xml_b)

    assert merged.find("./title").text == "new"


def test_xml_merge_adds_missing_element() -> None:
    instance = make_instance()
    xml_a = etree.fromstring("<root><title>old</title></root>")
    xml_b = etree.fromstring("<root><extra>value</extra></root>")

    merged = instance.xml_merge(xml_a, xml_b)

    assert merged.find("./extra") is not None
    assert merged.find("./extra").text == "value"


def test_xml_merge_blank_override_clears_whitelisted_tag() -> None:
    instance = make_instance()
    instance.blank_override = True
    instance.xnames = ["title"]
    xml_a = etree.fromstring("<root><title>old</title></root>")
    xml_b = etree.fromstring("<root><title/></root>")

    merged = instance.xml_merge(xml_a, xml_b)

    assert merged.find("./title").text is None


def test_xml_merge_blank_override_ignores_non_whitelisted_tag() -> None:
    instance = make_instance()
    instance.blank_override = True
    instance.xnames = ["other"]
    xml_a = etree.fromstring("<root><title>old</title></root>")
    xml_b = etree.fromstring("<root><title/></root>")

    merged = instance.xml_merge(xml_a, xml_b)

    assert merged.find("./title").text == "old"


def test_save_load_and_remove_continue_token(tmp_path: Path) -> None:
    instance = make_instance()
    token_base = str(tmp_path / "input.csv")

    instance._save_continue_token(token_base, 42)
    loaded = instance._load_continue_token(token_base)

    assert loaded == 42

    instance._remove_continue_token(token_base)
    assert not (tmp_path / "input.csv_continue.txt").exists()


def test_load_continue_token_invalid_value_raises(tmp_path: Path) -> None:
    instance = make_instance()
    token_base = str(tmp_path / "bad.csv")
    token_file = tmp_path / "bad.csv_continue.txt"
    token_file.write_text("not-an-int", encoding="utf-8")

    try:
        instance._load_continue_token(token_base)
    except Exception:
        pass
    else:
        raise AssertionError("Expected exception for invalid continue token content")
