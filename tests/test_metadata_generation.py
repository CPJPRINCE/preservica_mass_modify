from pathlib import Path

import pandas as pd
from lxml import etree

from preservica_modify.pres_modify import PreservicaMassMod


def write_xml(path: Path) -> None:
    path.write_text(
        """
<dc:record xmlns:dc='urn:test'>
  <dc:title></dc:title>
  <dc:description></dc:description>
</dc:record>
""".strip(),
        encoding="utf-8",
    )


def make_instance(df: pd.DataFrame, metadata_flag: str) -> PreservicaMassMod:
    instance = PreservicaMassMod.__new__(PreservicaMassMod)
    instance.df = df
    instance.metadata_flag = metadata_flag
    return instance


def test_generate_descriptive_metadata_exact_updates_by_path(tmp_path: Path) -> None:
    xml_file = tmp_path / "dc.xml"
    write_xml(xml_file)

    df = pd.DataFrame({"dc:title": ["Hello"], "dc:description": ["World"]})
    instance = make_instance(df, "exact")

    xml_files = [
        {
            "data": [
                {"Name": "dc:title", "Path": "dc:title", "Namespace": "urn:test", "XName": "{urn:test}title"},
                {"Name": "dc:description", "Path": "dc:description", "Namespace": "urn:test", "XName": "{urn:test}description"},
            ],
            "local_name": "dc",
            "local_ns": "urn:test",
            "xml_file": str(xml_file),
        }
    ]

    result = instance.generate_descriptive_metadata(0, xml_files)

    assert result is not None
    xml_new = result[0]["urn:test"]
    assert xml_new.find("./{urn:test}title").text == "Hello"
    assert xml_new.find("./{urn:test}description").text == "World"


def test_generate_descriptive_metadata_flat_updates_by_name(tmp_path: Path) -> None:
    xml_file = tmp_path / "dc.xml"
    write_xml(xml_file)

    df = pd.DataFrame({"dc:title": ["Flat Hello"], "dc:description": ["Flat World"]})
    instance = make_instance(df, "flat")

    xml_files = [
        {
            "data": [
                {"Name": "dc:title", "Path": "dc:title", "Namespace": "urn:test", "XName": "{urn:test}title"},
                {"Name": "dc:description", "Path": "dc:description", "Namespace": "urn:test", "XName": "{urn:test}description"},
            ],
            "local_name": "dc",
            "local_ns": "urn:test",
            "xml_file": str(xml_file),
        }
    ]

    result = instance.generate_descriptive_metadata(0, xml_files)

    assert result is not None
    xml_new = result[0]["urn:test"]
    assert xml_new.find(".//{urn:test}title").text == "Flat Hello"
    assert xml_new.find(".//{urn:test}description").text == "Flat World"


def test_generate_descriptive_metadata_skips_invalid_elements(tmp_path: Path) -> None:
    xml_file = tmp_path / "dc.xml"
    write_xml(xml_file)

    df = pd.DataFrame({"dc:title": ["Hello"]})
    instance = make_instance(df, "exact")

    xml_files = [
        {
            "data": [
                "invalid",
                {"Name": None, "Path": "dc:title", "Namespace": "urn:test", "XName": "{urn:test}title"},
                {"Name": "dc:title", "Path": "dc:title", "Namespace": "urn:test", "XName": "{urn:test}title"},
            ],
            "local_name": "dc",
            "local_ns": "urn:test",
            "xml_file": str(xml_file),
        }
    ]

    result = instance.generate_descriptive_metadata(0, xml_files)

    assert result is not None
    xml_new = result[0]["urn:test"]
    assert xml_new.find("./{urn:test}title").text == "Hello"


def test_generate_descriptive_metadata_skips_when_local_info_missing(tmp_path: Path) -> None:
    xml_file = tmp_path / "dc.xml"
    write_xml(xml_file)

    df = pd.DataFrame({"dc:title": ["Hello"]})
    instance = make_instance(df, "exact")

    xml_files = [
        {
            "data": [{"Name": "dc:title", "Path": "dc:title", "Namespace": "urn:test", "XName": "{urn:test}title"}],
            "localname": None,
            "localns": "urn:test",
            "xmlfile": str(xml_file),
        }
    ]

    result = instance.generate_descriptive_metadata(0, xml_files)

    assert result == []
