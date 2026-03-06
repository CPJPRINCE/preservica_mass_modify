import pandas as pd

from preservica_modify.pres_modify import PreservicaMassMod


def make_instance(df: pd.DataFrame) -> PreservicaMassMod:
    instance = PreservicaMassMod.__new__(PreservicaMassMod)
    instance.df = df
    instance.column_headers = list(df.columns.values)
    instance.blank_override = False
    instance.IDENTIFIER_FIELD = "Identifier"
    instance.ARCREF_FIELD = "Archive_Reference"
    instance.ACCREF_FIELD = "Accession_Reference"
    instance.ACCREF_CODE = "accref"
    instance.IDENTIFIER_DEFAULT = "code"
    instance.TITLE_FIELD = "Title"
    instance.DESCRIPTION_FIELD = "Description"
    instance.SECURITY_FIELD = "Security"
    instance.RETENTION_FIELD = "Retention Policy"
    instance.MOVETO_FIELD = "Move to"
    return instance


def test_ident_lookup_defaults() -> None:
    df = pd.DataFrame(
        {
            "Identifier": ["ID-1"],
            "Archive_Reference": ["ARC-1"],
            "Accession_Reference": ["ACC-1"],
        }
    )
    instance = make_instance(df)

    result = instance.ident_lookup(0)

    assert result == {
        "code": "ID-1",
        "code": "ARC-1",
        "accref": "ACC-1",
    }


def test_ident_lookup_custom_default_key() -> None:
    df = pd.DataFrame(
        {
            "Identifier": ["ID-2"],
            "Identifier:local": ["LOC-2"],
            "Identifier:altref": ["ALT-2"],
            "Archive_Reference": ["ARC-2"],
            "Accession_Reference": ["ACC-2"],
        }
    )
    instance = make_instance(df)

    result = instance.ident_lookup(0, default_key="custom")
    result2 = instance.ident_lookup(0)
    print(result)
    assert result == {
        "custom": "ARC-2",
        "altref": "ALT-2",
        "local": "LOC-2",
        "accref": "ACC-2",
    }
    print(result2)
    assert result2 == {
        "code": "ARC-2",
        "local": "LOC-2",
        "altref": "ALT-2",
        "accref": "ACC-2",
    }


def test_ident_lookup_no_identifiers() -> None:
    df = pd.DataFrame({"Title": ["No idents"]})
    instance = make_instance(df)

    result = instance.ident_lookup(0)

    assert result is None


def test_xip_lookup_uses_enabled_flags() -> None:
    df = pd.DataFrame(
        {
            "Title": ["New Title"],
            "Description": ["New Description"],
            "Security": ["internal"],
        }
    )
    instance = make_instance(df)
    instance.title_flag = True
    instance.description_flag = True
    instance.security_flag = True

    title, description, security = instance.xip_lookup(0)

    assert title == "New Title"
    assert description == "New Description"
    assert security == "internal"


def test_xip_lookup_returns_none_when_flags_disabled() -> None:
    df = pd.DataFrame({"Title": ["A"], "Description": ["B"], "Security": ["C"]})
    instance = make_instance(df)
    instance.title_flag = False
    instance.description_flag = False
    instance.security_flag = False

    title, description, security = instance.xip_lookup(0)

    assert title is None
    assert description is None
    assert security is None


def test_retention_lookup_returns_policy_when_flag_enabled() -> None:
    df = pd.DataFrame({"Retention Policy": ["Keep7"]})
    instance = make_instance(df)
    instance.retention_flag = True

    value = instance.retention_lookup(0)

    assert value == "Keep7"


def test_retention_lookup_returns_none_when_flag_disabled() -> None:
    df = pd.DataFrame({"Retention Policy": ["Keep7"]})
    instance = make_instance(df)
    instance.retention_flag = False

    value = instance.retention_lookup(0)

    assert value is None


def test_xip_lookup_raises_runtime_error_without_dataframe() -> None:
    instance = PreservicaMassMod.__new__(PreservicaMassMod)
    instance.title_flag = True
    instance.description_flag = False
    instance.security_flag = False
    instance.TITLE_FIELD = "Title"

    try:
        instance.xip_lookup(0)
    except RuntimeError:
        pass
    else:
        raise AssertionError("Expected RuntimeError when dataframe is not initialized")
