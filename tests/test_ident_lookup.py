import pandas as pd

from preservica_modify.pres_modify import PreservicaMassMod


def make_instance(df: pd.DataFrame) -> PreservicaMassMod:
    instance = PreservicaMassMod.__new__(PreservicaMassMod)
    instance.df = df
    instance.column_headers = list(df.columns.values)
    instance.IDENTIFIER_FIELD = "Identifier"
    instance.ARCREF_FIELD = "Archive_Reference"
    instance.ACCREF_FIELD = "Accession_Reference"
    instance.ACCREF_CODE = "accref"
    instance.IDENTIFIER_DEFAULT = "code"
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
