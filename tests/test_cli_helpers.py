import argparse

from preservica_modify.cli import fmthelper, metadata_helper, server_helper


def test_server_helper_strips_http_scheme() -> None:
    assert server_helper("http://example.com") == "example.com"


def test_server_helper_strips_https_scheme() -> None:
    assert server_helper("https://example.com") == "example.com"


def test_server_helper_keeps_plain_host() -> None:
    assert server_helper("example.com") == "example.com"


def test_fmthelper_aliases() -> None:
    assert fmthelper("excel") == "xlsx"
    assert fmthelper("txt") == "csv"
    assert fmthelper("jsn") == "json"
    assert fmthelper("open_document_spreadsheet") == "ods"
    assert fmthelper("html") == "xml"
    assert fmthelper("dictionary") == "dict"


def test_fmthelper_invalid_raises() -> None:
    try:
        fmthelper("nope")
    except argparse.ArgumentTypeError:
        pass
    else:
        raise AssertionError("Expected ArgumentTypeError for invalid format")


def test_metadata_helper_aliases_and_invalid() -> None:
    assert metadata_helper("e") == "exact"
    assert metadata_helper("f") == "flat"

    try:
        metadata_helper("other")
    except argparse.ArgumentTypeError:
        pass
    else:
        raise AssertionError("Expected ArgumentTypeError for invalid metadata mode")
