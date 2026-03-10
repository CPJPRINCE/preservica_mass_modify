import pandas as pd

from preservica_modify.pres_modify import PreservicaMassMod


def make_instance() -> PreservicaMassMod:
    instance = PreservicaMassMod.__new__(PreservicaMassMod)
    instance.input_file = "input.csv"
    instance.ENTITY_REF = "Entity Ref"
    instance.DOCUMENT_TYPE = "Document type"
    instance.metadata_flag = None
    instance.retention_flag = False
    instance.upload_flag = False
    return instance


def test_main_upload_mode_short_circuit() -> None:
    instance = make_instance()
    calls = []

    instance.init_df = lambda: calls.append("init_df")
    instance._set_input_flags = lambda: calls.append("set_input_flags")
    instance.login_preservica = lambda: calls.append("login")
    instance._process_upload_mode = lambda: calls.append("upload_mode")
    instance._remove_continue_token = lambda path: calls.append(("remove_token", path))
    instance.upload_flag = True

    instance.main()

    assert calls == [
        "init_df",
        "set_input_flags",
        "login",
        "upload_mode",
        ("remove_token", "input.csv"),
    ]


def test_main_calls_metadata_and_retention_initializers() -> None:
    instance = make_instance()
    calls = []

    def init_df():
        instance.df = pd.DataFrame({"Entity Ref": ["R1"], "Document type": ["SO"]})
        instance.column_headers = ["Entity Ref", "Document type"]
        calls.append("init_df")

    instance.init_df = init_df
    instance._set_input_flags = lambda: calls.append("set_input_flags")
    instance.login_preservica = lambda: calls.append("login")
    instance.init_generate_descriptive_metadata = lambda: calls.append("init_metadata")
    instance.get_retentions = lambda: calls.append("get_retentions")
    instance._process_rows = lambda data: calls.append(("process_rows", data))
    instance._remove_continue_token = lambda path: calls.append(("remove_token", path))

    instance.metadata_flag = "exact"
    instance.retention_flag = True

    instance.main()

    assert "init_metadata" in calls
    assert "get_retentions" in calls
    assert any(c[0] == "process_rows" for c in calls if isinstance(c, tuple))


def test_main_builds_data_dict_without_document_type_column() -> None:
    instance = make_instance()
    captured = []

    def init_df():
        instance.df = pd.DataFrame({"Entity Ref": ["R1"]})
        instance.column_headers = ["Entity Ref"]

    instance.init_df = init_df
    instance._set_input_flags = lambda: None
    instance.login_preservica = lambda: None
    instance._process_rows = lambda data: captured.append(data)
    instance._remove_continue_token = lambda _: None

    instance.main()

    assert captured == [{0: {"Entity Ref": "R1"}}]


def test_main_reraises_value_error() -> None:
    instance = make_instance()

    def broken_init_df():
        raise ValueError("bad input")

    instance.init_df = broken_init_df

    try:
        instance.main()
    except ValueError:
        pass
    else:
        raise AssertionError("Expected ValueError to be re-raised from main")
