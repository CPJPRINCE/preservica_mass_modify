import argparse

import preservica_modify.cli as cli


class DummyPMM:
    created = []

    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.calls = []
        DummyPMM.created.append(self)

    def print_local_xmls(self):
        self.calls.append("print_local_xmls")

    def convert_local_xmls(self, fmt):
        self.calls.append(("convert_local_xmls", fmt))

    def print_remote_xmls(self):
        self.calls.append("print_remote_xmls")

    def convert_remote_xmls(self, fmt):
        self.calls.append(("convert_remote_xmls", fmt))

    def login_preservica(self):
        self.calls.append("login_preservica")

    def main(self):
        self.calls.append("main")


def make_args(**overrides) -> argparse.Namespace:
    base = {
        "log_level": None,
        "log_file": None,
        "print_xmls": False,
        "convert_xmls": None,
        "print_remote_xmls": False,
        "convert_remote_xmls": None,
        "input": "input.csv",
        "metadata_dir": "metadata",
        "use_credentials": None,
        "server": "example.preservica.com",
        "username": "user@example.com",
        "delete": False,
        "manager_username": None,
        "test_login": False,
        "tenant": None,
        "use_keyring": False,
        "keyring_service": "preservica_modify",
        "save_password": False,
        "save_password_to_keyring": False,
        "descendants": None,
        "blank_override": False,
        "metadata": None,
        "dummy": True,
        "upload_mode": False,
        "options_file": "options.properties",
        "column_sensitivity": False,
    }
    base.update(overrides)
    return argparse.Namespace(**base)


def test_run_cli_print_xmls_exits(monkeypatch) -> None:
    DummyPMM.created = []
    monkeypatch.setattr(cli, "PreservicaMassMod", DummyPMM)

    args = make_args(print_xmls=True)

    try:
        cli.run_cli(args)
    except SystemExit:
        pass
    else:
        raise AssertionError("Expected SystemExit for print-xmls mode")

    assert DummyPMM.created
    assert DummyPMM.created[0].calls == ["print_local_xmls"]


def test_run_cli_missing_input_file_raises(monkeypatch) -> None:
    monkeypatch.setattr(cli.os.path, "isfile", lambda _: False)

    try:
        cli.run_cli(make_args())
    except FileNotFoundError:
        pass
    else:
        raise AssertionError("Expected FileNotFoundError for missing input")


def test_run_cli_missing_server_without_credentials_raises(monkeypatch) -> None:
    monkeypatch.setattr(cli.os.path, "isfile", lambda _: True)

    try:
        cli.run_cli(make_args(server=None, use_credentials=None))
    except ValueError:
        pass
    else:
        raise AssertionError("Expected ValueError when no server or credentials are provided")


def test_run_cli_invalid_descendants_raises(monkeypatch) -> None:
    monkeypatch.setattr(cli.os.path, "isfile", lambda _: True)
    monkeypatch.setattr(cli.os.path, "isdir", lambda _: True)

    try:
        cli.run_cli(make_args(descendants=["include-xml"]))
    except ValueError:
        pass
    else:
        raise AssertionError("Expected ValueError for invalid descendants selection")


def test_run_cli_descendant_title_confirmation_no_exits(monkeypatch) -> None:
    monkeypatch.setattr(cli.os.path, "isfile", lambda _: True)
    monkeypatch.setattr(cli.os.path, "isdir", lambda _: True)
    monkeypatch.setattr("builtins.input", lambda _: "n")

    try:
        cli.run_cli(make_args(descendants=["include-assets", "include-title"]))
    except SystemExit:
        pass
    else:
        raise AssertionError("Expected SystemExit when title descendant confirmation is declined")


def test_run_cli_test_login_calls_login_and_exits(monkeypatch) -> None:
    DummyPMM.created = []
    monkeypatch.setattr(cli, "PreservicaMassMod", DummyPMM)
    monkeypatch.setattr(cli.os.path, "isfile", lambda _: True)

    args = make_args(test_login=True)

    try:
        cli.run_cli(args)
    except SystemExit:
        pass
    else:
        raise AssertionError("Expected SystemExit for test-login mode")

    assert DummyPMM.created
    assert DummyPMM.created[0].calls == ["login_preservica"]


def test_run_cli_happy_path_calls_main(monkeypatch) -> None:
    DummyPMM.created = []
    monkeypatch.setattr(cli, "PreservicaMassMod", DummyPMM)
    monkeypatch.setattr(cli.os.path, "isfile", lambda _: True)
    monkeypatch.setattr(cli.os.path, "isdir", lambda _: True)

    cli.run_cli(make_args())

    assert DummyPMM.created
    assert DummyPMM.created[0].calls == ["main"]
