from pathlib import Path

import preservica_modify.cli as cli
import preservica_modify.pres_modify as pm
from preservica_modify.pres_modify import PreservicaMassMod


class DummyKeyring:
    def __init__(self):
        self.saved = []

    def get_password(self, service, username):
        return f"pw:{service}:{username}"

    def set_password(self, service, username, password):
        self.saved.append((service, username, password))


class FailingKeyring(DummyKeyring):
    def get_password(self, service, username):
        raise RuntimeError("fail")

    def set_password(self, service, username, password):
        raise RuntimeError("fail")


def make_instance() -> PreservicaMassMod:
    instance = PreservicaMassMod.__new__(PreservicaMassMod)
    instance.use_keyring = True
    instance.save_password_to_keyring = True
    instance.keyring_service = "svc"
    instance.server = "example.server"
    instance.tenant = "tenant-a"
    instance.username = "user-a"
    return instance


def test_keyring_entry_name_uses_defaults_when_missing_values() -> None:
    instance = PreservicaMassMod.__new__(PreservicaMassMod)
    instance.keyring_service = "svc"
    instance.server = None
    instance.tenant = None

    assert instance._keyring_entry_name() == "svc:default-server:default"


def test_get_password_from_keyring_success(monkeypatch) -> None:
    instance = make_instance()
    monkeypatch.setattr(pm, "keyring", DummyKeyring())

    value = instance._get_password_from_keyring("user-a")

    assert value == "pw:svc:example.server:tenant-a:user-a"


def test_get_password_from_keyring_returns_none_when_disabled() -> None:
    instance = make_instance()
    instance.use_keyring = False

    assert instance._get_password_from_keyring("user-a") is None


def test_get_password_from_keyring_raises_when_package_missing(monkeypatch) -> None:
    instance = make_instance()
    monkeypatch.setattr(pm, "keyring", None)

    try:
        instance._get_password_from_keyring("user-a")
    except RuntimeError:
        pass
    else:
        raise AssertionError("Expected RuntimeError when keyring package is unavailable")


def test_get_password_from_keyring_handles_keyring_error(monkeypatch) -> None:
    instance = make_instance()
    monkeypatch.setattr(pm, "keyring", FailingKeyring())
    monkeypatch.setattr(pm, "KeyringError", RuntimeError, raising=False)

    value = instance._get_password_from_keyring("user-a")

    assert value is None


def test_set_password_in_keyring_success(monkeypatch) -> None:
    instance = make_instance()
    fake = DummyKeyring()
    monkeypatch.setattr(pm, "keyring", fake)

    instance._set_password_in_keyring("user-a", "secret")

    assert fake.saved == [("svc:example.server:tenant-a", "user-a", "secret")]


def test_set_password_in_keyring_returns_when_saving_disabled(monkeypatch) -> None:
    instance = make_instance()
    instance.save_password_to_keyring = False
    fake = DummyKeyring()
    monkeypatch.setattr(pm, "keyring", fake)

    instance._set_password_in_keyring("user-a", "secret")

    assert fake.saved == []


def test_set_password_in_keyring_handles_keyring_error(monkeypatch) -> None:
    instance = make_instance()
    monkeypatch.setattr(pm, "keyring", FailingKeyring())
    monkeypatch.setattr(pm, "KeyringError", RuntimeError, raising=False)

    instance._set_password_in_keyring("user-a", "secret")


def test_parse_config_uses_defaults_when_missing_file(tmp_path: Path) -> None:
    instance = PreservicaMassMod.__new__(PreservicaMassMod)

    instance.parse_config(str(tmp_path / "missing.properties"))

    assert instance.ENTITY_REF == "Entity Ref"
    assert instance.DOCUMENT_TYPE == "Document type"
    assert instance.IDENTIFIER_DEFAULT == "code"


def test_parse_config_applies_case_lowering_when_enabled(tmp_path: Path) -> None:
    options = tmp_path / "options.properties"
    options.write_text(
        "[options]\nTITLE_FIELD=MyTitle\nDESCRIPTION_FIELD=MyDescription\n",
        encoding="utf-8",
    )

    instance = PreservicaMassMod.__new__(PreservicaMassMod)
    instance.parse_config(str(options), column_sensitivity=True)

    assert instance.TITLE_FIELD == "Title"
    assert instance.DESCRIPTION_FIELD == "Description"


def test_create_parser_parses_basic_required_input() -> None:
    parser = cli.create_parser()

    args = parser.parse_args(["-i", "input.csv", "-u", "name", "-s", "https://example.com"])

    assert args.input == "input.csv"
    assert args.server == "example.com"
    assert args.username == "name"
