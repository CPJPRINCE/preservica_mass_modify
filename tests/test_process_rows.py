from preservica_modify.pres_modify import EntityType, PreservicaMassMod


class DummyEntity:
    def __init__(self, reference: str, entity_type):
        self.reference = reference
        self.entity_type = entity_type


def make_instance() -> PreservicaMassMod:
    instance = PreservicaMassMod.__new__(PreservicaMassMod)
    instance.ENTITY_REF = "Entity Ref"
    instance.DOCUMENT_TYPE = "Document type"
    instance.disable_continue = True
    instance.input_file = "input.csv"
    return instance


def test_process_rows_calls_row_handler_for_valid_entity_only() -> None:
    instance = make_instance()

    data_dict = {
        0: {"Entity Ref": "R1", "Document type": "SO"},
        1: {"Entity Ref": None, "Document type": "SO"},
        2: {"Entity Ref": "R2", "Document type": "SO"},
    }

    instance._process_continue_token = lambda d: (list(d.keys()), 0)

    def fake_fetch(ref, doc_type):
        if ref == "R1":
            return DummyEntity(ref, EntityType.FOLDER)
        return None

    calls = []
    instance._process_fetch_ent = fake_fetch
    instance._process_row_ent = lambda ent, idx, row: calls.append((idx, ent.reference))

    instance._process_rows(data_dict)

    assert calls == [(0, "R1")]


def test_process_rows_raises_for_missing_row_dict() -> None:
    instance = make_instance()
    data_dict = {0: None}
    instance._process_continue_token = lambda d: (list(d.keys()), 0)

    try:
        instance._process_rows(data_dict)
    except ValueError:
        pass
    else:
        raise AssertionError("Expected ValueError when row data is missing")


def test_process_rows_saves_continue_token_on_keyboard_interrupt() -> None:
    instance = make_instance()
    instance.disable_continue = False

    data_dict = {0: {"Entity Ref": "R1", "Document type": "SO"}}
    instance._process_continue_token = lambda d: (list(d.keys()), 0)

    def fake_fetch(_ref, _doc_type):
        raise KeyboardInterrupt()

    saved = []
    instance._process_fetch_ent = fake_fetch
    instance._save_continue_token = lambda file_name, token: saved.append((file_name, token))

    try:
        instance._process_rows(data_dict)
    except KeyboardInterrupt:
        pass
    else:
        raise AssertionError("Expected KeyboardInterrupt to be re-raised")

    assert saved == [("input.csv", 0)]


def test_process_rows_passes_none_doc_type_to_fetch_when_missing() -> None:
    instance = make_instance()

    data_dict = {0: {"Entity Ref": "R1", "Document type": None}}
    instance._process_continue_token = lambda d: (list(d.keys()), 0)

    captured = []

    def fake_fetch(ref, doc_type):
        captured.append((ref, doc_type))
        return None

    instance._process_fetch_ent = fake_fetch
    instance._process_row_ent = lambda *args, **kwargs: None

    instance._process_rows(data_dict)

    assert captured == [("R1", None)]
