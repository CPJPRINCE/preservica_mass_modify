import pandas as pd

from preservica_modify.pres_modify import PreservicaMassMod


def make_instance(df: pd.DataFrame) -> PreservicaMassMod:
    instance = PreservicaMassMod.__new__(PreservicaMassMod)
    instance.df = df
    instance.column_headers = list(df.columns.values)
    instance.blank_override = False
    instance.TITLE_FIELD = "Title"
    instance.DESCRIPTION_FIELD = "Description"
    instance.SECURITY_FIELD = "Security"
    instance.RETENTION_FIELD = "Retention Policy"
    instance.MOVETO_FIELD = "Move to"
    return instance


def test_set_input_flags_from_headers() -> None:
    df = pd.DataFrame(
        {
            "Title": ["A"],
            "Description": ["B"],
            "Security": ["open"],
            "Retention Policy": ["Keep7"],
            "Move to": ["dest"],
        }
    )
    instance = make_instance(df)

    instance._set_input_flags()

    assert instance.title_flag is True
    assert instance.description_flag is True
    assert instance.security_flag is True
    assert instance.retention_flag is True
    assert instance.move_flag is True