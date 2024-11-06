
def check_nan(value):
    if str(value).lower() in {"nan","nat"}:
        value = None
    return value
