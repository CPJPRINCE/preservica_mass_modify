def check_nan(value):
    if str(value).lower() in {"nan","nat"}:
        value = None
    return value

def check_bool(value):
    if str(value).lower() in {"true","1","yes"}:
        return True
    elif check_nan(value) in {None,"","false","0","no"}:
        return False