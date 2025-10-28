
def value_to_bool_bit(value):
    if value is None or str(value).strip() == '':
        return 0
    if str(value).strip() in ['0', 'false', 'no']:
        return 0
    return 1