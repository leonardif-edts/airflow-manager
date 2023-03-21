def _coalesce(*values):
    return next((v for v in values if v is not None), None)

def _get_filtered_columns(data: list, key: str, value) -> list:
    fltr_columns = [
        column["name"]
        for column in data
        if column[key] == value
    ]
    return fltr_columns

def _filter_out_keys(data: list, keys: list) -> list:
    fltr_columns = [
        {
            key: value
            for key, value in column.items()
            if (key not in keys)
        }
        for column in data
    ]
    return fltr_columns