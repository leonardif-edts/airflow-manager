COALESCE_VALUE = {
    "int64": 0,
    "float64": 0.0,
    "string": " ",
    "datetime": "1990-01-01 00:00:00",
    "date": "1990-01-01"
}

def _extend_coalesce(data: list) -> list:
    extended = [
        {**col, "coalesce": COALESCE_VALUE[col["datatype"]]}
        for col in data
    ]
    return extended

def _coalesce(*values):
    return next((v for v in values if v is not None), None)

def _get_filtered_columns(data: list, key: str, value) -> list:
    fltr_columns = [
        {"name": column["name"], "datatype": column["datatype"]}
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