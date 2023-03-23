import os


# Constant
COALESCE_VALUE = {
    "int64": 0,
    "float64": 0.0,
    "string": " ",
    "datetime": "1990-01-01 00:00:00",
    "date": "1990-01-01"
}


# Public
def create_dir(dirname: str, prune: int = 0):
    dirpath = ""
    walks = os.path.split(dirname)
    for path in walks[:len(walks)-prune]:
        if (path == ""):
            continue
        
        dirpath = os.path.join(dirpath, path)
        if (not os.path.exists(dirpath)):
            os.mkdir(dirpath)
    return dirpath


def coalesce(*values):
    return next((v for v in values if v is not None), None)

def extend_coalesce(data: list) -> list:
    extended = [
        {**col, "coalesce": COALESCE_VALUE[col["datatype"]]}
        for col in data
    ]
    return extended

def filter_out_keys(data: list, keys: list) -> list:
    fltr_columns = [
        {
            key: value
            for key, value in column.items()
            if (key not in keys)
        }
        for column in data
    ]
    return fltr_columns

def get_filtered_columns(data: list, key: str, value) -> list:
    fltr_columns = [
        {"name": column["name"], "datatype": column["datatype"]}
        for column in data
        if column[key] == value
    ]
    return fltr_columns