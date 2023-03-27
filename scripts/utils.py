import os
import json
from typing import List, Union
from datetime import date, datetime

from scripts import const


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
        {**col, "coalesce": const.COALESCE_VALUE[col["datatype"]]}
        for col in data
    ]
    return extended

def export_json(data: dict, dirname: str, filename: str):
    pathname = os.path.join(dirname, filename)
    with open(pathname, "w") as file:
        if (data):
            file.write(json.dumps(data, indent=2, default=_json_serializer))
        else:
            file.write("")

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

def get_filtered_columns(data: list, key: str, value: list) -> list:
    fltr_columns = [
        {"name": column["name"], "datatype": column["datatype"]}
        for column in data
        if column[key] in value
    ]
    return fltr_columns

def slicing_list(data: List[list], min_row: int = 1, max_row: int = None, min_col: int = 1, max_col: int = None) -> list:
    sliced = [
        row[min_col-1:max_col] if(max_col) else row[min_col-1:]
        for row in (data[min_row-1:max_row] if (max_row) else data[min_row-1:])
    ]
    return sliced


# Private
def _json_serializer(obj: Union[date, datetime]):
    if isinstance(obj, date):
        return obj.strftime("%Y-%m-%d")
    elif isinstance(obj, datetime):
        return obj.strftime("%Y-%m-%d %H:%M:%S")
    raise TypeError(f"Type {str(type(obj))} not serializable")