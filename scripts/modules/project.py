import os
import json
import flatten_dict

from scripts import utils


# Public
def init_project():
    current_dir = os.getcwd()
    manager_dir = utils.create_dir(os.path.join(current_dir, ".airflow-manager"))
    version_dir = utils.create_dir(os.path.join(manager_dir, "version"))

    project_metadata = {
        "dirname": {
            "project": current_dir,
            "manager": manager_dir,
            "version": version_dir
        },
        "deploy": {
            "latest_version": None,
            "update_tm": None
        },
        "plan": {
            "latest_version": None,
            "update_tm": None
        }
    }
    sorted_project_metadata = {k: project_metadata[k] for k in sorted(project_metadata.keys())}
    utils.export_json(sorted_project_metadata, manager_dir, "metadata.json")

def get_version_dir():
    manager_dir = _get_manager_dir()
    version_dir = _get_metadata(manager_dir, "dirname.version")
    return version_dir

def update_metadata(values: dict):
    manager_dir = _get_manager_dir()
    metadata = json.load(open(os.path.join(manager_dir, "metadata.json")))
    upt_metadata = _update_metadata_value(metadata, values)
    utils.export_json(upt_metadata, manager_dir, "metadata.json")

# Private
def _get_manager_dir():
    current_dir = os.getcwd()
    if (".airflow-manager" in os.listdir(current_dir)):
        return os.path.join(current_dir, ".airflow-manager")
    
    trace = current_dir 
    trace_parent = os.path.dirname(current_dir)
    while(trace_parent != current_dir):
        if (".airflow-manager" in os.listdir(trace)):
            return os.path.join(trace, ".airflow-manager")
        trace = trace_parent
        trace_parent = os.path.dirname(trace)

def _get_metadata(manager_dir: str, key: str):
    pathname = os.path.join(manager_dir, "metadata.json")
    metadata = json.load(open(pathname))

    value = metadata
    for k in key.split("."):
        value = value[k]
    return value

def _update_metadata_value(metadata: dict, values: dict) -> dict:
    flt_metadata = flatten_dict.flatten(metadata, reducer="dot")
    for k, v in values.items():
        flt_metadata[k] = v
    upt_metadata = flatten_dict.unflatten(flt_metadata, splitter="dot")
    upt_metadata = {k:upt_metadata[k] for k in sorted(upt_metadata.keys())}
    return upt_metadata
