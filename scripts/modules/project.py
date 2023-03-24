import os
import json
import flatten_dict
from datetime import datetime

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
            "update_ts": None
        },
        "plan": {
            "latest_version": None,
            "update_ts": None
        }
    }
    sorted_project_metadata = {k: project_metadata[k] for k in sorted(project_metadata.keys())}
    utils.export_json(sorted_project_metadata, manager_dir, "metadata.json")
    utils.export_json(None, manager_dir, "plan_logs.json")
    utils.export_json(None, manager_dir, "deploy_logs.json")

def get_version_dir():
    manager_dir = _get_manager_dir()
    version_dir = _get_metadata(manager_dir, "dirname.version")
    return version_dir

def update_metadata(values: dict):
    manager_dir = _get_manager_dir()
    metadata = json.load(open(os.path.join(manager_dir, "metadata.json")))
    upt_metadata = _update_metadata_value(metadata, values)
    utils.export_json(upt_metadata, manager_dir, "metadata.json")

def update_deploy_logs(config: dict, version: str):
    manager_dir = _get_manager_dir()
    deploy_logs_path = os.path.join(manager_dir, "deploy_logs.json")
    
    timestamp = datetime.now()
    log_data = {
        "id": timestamp.strftime("%Y%m%d%H%M%S"),
        "create_ts": timestamp.strftime("%Y-%m-%d %H:%M:%S"),
        "plan_id": version,
        "total_dags": len(config["dags"])
    }

    _append_logs(log_data, deploy_logs_path)

def update_plan_logs(config: dict, source_type: str, source_id: str, filename: str):
    manager_dir = _get_manager_dir()
    plan_logs_path = os.path.join(manager_dir, "plan_logs.json")

    config_id = filename.replace(".json", "")
    log_data = {
        "id": config_id,
        "create_ts": datetime.strptime(config_id, "%Y%m%d%H%M%S").strftime("%Y-%m-%d %H:%M:%S"),
        "source_type": source_type,
        "source_id": source_id,
        "total_dags": len(config["dags"]),
        **_count_dag(config["dags"], prefix="table_type", key="type"),
        **_count_dag(config["dags"], prefix="table_method", key="method"),
        **_count_dag(config["dags"], prefix="dag_type", key="dag_type")
    }

    _append_logs(log_data, plan_logs_path)


# Private
def _append_logs(log_data: dict, logs_path: str):
    with open(logs_path, "a") as file:
        file.write(json.dumps(log_data))
        file.write("\n")

def _count_dag(dags: list, prefix: str, key: str):
    unique_values = [
        f"{prefix}_{k.lower().replace(' ', '_')}"
        for k in set([dag[key] for dag in dags])
    ]

    counter = {k: 0 for k in unique_values}
    for dag in dags:
        code = f"{prefix}_{dag[key].lower().replace(' ', '_')}"
        counter[code] += 1
    
    sorted_counter = {k: counter[k] for k in sorted(counter.keys())}
    return sorted_counter

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
