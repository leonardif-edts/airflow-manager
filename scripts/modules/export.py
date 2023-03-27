import os
import json
import logging
from datetime import datetime

from scripts import utils
from scripts.modules import blueprint, project


# Public
def export_project(version: str, script_dir: str, version_dir: str):
    latest_version = project.get_latest_deploy()
    if (latest_version) and (latest_version == version):
        logging.info(f"Current deployment already using `{version}` as deployment configuration: Aborted")
    else:
        config = json.load(open(os.path.join(version_dir, f"{version}.json")))

        logging.info("Create/Get DAG and DDL directory")
        timestamp = datetime.now()
        dags_dir = utils.create_dir("dags")
        ddl_dir = utils.create_dir("ddl")
        
        for dag in config["dags"]:
            logging.info(f"Exporting DAG: {dag['dag_id']}")
            dag_config = {"dag": dag, "project": config["project"]}

            _export_ddl(ddl_dir, dag_config, script_dir)
            _export_dag(dags_dir, dag_config, script_dir)
        
        project.update_deploy_logs(config, version)
        project.update_metadata({
            "deploy.latest_version": version,
            "deploy.update_ts": timestamp
        })


# Private
def _export_script(script: str, directory: str, filename: str):
    pathname = os.path.join(directory, filename)
    utils.create_dir(pathname, prune=1)
    with open(pathname, "w") as file:
        file.write(script)

def _export_ddl(ddl_dir: str, dag_config: dict, script_dir: str):
    ddl_script = blueprint.render_script(script_dir, None, "ddl.sql", dag_config)
    ddl_directory = os.path.join(ddl_dir, dag_config["dag"].get("type", "").lower())
    ddl_filename = f"{dag_config['dag']['bq_tablename']}.sql"

    logging.info(f"Export DDL script to `{os.path.join(ddl_directory, ddl_filename)}`")
    _export_script(ddl_script, ddl_directory, ddl_filename)

def _export_dag(dags_dir: str, dag_config: dict, script_dir: str):
    dag_directory = os.path.join(dags_dir, dag_config["dag"].get("dag_id", "").lower())
    
    blueprint_code = blueprint.get_blueprint_code(dag_config["dag"]["dag_type"])
    blueprint_filelist = blueprint.get_blueprint_filelist(script_dir, blueprint_code)
    for blueprint_file in blueprint_filelist:
        dag_script = blueprint.render_script(script_dir, blueprint_code, blueprint_file, dag_config)
        dag_script_filename = blueprint_file if (blueprint_file != "dag.py") else f"{dag_config['dag']['dag_id']}.py"

        logging.info(f"Export DAG script `{blueprint_file}` to `{os.path.join(dag_directory, dag_script_filename)}`")
        _export_script(dag_script, dag_directory, dag_script_filename)