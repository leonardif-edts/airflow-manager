import os
import logging

import json
import jinja2


def get_blueprint(script_dir: str, template: str) -> jinja2.Template:
    pathname = os.path.join(script_dir, "blueprint", template)
    with open(pathname, "r") as file:
        raw = file.read()
    blueprint = jinja2.Template(raw)
    return blueprint


def create_dir(dirname: str):
    dirpath = ""
    for path in os.path.split(dirname):
        if (path == ""):
            continue
        
        dirpath = os.path.join(dirpath, path)
        if (not os.path.exists(dirpath)):
            os.mkdir(dirpath)
    return dirpath


def create_ddl(ddl_dir: str, dag_config: dict, script_dir: str):
    ddl_type = os.path.join(ddl_dir, dag_config["dag"].get("type", "").lower())
    create_dir(ddl_type)

    pathname = os.path.join(ddl_type, dag_config["dag"]["bq_tablename"])
    ddl_script = render_ddl(script_dir, dag_config)
    with open(pathname, "w") as file:
        file.write(ddl_script)


def render_ddl(script_dir: str, dag_config: dict) -> str:
    blueprint = get_blueprint(script_dir, "ddl.sql")
    ddl_script = blueprint.render(**dag_config)
    return ddl_script


def export_project(filename: str, script_dir: str):
    config = json.load(open(filename))

    logging.debug("Create/Get DAG and DDL directory")
    dags_dir = create_dir("dags")
    ddl_dir = create_dir("ddl")
    
    for dag in config["dags"]:
        logging.debug(f"Create/Update DAG: {dag['dag_id']}")
        dag_config = {"dag": dag, "project": config["project"]}

        create_ddl(ddl_dir, dag_config, script_dir)