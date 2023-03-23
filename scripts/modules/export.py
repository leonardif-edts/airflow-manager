import os
import logging
from typing import List

import json
import jinja2

BLUEPRINT_CODE = {
    "Postgres to BQ RV": "postgres_to_bq_rv"
}

# Public
def export_project(filename: str, script_dir: str):
    config = json.load(open(filename))

    logging.debug("Create/Get DAG and DDL directory")
    dags_dir = _create_dir("dags")
    ddl_dir = _create_dir("ddl")
    
    for dag in config["dags"]:
        logging.debug(f"Create/Update DAG: {dag['dag_id']}")
        dag_config = {"dag": dag, "project": config["project"]}

        _create_ddl(ddl_dir, dag_config, script_dir)
        _create_dags(dags_dir, dag_config, script_dir)

def export_script(script: str, directory: str, filename: str):
    pathname = os.path.join(directory, filename)
    _create_dir(pathname, prune=1)
    with open(pathname, "w") as file:
        file.write(script)


# Private
def _create_dags(dags_dir: str, dag_config: dict, script_dir: str):
    dag_pathdir = os.path.join(dags_dir, dag_config["dag"].get("dag_id", "").lower())
    _create_dir(dag_pathdir)

    blueprint_code = _get_blueprint_code(dag_config["dag"]["dag_type"])
    blueprint_filelist = _get_blueprint_docs(script_dir, blueprint_code)
    for blueprint_file in blueprint_filelist:
        logging.debug(f"Generating script: {blueprint_file}")
        rendered_script = _render_script(script_dir, blueprint_code, blueprint_file, dag_config)

        filename =  blueprint_file if (blueprint_file != "dag.py") else f"{dag_config['dag']['dag_id']}.py"
        export_script(rendered_script, dag_pathdir, filename)


def _create_ddl(ddl_dir: str, dag_config: dict, script_dir: str):
    ddl_type = os.path.join(ddl_dir, dag_config["dag"].get("type", "").lower())
    _create_dir(ddl_type)

    ddl_script = _render_script(script_dir, None, "ddl.sql", dag_config)
    pathname = os.path.join(ddl_type, dag_config["dag"]["bq_tablename"])
    with open(pathname, "w") as file:
        file.write(ddl_script)


def _create_dir(dirname: str, prune: int = 0):
    dirpath = ""
    walks = os.path.split(dirname)
    for path in walks[:len(walks)-prune]:
        if (path == ""):
            continue
        
        dirpath = os.path.join(dirpath, path)
        if (not os.path.exists(dirpath)):
            os.mkdir(dirpath)
    return dirpath


def _get_blueprint(script_dir: str, script_code: str, filename: str) -> jinja2.Template:
    paths = [p for p in (script_dir, "blueprint", script_code, filename) if p is not None]
    pathname = os.path.join(*paths)
    with open(pathname, "r") as file:
        raw = file.read()
    blueprint = jinja2.Template(raw)
    return blueprint


def _get_blueprint_code(dag_type: str):
    return BLUEPRINT_CODE[dag_type]


def _get_blueprint_docs(script_dir: str, docname: str) -> List[str]:
    pathname = os.path.join(script_dir, "blueprint", docname)
    
    blueprint_filelist = []
    for dirname, _, files in os.walk(pathname):
        direct_dirname = dirname.replace(pathname, "").strip("\\") if (dirname != pathname) else None
        for file in files:
            blueprint_file = os.path.join(*[f for f in (direct_dirname, file) if f is not None])
            blueprint_filelist.append(blueprint_file)
    return blueprint_filelist


def _render_script(script_dir: str, script_code: str, script_name: str, dag_config: dict) -> str:
    blueprint = _get_blueprint(script_dir, script_code, script_name)
    ddl_script = blueprint.render(**dag_config)
    return ddl_script