import os
from typing import List

import jinja2

from scripts import const


# Public
def get_blueprint_code(dag_type: str):
    return const.BLUEPRINT_CODE[dag_type]

def get_blueprint_filelist(script_dir: str, blueprint_code: str) -> List[str]:
    pathname = os.path.join(script_dir, "blueprint", blueprint_code)
    
    blueprint_filelist = []
    for dirname, _, files in os.walk(pathname):
        direct_dirname = dirname.replace(pathname, "").strip("\\") if (dirname != pathname) else None
        for file in files:
            blueprint_file = os.path.join(*[f for f in (direct_dirname, file) if f is not None])
            blueprint_filelist.append(blueprint_file)
    return blueprint_filelist

def render_script(script_dir: str, script_code: str, script_name: str, dag_config: dict) -> str:
    blueprint = _get_blueprint(script_dir, script_code, script_name)
    ddl_script = blueprint.render(**dag_config)
    return ddl_script


# Private
def _get_blueprint(script_dir: str, script_code: str, filename: str) -> jinja2.Template:
    paths = [p for p in (script_dir, "blueprint", script_code, filename) if p is not None]
    pathname = os.path.join(*paths)
    with open(pathname, "r") as file:
        raw = file.read()
    blueprint = jinja2.Template(raw)
    return blueprint