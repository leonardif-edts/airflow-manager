import os
import json
import jinja2


def get_config(filename: str) -> dict:
    with open(filename, "r") as file:
        config = json.loads(file.read())
    return config

def get_template_script(dirname: str, template: str) -> jinja2.Template:
    pathname = os.path.join("templates", dirname, template)
    with open(pathname, "r") as file:
        raw = file.read()
    template_script = jinja2.Template(raw)
    return template_script

def create_dir(dirname: str):
    dirpath = ""
    for path in os.path.split(dirname):
        if (path == ""):
            continue
        
        dirpath = os.path.join(dirpath, path)
        if (not os.path.exists(dirpath)):
            os.mkdir(dirpath)
    return dirpath

def create_ddl(ddl_dir: str, dag_config: dict, ddl_template: str = "standard.sql"):
    ddl_type = os.path.join(ddl_dir, dag_config["dag"].get("type", "").lower())
    create_dir(ddl_type)

    pathname = os.path.join(ddl_type, dag_config["dag"]["bq_tablename"])
    ddl_script = render_script("ddl", "standard.sql", dag_config)
    with open(pathname, "w") as file:
        file.write(ddl_script)

def render_script(dirname: str, template: str, dag_config: str):
    template_script = get_template_script(dirname ,template)
    rendered_script = template_script.render(**dag_config)
    return rendered_script


if __name__ == "__main__":
    config = get_config("config.json")

    # Get / Create directories: dags, ddl
    print("Create/Get DAG and DDL directory")
    dags_dir = create_dir("dags")
    ddl_dir = create_dir("ddl")
    
    for dag in config["dags"]:
        print(f"Create/Update DAG: {dag['dag_id']}")
        dag_config = {"dag": dag, "project": config["project"]}

        create_ddl(ddl_dir, dag_config)

        # Get template
        # template = get_template()

        # Generate Script
        # create_dag()