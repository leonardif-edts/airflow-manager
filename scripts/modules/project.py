import os
import json

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
        "deployment": {
            "current_version": None,
            "update_tm": None
        }
    }
    metadata_pathname = os.path.join(manager_dir, "metadata.json")
    with open(metadata_pathname, "w") as file:
        file.write(
            json.dumps(
                {k:project_metadata[k] for k in sorted(project_metadata.keys())},
                indent=2
            )
        )
