import os
from setuptools import setup

def package_files(directory):
    paths = []
    for (path, _, filenames) in os.walk(directory):
        for filename in filenames:
            paths.append(os.path.join("..", path, filename))
    return paths

data_paths = package_files("airflow_manager/blueprint")

setup(
    name = "airflow-manager",
    version = "0.1",
    packages = ["airflow_manager", "airflow_manager.modules"],
    package_dir = {"airflow_manager": "airflow_manager"},
    package_data = {"airflow_manager": data_paths},
    include_package_data = True,
    install_requires = [
        "Click",
        "jinja2",
        "openpyxl",
        "gspread",
        "flatten-dict",
        "prettytable"
    ],
    entry_points = {
        "console_scripts": [
             "airflow-manager = airflow_manager.main:cli"   
        ]
    }
)