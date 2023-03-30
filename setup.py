from setuptools import setup

setup(
    name = "airflow-manager",
    version = "0.1",
    packages = ["airflow_manager", "airflow_manager.modules"],
    package_dir = {"airflow_manager": "airflow_manager"},
    package_data = {"airflow_manager": ["blueprint/*"]},
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