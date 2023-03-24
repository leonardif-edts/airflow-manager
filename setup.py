from setuptools import setup, find_packages

setup(
    name = "airflow-manager",
    version = "0.1",
    packages = find_packages(),
    include_package_data = True,
    install_requires = [
        "Click",
        "jinja2",
        "openpyxl",
        "gspread"
    ],
    entry_points = {
        "console_scripts": [
             "airflow-manager = scripts.main:cli"   
        ]
    }
)