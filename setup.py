from setuptools import setup, find_packages

setup(
    name = "airflow-manager",
    version = "1.0",
    packages = find_packages(),
    include_package_data = True,
    install_requires = [
        "Click",
        "jinja2"
    ],
    entry_points = {
        "console_scripts": [
             "airflow-manager = scripts.main:cli"   
        ]
    }
)