import os
import logging
import click

from scripts.modules import (
    export,
    extract,
    project
)


# Interface
@click.group()
@click.option('-q', '--quite', is_flag=True, default=False, help="Run in quite mode")
@click.pass_context
def cli(ctx: click.core.Context, quite: bool):
    ctx.ensure_object(dict)
    ctx.obj["scripts_dir"] = os.path.dirname(__file__)

    level = logging.ERROR if (quite) else logging.INFO
    logging.basicConfig(
        level = level,
        format = "%(levelname)s - %(message)s",
        datefmt = "%Y-%m-%d %H:%M:%S"
    )


# Command
@cli.command()
@click.argument("filename", type=click.Path(exists=True), default="deployment.json")
@click.pass_context
def deploy(ctx: click.core.Context, filename: str):
    """Deploy selected configuration"""

    logging.info(f"Deploy from `{filename}` configuration")
    export.export_project(filename, script_dir=ctx.obj["scripts_dir"])


@cli.command()
def init():
    """Initiate current directory to Airflow repository"""

    logging.info(f"Initiate Airflow repository")
    project.init_project()


@cli.group()
def plan():
    """Managing deployment plan creation and versioning"""
    pass

@plan.command(name="create")
@click.option("--xlsx", type=bool, is_flag=True, default=False, help="Set mode to read xlsx")
@click.argument("id", type=str)
def plan_create(xlsx: bool, id: str):
    """Extract config from Transformation Rules"""
    
    logging.info(f"Plan deployment with transformation rules {'xlsx' if (xlsx) else 'sheet'}:  `{id}`")
    version_dir = project.get_version_dir()
    if (xlsx):
        extract.extract_tf(filename=id, version_dir=version_dir)
    else:
        extract.extract_tf(sheet_id=id, version_dir=version_dir)

@plan.command(name="list")
def plan_list():
    """Get list of saved deployment plan"""
    plan_list = project.get_plan_list()
    if (plan_list == []):
        logging.info("There is no available deployment plan")


if __name__ == "__main__":
    cli(obj={})