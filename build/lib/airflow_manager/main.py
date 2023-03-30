import os
import logging
import click
import prettytable

from airflow_manager import const
from airflow_manager.modules import (
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
@cli.group()
@click.pass_context
def deploy(ctx):
    """Deploy selected configuration"""
    pass

@deploy.command(name="create")
@click.argument("version", type=str, required=False)
@click.pass_context
def deploy_create(ctx: click.core.Context, version: str):
    """Deploy selected configuration"""

    version = version if (version) else project.get_latest_version()
    if (version):
        try:
            logging.info(f"Deploy from `{version}` configuration")
            version_dir = project.get_version_dir()
            export.export_project(version, script_dir=ctx.obj["scripts_dir"], version_dir=version_dir)
        except EOFError:
            logging.info("No `.airflow-manager` config directory found")
    else:
        logging.info("No plan found")

@deploy.command(name="list")
def deploy_list():
    """Show list of created deployment config from Transformation Rules"""
    try:
        deploys = project.get_log_list("deploy_logs.json", const.PLAN_DEPLOY_COLUMNS)
        if (len(deploys) > 1):
            tbl = prettytable.PrettyTable(deploys[0])
            for row in deploys[1:]:
                tbl.add_row(row)
            print(tbl)
        else:
            logging.info("No deploy logs found")
    except EOFError:
        logging.info("No `.airflow-manager` config directory found")


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
    try:
        logging.info(f"Plan deployment with transformation rules {'xlsx' if (xlsx) else 'sheet'}:  `{id}`")
        version_dir = project.get_version_dir()
        if (xlsx):
            extract.extract_tf(filename=id, version_dir=version_dir)
        else:
            extract.extract_tf(sheet_id=id, version_dir=version_dir)
    except EOFError:
        logging.info("No `.airflow-manager` config directory found")

@plan.command(name="list")
def plan_list():
    """Show list of created deployment config from Transformation Rules"""
    try:
        plans = project.get_log_list("plan_logs.json", const.PLAN_LOG_COLUMNS)
        if (len(plans) > 1):
            tbl = prettytable.PrettyTable(plans[0])
            for row in plans[1:]:
                tbl.add_row(row)
            print(tbl)
        else:
            logging.info("No logs for plan")
    except EOFError:
        logging.info("No `.airflow-manager` config directory found")


if __name__ == "__main__":
    cli(obj={})