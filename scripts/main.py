import os
import logging
import click

from scripts.modules import (
    extract,
    export
)


# Interface
@click.group()
@click.option('--debug/--no-debug', default=False, help="Activate logging")
@click.pass_context
def cli(ctx: click.core.Context, debug: bool):
    click.echo(f"INFO - Debug mode is {'ON' if (debug) else 'OFF'}")
    ctx.ensure_object(dict)
    ctx.obj["scripts_dir"] = os.path.dirname(__file__)

    level = logging.DEBUG if (debug) else logging.INFO
    logging.basicConfig(
        level = level,
        format = "%(levelname)s - %(message)s",
        datefmt = "%Y-%m-%d %H:%M:%S"
    )


# Command
@cli.command()
@click.argument("filename", type=click.Path(exists=True))
def plan(filename: str):
    """Extract config from Transformation Rules"""

    logging.info(f"Plan deployment with `{filename}` transformation rules")
    config_path = extract.extract_tf(filename)
    return config_path


@cli.command()
@click.argument("filename", type=click.Path(exists=True), default="config.json")
@click.pass_context
def deploy(ctx: click.core.Context, filename: str):
    """Deploy selected configuration"""

    logging.info(f"Deploy from `{filename}` configuration")
    export.export_project(filename, script_dir=ctx.obj["scripts_dir"])


if __name__ == "__main__":
    cli()