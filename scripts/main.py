import logging
import click

from scripts.modules import (
    extract
)


# Interface
@click.group()
@click.option('--debug/--no-debug', default=False, help="Activate logging")
def cli(debug: bool):
    click.echo(f"INFO - Debug mode is {'ON' if (debug) else 'OFF'}")
    
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


if __name__ == "__main__":
    cli()