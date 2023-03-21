import click

from scripts.modules import (
    extract
)


# Interface
@click.group()
def cli():
    pass

@cli.command()
@click.option("--count", default=1, help="number of greetings")
@click.argument("name")
def hello(count, name):
    for x in range(count):
        click.echo(f"Hello {name}!")

# Command
def plan(filename: str):
    config_path = extract.extract_tf(filename)
    return config_path


if __name__ == "__main__":
    cli()