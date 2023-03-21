import sys

from modules import (
    extract
)

def plan(filename: str):
    config_path = extract.extract_tf(filename)
    return config_path


if __name__ == "__main__":
    _, filename = sys.argv
    plan(filename)