from typing import Dict, Tuple
from jinja2 import Template
import typer

import os
from pathlib import Path
import re

SESSION_REGEX = re.compile(
    r"(?:\*\s+)?([a-zA-Z\-_0-9\.]+)\(((?:[a-zA-Z\-_0-9\.]+='[/a-zA-Z\-_0-9\.\ ,]+',?)+)\)"
)


def parse_session(session_str: str) -> Tuple[str, Dict[str, str]]:

    m = SESSION_REGEX.match(session_str)

    if not m:
        raise ValueError(f"Invalid session string: '{session_str}'")

    session_name = m.group(1)
    session_params = {}

    for g in m.group(2).split(","):
        key, value = g.strip(" ").split("=")
        session_params[key] = value.strip("'")

    return session_name, session_params


def main(session_str: str, workdir: Path):

    session_name, session_params = parse_session(session_str)

    fname = Path(session_params["notebook_file"]).name
    fname_relative = Path(session_params["notebook_file"]).relative_to(os.getcwd())
    fname_delimited = fname.replace(".", "__")

    template = Template(workdir.joinpath("test_notebook.yml.tmpl").open("r").read())

    output_path = workdir.joinpath(f"test_{fname_delimited}.yml")

    output_path.open("w+").write(
        template.render(
            fname=str(output_path),
            session=f"{session_name}(notebook_file={fname_relative})",
        )
    )


if __name__ == "__main__":
    typer.run(main)
