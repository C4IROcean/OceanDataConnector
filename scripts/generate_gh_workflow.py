import os
import re
from pathlib import Path
from typing import Dict, Tuple

import typer
from jinja2 import Template
from nox._options import noxfile_options
from nox.tasks import discover_manifest, load_nox_module
from slugify import slugify

SESSION_REGEX = re.compile(r"(?:\*\s+)?([a-zA-Z\-_0-9\.]+)\(((?:[a-zA-Z\-_0-9\.]+='[/a-zA-Z\-_0-9\.\ ,]+',?)+)\)")


def parse_session(session_str: str) -> Tuple[str, Dict[str, str]]:
    m = SESSION_REGEX.match(session_str)

    if not m:
        raise ValueError(f"Invalid session string: '{session_str}'")

    session_name = m.group(1)
    session_params = {}

    key, value = m.group(2).strip(" ").split("=")
    session_params[key] = value.strip("'")

    return session_name, session_params


def main(workdir: Path):
    noxfile_options.noxfile = "noxfile.py"
    noxfile_options.extra_pythons = []
    noxfile_options.posargs = []
    manifests = discover_manifest(load_nox_module(noxfile_options), noxfile_options)

    sessions = [man.signatures[0] for man in manifests]  # notype

    output_sessions = {}

    for session_str in sessions:
        session_name, session_params = parse_session(session_str)

        fname = Path(session_params["notebook_file"]).name
        fname_relative = Path(session_params["notebook_file"]).relative_to(os.getcwd())
        # fname_delimited = fname.replace(".", "__")

        slug = slugify(fname)
        output_sessions[
            slug
        ] = f"{session_name}(notebook_file='/__w/OceanDataConnector/OceanDataConnector/{fname_relative}')"

    output_path = workdir.joinpath("test_notebook.yml")

    template = Template(workdir.joinpath("test_notebook.yml.tmpl").open("r").read())
    output_path.open("w+").write(
        template.render(
            sessions=output_sessions,
        )
    )


if __name__ == "__main__":
    typer.run(main)
