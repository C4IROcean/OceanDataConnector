# from nox_poetry import session, Session
from nox import parametrize, session, Session
import os
import sys
from glob import glob
import shutil
import tempfile
from typing import Iterable
from dotenv import load_dotenv

PYTHON_VERSIONS = ["3.9"]

NOTEBOOK_DIRS = [
    "data",
    "Data Products",
    "SDK"
        ]

NOTEBOOK_CELL_EXECUTION_TIMEOUT_SECONDS = 60
CWD = os.getcwd()

#
# Set up NOX environment
#

load_dotenv()

def get_notebook_files() -> Iterable[str]:
    for dir in NOTEBOOK_DIRS:
        for root, dirs, files in os.walk(os.path.join(CWD, dir)):
            for name in files:
                if name.endswith(".ipynb"):
                    yield os.path.join(root, name)


notebook_files = list(get_notebook_files())

#
# Define NOX sessions
#

# @session(python=PYTHON_VERSIONS)
# def install_deps(session: Session):
#     session.install("-r", "requirements.txt")


@session(python=PYTHON_VERSIONS, reuse_venv=True)
@parametrize("notebook_file", notebook_files)
def test_notebook(session: Session, notebook_file: str):
    with tempfile.TemporaryDirectory() as tdir:

        session.run(
                # "poetry", "run", "papermill", "--log-level", "INFO", "--execution-timeout", str(NOTEBOOK_CELL_EXECUTION_TIMEOUT_SECONDS),
                "poetry", "run", "papermill", "--log-level", "INFO",
                "--cwd", os.path.dirname(notebook_file),
                notebook_file, os.path.join(tdir, "output.ipynb"),
                env = {
                        "DATABASE_URL": os.environ["DATABASE_URL"],
                        "ODE_CONNECTION_STR": os.environ["ODE_CONNECTION_STR"],
                        "ODE_MAPBOX_API_TOKEN": os.environ["ODE_MAPBOX_API_TOKEN"],
                    }
                )
