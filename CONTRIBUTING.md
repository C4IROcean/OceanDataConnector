# Contributing

## Installing dependencies

This project uses the [poetry](https://python-poetry.org/) package manager. For
installation instructions, please see the
[official documentation](https://python-poetry.org/docs/#installation).

### Getting started

In order to get started, simply clone the repository and install the
dependencies using poetry

```sh
git clone https://github.com/C4IROcean/OceanDataConnector.git
cd OceanDataConnector
poetry install
```

### Using pre-commit hooks

In order to run the pre-commit hooks:

```sh
poetry run pre-commit -a # Will run the pre-commit hooks on the entire codebase
```

The pre-commit hook can be installed with the following command. This will
ensure that the pre-commit hooks are run on all staged files when commiting.

```sh
poetry run pre-commit install
```

### Running notebook tests locally

In order to run the notebook tests locally,
[Docker](https://docs.docker.com/engine/) must be installed. Please see the
[official documentation](https://docs.docker.com/engine/install/) for install
instructions.

The notebook tests can be run by executing the following two commands.

```sh
docker build . -t odc-example-notebooks
docker run odc-example-notebooks
```
