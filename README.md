# Graphlet AI Knowledge Graph Factory

<p align="center">
    <img src="https://github.com/Graphlet-AI/graphlet/raw/main/images/graphlet_logo.png" alt="Our mascot Orbits the Squirrel has 5 orbits. Everyone knows this about squirrels!" width="300"/>
</p>

This is the PyPi module for the Graphlet AI Knowledge Graph Factory. Our mission is to create a Spark-based wizard for building large knowledge graphs that makes them easier to build for fewer dolalrs and with less risk.

## Core Features

This project is new, some features we are building are:

1) [Create a Pydantic/PySpark base class for transforming multiple entities into a uniform ontology](https://github.com/Graphlet-AI/graphlet/issues/1)

2) [Create a generic, configurable system for entity resolution of heterogeneous networks](https://github.com/Graphlet-AI/graphlet/issues/3)

3) [Create an efficient pipeline for computing network motifs and aggregating higher order networks](https://github.com/Graphlet-AI/graphlet/issues/5)

4) [Implement efficient motif searching via neural subgraph matching](https://github.com/Graphlet-AI/graphlet/issues/4)

## System Architecture

![Graphlet AI System Architecture](https://github.com/Graphlet-AI/graphlet/raw/main/images/System-Architecture---From-OmniGraffle.png)

## License

This project is created and published under the [Apache License, version 2.0](https://www.apache.org/licenses/LICENSE-2.0).

## Conventions

This project uses pre-commit hooks to enforce its conventions: git will reject commits that don't comply with our various flake8 plugins.

We use [numpy docstring format](https://numpydoc.readthedocs.io/en/latest/format.html#docstring-standard) on all Python classes and functions, which is enforced by [pydocstring](https://github.com/robodair/pydocstring) and [flake8-docstrings](https://gitlab.com/pycqa/flake8-docstrings).

We run `black`, `flake8`, `isort` and `mypy` in [.pre-commit-config.yaml](.pre-commit-config.yaml). All of these are configured in [pyproject.toml](pyproject.toml) except for flake8 which uses [`.flake8`](.flake8).
Flake8 uses the following plugins. We will consider adding any exceptions to the flake config that are warranted, but please document them in your pull requests.

```toml
flake8-docstrings = "^1.6.0"
pydocstyle = "^6.1.1"
flake8-simplify = "^0.19.2"
flake8-unused-arguments = "^0.0.10"
flake8-class-attributes-order = "^0.1.3"
flake8-comprehensions = "^3.10.0"
flake8-return = "^1.1.3"
flake8-use-fstring = "^1.3"
flake8-builtins = "^1.5.3"
flake8-functions-names = "^0.3.0"
flake8-comments = "^0.1.2"
```

## Developer Setup

This project is in a state of development, things are still forming and changing. If you are here, it must be to contribute :)
### Dependencies

We manage dependencies with [poetry](https://python-poetry.org/) which are managed (along with most settings) in [pyproject.toml](pyproject.toml).

To build the project, run:

```bash
poetry install
```

To add a PyPi package, run:

```bash
poetry add <package>
```

To add a development package, run:

```bash
poetry add --dev <package>
```

If you do edit [pyproject.toml](pyproject.toml) you must update to regenerate [poetry.lock](poetry.lock):

```bash
poetry update
```

### Pre-Commit Hooks

We use [pre-commit](https://pre-commit.com/) to run [black](https://github.com/psf/black), [flake8](https://flake8.pycqa.org/en/latest/), [isort](https://pycqa.github.io/isort/) and [mypy](http://mypy-lang.org/). This is configured in [.pre-commit-config.yaml](.pre-commit-config.yaml).

### VSCode Settings

The following [VSCode](https://code.visualstudio.com/) settings are defined for the project in [.vscode/settings.json](.vscode/settings.json) to ensure code is formatted consistent with our pre-commit hooks:

```json
{
    "editor.rulers": [90, 120],
    "[python]": {
        "editor.defaultFormatter": "ms-python.python",
        "editor.formatOnSave": true,
        "editor.codeActionsOnSave": {"source.organizeImports": true},
    },
    "python.jediEnabled": false,
    "python.languageServer": "Pylance",
    "python.linting.enabled": true,
    "python.formatting.provider": "black",
    "python.sortImports.args": ["--profile", "black"],
    "python.linting.pylintEnabled": false,
    "python.linting.flake8Enabled": true,
    "autoDocstring.docstringFormat": "numpy",
}
```
