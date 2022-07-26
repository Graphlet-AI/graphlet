# Graphlet AI Knowledge Graph Factory

This is the PyPi module for the Graphlet AI Knowledge Graph Factory.

## Conventions

This project uses pre-commit hooks to enforce its conventions: git will reject commits that don't comply with our various flake8 plugins.

We use [numpy docstring format](https://numpydoc.readthedocs.io/en/latest/format.html#docstring-standard) on all Python classes and functions, which is enforced [pydocstring](https://github.com/robodair/pydocstring) and [flake8-docstrings](https://gitlab.com/pycqa/flake8-docstrings).

We run `black`, `flake8`, `isort` and `mypy` in [.pre-commit-config.yaml](.pre-commit-config.yaml). All of these except flake8 are configured in [pyproject.toml](pyproject.toml) except for Flake8, which is configured in [.flake8](.flake8).

Flake8 uses the following plugins. We will consider adding any exceptions to the flake config that are warranted, but please document them in your pull requests.

```toml
flake8-docstrings = "^1.6.0"
pydocstyle = "^6.1.1"
flake8-spellcheck = "^0.28.0"
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
