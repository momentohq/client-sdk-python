# Welcome to client-sdk-python contributing guide :wave:

Thank you for taking your time to contribute to our Python SDK!
<br/>
This guide will provide you information to start your own development and testing.
<br/>
Happy coding :dancer:
<br/>

## Minimum Python version :snake:

Our minimum supported python version is currently `3.7`. This is the oldest version of python that is
currently supported by the python maintainers as well:

https://en.wikipedia.org/wiki/History_of_Python#Table_of_versions

We rely on some features in `asyncio` that were introduced in `3.7`. If we have a customer use case that
requires support for `3.6` then we can revisit at that time.

<br/>

## Requirements :eyes:

- [pyenv](https://github.com/pyenv/pyenv)
- [poetry](https://python-poetry.org/docs/)

<br/>

## First-time setup :wrench:

We use [poetry](https://python-poetry.org/docs/) to manage dependencies and packaging.
This allows us to cleanly separate release vs development dependencies, as well as
streamline deployment.

<br/>

### Make sure you have the required python runtimes

We currently test against multiple python versions: 3.7, 3.8, 3.9, and 3.10.
For whichever subset of these you wish to use locally, you will need to make sure
that you have a `pyenv` installation of that version. For example:

```
pyenv install 3.7.13
pyenv install 3.8.13
pyenv install 3.9.12
pyenv install 3.10.4
```

NB: These are examples. You will need one of each (3.7, 3.8, 3.9, 3.10)
but the patch version ("13" in "3.7.13") can be the latest one.

<be/>

### Configure poetry to use your particular python version

We recommend developing against python 3.7 as it is the lowest common
denominator.

You need to run `pyenv local ...` once to configure this directory with
the list of pyenv python versions that you want tox to be able to use.

e.g. from the SDK root directory:

```
pyenv local 3.7.13
```

This will create a file called `.python-version` containing the desired
version numbers, and from now on whenever you `cd` into this directory,
`pyenv` will automatically know which python versions you want to use.

<br />

### Install dependencies with Poetry

If you haven't installed poetry yet, follow the instructions on the [website](https://python-poetry.org/docs/#installation).

Tell poetry to use your desired python version:

```
poetry env use $(which python)
```

As a matter of preference, you may like having the virtual environment in the project folder:

```
poetry config virtualenvs.in-project true
```

Finally install the project dependencies as specified in `pyproject.toml`:

```
poetry install
```

<br />

### Setting up IDE

The only trick to setting up your IDE is to point it to your preferred
virtualenv created by `tox`. These live in the `.tox` dir; e.g.
`.tox/py39/bin/python`.

- Visual Studio Code

  Use `Cmd` + `Shift` + `P` to search for `Python: Interpreter` and select:
  `.venv/py37/bin/python`

- IntelliJ

  `File`->`New Project`->`Python`; when you get to the screen where it lets you
  choose your python SDK, you may need to click `Edit`, then the `+` button
  to register a new Python SDK, then choose the `Existing` radio button, then
  navigate to e.g. `.venv/py37/bin/python`.

<br />

## Developing :computer:

To test your changes in a python shell, just use poetry:

```
poetry run python -m unittest discover
```

<br/>

## Linting :flashlight:

We use `mypy` for ensuring that we have type annotations and that the types are correct.
(We should use it on all of our python projects; there is a movement
in the python community to try to create type stubs for all major open source libraries
via [typeshed](https://github.com/python/typeshed), similar to the [DefinitelyTyped](https://github.com/DefinitelyTyped/DefinitelyTyped)
project for javascript/typescript. In the not-too-distant future this will become table
stakes for open source python libraries.)

We also use `flake8` for static analysis.

We use `black` for formatting and checking the formatting of the code.

To run the linters:

```
poetry run mypy
poetry run flake8
```

To run the code formatter:

```
poetry run black
```

<br/>

## Tests :zap:

Integration tests require an auth token for testing. Set the env var `TEST_AUTH_TOKEN` to
provide it. The env `TEST_CACHE_NAME` is also required, but for now any string value works.

Example of running tests against all python versions:

```
TEST_AUTH_TOKEN=<auth token> TEST_CACHE_NAME=<cache name> poetry run python -m unittest discover
```

<br/>
