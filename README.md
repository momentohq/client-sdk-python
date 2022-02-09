# client-sdk-python
Python SDK for Momento

# Minimum Python version

Our minimum supported python version is currently `3.7`.  This is the oldest version of python that is
currently supported by the python maintainers as well:

https://en.wikipedia.org/wiki/History_of_Python#Table_of_versions

We rely on some features in `asyncio` that were introduced in `3.7`.  If we have a customer use case that
requires support for `3.6` then we can revisit at that time.

# Requirements

* [pyenv](https://github.com/pyenv/pyenv)
* [Working knowledge of python vitualenv](https://packaging.python.org/guides/installing-using-pip-and-virtual-environments/#installing-virtualenv)

# First-time setup

We use [tox](https://tox.wiki/en/latest/) to run linting, tests, etc.  Tox
allows us to easily test against multiple versions of python as well as
providing a simple way to organize build tasks (akin to a Makefile).

Our tox config is modeled on:
* [pytest project](https://github.com/pytest-dev/pytest/blob/12b288d84af798c36842fb4c973c144068e5c6d0/tox.ini)
* [twisted python](https://github.com/twisted/twisted/blob/171fd5c3331d1d2a8cc8cca2c96d04ea654712bc/tox.ini)

## Make sure you have the required python runtimes

At the top of our [tox.ini](./tox.ini) file, you will see a list of the
different python versions that we currently test against.  For whichever
subset of these you wish to use locally, you will need to make sure that
you have a `pyenv` installation of that version.  For example:

```
pyenv install 3.7.12
pyenv install 3.8.12
pyenv install 3.9.10
pyenv install 3.10.2
```

## Make the pyenv python runtimes visible to tox

You need to run `pyenv local ...` once to configure this directory with
the list of pyenv python versions that you want tox to be able to use.

e.g.:

```
pyenv local 3.7.12 3.8.12 3.9.10 3.10.2
```

This will create a file called `.python-version` containing the desired
version numbers, and from now on whenever you `cd` into this directory,
`pyenv` and `tox` will automatically know which python versions you
want to use.

## First run of `tox` to set up tox virtual envs

If you haven't installed tox yet:

```
pip install tox
```

If you want to test with all of the versions of python that we have listed
in our `tox.ini`, and you have already run `pyenv local` to register all
of the versions, run this command to do the initial setup of virtual envs
for all the python versions:

```
tox --notest
```

If you only intend to develop with one specific version of python, you can
do either of the following to limit `tox` to one version (in these examples,
python 3.9):

```
TOXENV=python3.9 tox --notest
```

or:

```
tox -e python3.9 --notest
```


## Setting up IDE

The only trick to setting up your IDE is to point it to your preferred
virtualenv created by `tox`.  These live in the `.tox` dir; e.g.
`.tox/py39/bin/python`.

### Visual Studio Code
Use `Cmd` + `Shift` + `P` to search for `Python: Interpreter` and select:
`.tox/py39/bin/python`

### IntelliJ

`File`->`New Project`->`Python`; when you get to the screen where it lets you
choose your python SDK, you may need to click `Edit`, then the `+` button
to register a new Python SDK, then choose the `Existing` radio button, then
navigate to e.g. `.tox/py39/bin/python`.

# Developing

To test your changes in a python shell, just launch python from the desired
tox virtualenv, e.g.:

`.tox/py39/bin/python`: this will start the interactive shell.

Alternately you may put all of your code in a my_test.py file and
run `.tox/py39/bin/python my_test.py`

# Linting

We use `mypy` for ensuring that we have type annotations and that the types are correct.
(We should use it on all of our python projects; there is a movement
in the python community to try to create type stubs for all major open source libraries
via [typeshed](https://github.com/python/typeshed), similar to the [DefinitelyTyped](https://github.com/DefinitelyTyped/DefinitelyTyped)
project for javascript/typescript.  In the not-too-distant future this will become table
stakes for open source python libraries.)

We use `black` for formatting and checking the formatting of the code.

To run the lint checks:

```
tox -e lint
```

To run the auto-formatter:

```
tox -e format
```

# Tests

Integration tests require an auth token for testing. Set the env var `TEST_AUTH_TOKEN` to
provide it.  The env `TEST_CACHE_NAME` is also required, but for now any string value works.

Example of running tests against all python versions:

```
TEST_AUTH_TOKEN=<auth token> TEST_CACHE_NAME=<cache name> tox
```

Example of running tests against one specific python version:

```
TOXENV=py39 TEST_AUTH_TOKEN=<auth token> TEST_CACHE_NAME=<cache name> tox
```

# Linting

TODO (add `black`)

# Mypy

TODO (add `mypy`)
