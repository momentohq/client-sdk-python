# client-sdk-python
Python SDK for Momento

# Requirements
* Python 3
* [Virtual environment setup](https://packaging.python.org/guides/installing-using-pip-and-virtual-environments/#installing-virtualenv)

# Setup pre-requisites
`./prepare_local_dev_env.sh`
The script creates a python virtual environment and installs dependencies

## Setting up IDE
### Visual Studio Code
Use `Cmd` + `Shift` + `P` to search for `Python: Interpreter` and select:
`./client_sdk_python_env/bin/python`

# Developing
Once your pre-requisites are accomplished

Run the following command to start your virtual environment

`source client_sdk_python_env/bin/activate`

To install the package under development

`pip install -e .`

To test your changes you can then just run your python shell as follows:

`python` this will start the interactive shell or if you prefer you may put all
your code in a my_test.py file and run `python my_test.py`

# Tests

- Integration tests require an auth token for testing. Set this as `TEST_AUTH_TOKEN`.
- `TEST_CACHE_NAME` is required, but for now any string value works.

Run:

```
TEST_AUTH_TOKEN=<auth token> TEST_CACHE_NAME=<cache name> python3 tests/*
```
