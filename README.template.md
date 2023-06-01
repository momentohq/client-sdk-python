{{ ossHeader }}

## Packages

The Momento Python SDK package is [available on PyPi](https://pypi.org/project/momento/).

## Usage

The examples below require an environment variable named MOMENTO_AUTH_TOKEN which must
be set to a valid [Momento authentication token](https://docs.momentohq.com/docs/getting-started#obtain-an-auth-token).

Python 3.10 introduced the `match` statement, which allows for [structural pattern matching on objects](https://peps.python.org/pep-0636/#adding-a-ui-matching-objects).
If you are running python 3.10 or greater, here is a quickstart you can use in your own project:

```python
{% include "./examples/py310/readme.py" %}
```

The above code uses [structural pattern matching](https://peps.python.org/pep-0636/), a feature introduced in Python 3.10.
Using a Python version less than 3.10? No problem. Here is the same example compatible across all versions of Python:

```python
{% include "./examples/prepy310/readme.py" %}
```

## Getting Started and Documentation

Documentation is available on the [Momento Docs website](https://docs.momentohq.com).

## Examples

Working example projects, with all required build configuration files, are available for both Python 3.10 and up
and Python versions before 3.10:

* [Python 3.10+ examples](./examples/py310)
* [Pre-3.10 Python examples](./examples/prepy310)

## Developing

If you are interested in contributing to the SDK, please see the [CONTRIBUTING](./CONTRIBUTING.md) docs.

{{ ossFooter }}
