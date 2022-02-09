# Momento SDK

## Getting started

### Requirements

- [Python 3.7 or above](https://www.python.org/downloads/)
- A Momento Auth Token - you can generate one using [the Momento CLI](https://github.com/momentohq/momento-cli)

### Installation

```
python3 -m pip install momento
```

<br/>

## Running the Example

Check out [Momento SDK Example](https://github.com/momentohq/client-sdk-examples/tree/main/python) to run the following examples.

```
MOMENTO_AUTH_TOKEN=<YOUR_TOKEN> python3 example.py
```

For async version, run as follows:

```
MOMENTO_AUTH_TOKEN=<YOUR_TOKEN> python3 example_async.py
```

To turn on SDK debug logs, run as follows:

```
DEBUG=true MOMENTO_AUTH_TOKEN=<YOUR_TOKEN> python3 example.py
```

<br/>

## Using SDK in your project

Add [`momento==0.8.1`](https://pypi.org/project/momento-wire-types/0.8.1/) to `requirements.txt` or any other dependency management framework used by your project.

To install directly to your system:

```
python3 -m pip install momento-wire-types==0.8.1
```
