<head>
  <meta name="Momento Python Client Library Documentation" content="Python client software development kit for Momento Cache">
</head>
<img src="https://docs.momentohq.com/img/logo.svg" alt="logo" width="400"/>

[![project status](https://momentohq.github.io/standards-and-practices/badges/project-status-official.svg)](https://github.com/momentohq/standards-and-practices/blob/main/docs/momento-on-github.md)
[![project stability](https://momentohq.github.io/standards-and-practices/badges/project-stability-stable.svg)](https://github.com/momentohq/standards-and-practices/blob/main/docs/momento-on-github.md)

# Momento Python Client Library

Momento Cache is a fast, simple, pay-as-you-go caching solution without any of the operational overhead
required by traditional caching solutions.  This repo contains the source code for the Momento Python client library.

To get started with Momento you will need a Momento Auth Token. You can get one from the [Momento Console](https://console.gomomento.com).

* Website: [https://www.gomomento.com/](https://www.gomomento.com/)
* Momento Documentation: [https://docs.momentohq.com/](https://docs.momentohq.com/)
* Getting Started: [https://docs.momentohq.com/getting-started](https://docs.momentohq.com/getting-started)
* Python SDK Documentation: [https://docs.momentohq.com/develop/sdks/python](https://docs.momentohq.com/develop/sdks/python)
* Discuss: [Momento Discord](https://discord.gg/3HkAKjUZGq)

## Packages

The Momento Python SDK package is available on pypi: [momento](https://pypi.org/project/momento/).

## Usage

The examples below require an environment variable named MOMENTO_AUTH_TOKEN which must
be set to a valid [Momento authentication token](https://docs.momentohq.com/docs/getting-started#obtain-an-auth-token).

Python 3.10 introduced the `match` statement, which allows for [structural pattern matching on objects](https://peps.python.org/pep-0636/#adding-a-ui-matching-objects).
If you are running python 3.10 or greater, here is a quickstart you can use in your own project:

```python
from datetime import timedelta

from momento import CacheClient, Configurations, CredentialProvider
from momento.responses import CacheGet, CacheSet, CreateCache, ListCaches

cache_client = CacheClient(
    Configurations.Laptop.v1(),
    CredentialProvider.from_environment_variable("MOMENTO_AUTH_TOKEN"),
    timedelta(seconds=60)
)

cache_client.create_cache("cache")
cache_client.set("cache", "my-key", "my-value")
get_response = cache_client.get("cache", "my-key")
match get_response:
    case CacheGet.Hit() as hit:
        print(f"Got value: {hit.value_string}")
    case _:
        print(f"Response was not a hit: {get_response}")

```

The above code uses [structural pattern matching](https://peps.python.org/pep-0636/), a feature introduced in Python 3.10.
Using a Python version less than 3.10? No problem. Here is the same example compatible across all versions of Python:

```python
from datetime import timedelta

from momento import CacheClient, Configurations, CredentialProvider
from momento.responses import CacheGet, CacheSet, CreateCache

cache_client = CacheClient(
    configuration=Configurations.Laptop.v1(),
    credential_provider=CredentialProvider.from_environment_variable('MOMENTO_AUTH_TOKEN'),
    default_ttl=timedelta(seconds=60)
)
cache_client.create_cache("cache")
cache_client.set("cache", "myKey", "myValue")
get_response = cache_client.get("cache", "myKey")
if isinstance(get_response, CacheGet.Hit):
    print(f"Got value: {get_response.value_string}")

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

----------------------------------------------------------------------------------------
For more info, visit our website at [https://gomomento.com](https://gomomento.com)!
