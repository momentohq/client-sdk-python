# Momento client-sdk-python

Python SDK for Momento, which allows developers to easily use cache that works at scale without any of the operational overhead required by traditional caching solution.

<br/>

## Getting Started :running:

### Requirements

- [Python 3.7](https://www.python.org/downloads/) or above is required
- A Momento Auth Token is required, you can generate one using the [Momento CLI](https://github.com/momentohq/momento-cli)

<br/>

### Installing Momento and Running the Example

Check out our [Python SDK example repo](https://github.com/momentohq/client-sdk-examples/tree/main/python)!

<br/>

### Using Momento

```python
import os
import momento.simple_cache_client as simple_cache_client

# Initializing Momento
_MOMENTO_AUTH_TOKEN = os.getenv('MOMENTO_AUTH_TOKEN')
_ITEM_DEFAULT_TTL_SECONDS = 60
simple_cache_client.init(_MOMENTO_AUTH_TOKEN, _ITEM_DEFAULT_TTL_SECONDS) as cache_client

# Creating a cache named "cache"
_CACHE_NAME = 'cache'
cache_client.create_cache(_CACHE_NAME)

# Sets key with default TTL and get value with that key
_KEY = 'MyKey'
_VALUE = 'MyValue'
cache_client.set(_CACHE_NAME, _KEY, _VALUE)
get_resp = cache_client.get(_CACHE_NAME, _KEY)
print(f'Looked up Value: {str(get_resp.value())}')\

# Sets key with TTL of 5 seconds
cache_client.set(_CACHE_NAME, _KEY, _VALUE, 5)

# Permanently deletes cache
cache_client.delete_cache(_CACHE_NAME)
```

<br/>

## Running Tests :zap:

Integration tests require an auth token for testing. Set the env var `TEST_AUTH_TOKEN` to
provide it. The env `TEST_CACHE_NAME` is also required, but for now any string value works.

Example of running tests against all python versions:

```
TEST_AUTH_TOKEN=<auth token> TEST_CACHE_NAME=<cache name> tox
```

Example of running tests against one specific python version:

```
TOXENV=py39 TEST_AUTH_TOKEN=<auth token> TEST_CACHE_NAME=<cache name> tox
```
