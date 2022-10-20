# Momento client-sdk-python

:warning: Experimental SDK :warning:

Python SDK for Momento is experimental and under active development.
There could be non-backward compatible changes or removal in the future.
Please be aware that you may need to update your source code with the current version of the SDK when its version gets upgraded.

---

<br/>

Python SDK for Momento, a serverless cache that automatically scales without any of the operational overhead required by traditional caching solutions.

<br/>

## Getting Started :running:

### Requirements

- [Python 3.7](https://www.python.org/downloads/) or above is required
- A Momento Auth Token is required, you can generate one using the [Momento CLI](https://github.com/momentohq/momento-cli)

<br/>

### Installing Momento and Running the Example

Check out our [Python SDK example](/examples/)!

<br/>

### Using Momento

```python
import os
from momento import simple_cache_client as scc

# Initializing Momento
_MOMENTO_AUTH_TOKEN = os.getenv('MOMENTO_AUTH_TOKEN')
_ITEM_DEFAULT_TTL_SECONDS = 60
with scc.SimpleCacheClient(_MOMENTO_AUTH_TOKEN, _ITEM_DEFAULT_TTL_SECONDS) as cache_client:
    # Creating a cache named "cache"
    _CACHE_NAME = 'cache'
    cache_client.create_cache(_CACHE_NAME)

    # Sets key with default TTL and get value with that key
    _KEY = 'MyKey'
    _VALUE = 'MyValue'
    cache_client.set(_CACHE_NAME, _KEY, _VALUE)
    get_resp = cache_client.get(_CACHE_NAME, _KEY)
    print(f'Looked up Value: {str(get_resp.value())}')

    # Sets key with TTL of 5 seconds
    cache_client.set(_CACHE_NAME, _KEY, _VALUE, 5)

    # Permanently deletes cache
    cache_client.delete_cache(_CACHE_NAME)
```

<br/>
