# Incubating
The `momento.incubating` package has work-in-progress features that may or may be be in the final version.

## Dictionary Methods

This demonstrates the methods and response types for a dictionary data type in the cache.

1. Initialize the cache
```
>>> import momento.incubating.simple_cache_client as scc
>>> client = scc.init(auth_token=AUTH_TOKEN, item_default_ttl_seconds=DEFAULT_TTL)
```

2. Set a dictionary
```
>>> client.dictionary_set(cache_name="my-example-cache", dictionary_name="my-dictionary", dictionary={"key1": "value1", "key2": "value2"})
CacheDictionarySetResponse(key='my-dictionary', value={'key1': 'value1', 'key2': 'value2'})
```

3. Get a value from a dictionary
```
>>> client.dictionary_get(cache_name="my-example-cache", dictionary_name="my-dictionary", key="key1")
CacheDictionaryGetResponse(value='value1', result=<CacheGetStatus.HIT: 1>)
```

4. Get the entire dictionary
```
>>> dictionary_get_all_response = client.dictionary_get_all(cache_name="my-example-cache", dictionary_name="my-dictionary")
>>> dictionary_get_all_response
CacheDictionaryGetAllResponse(value={b'key1': CacheDictionaryValue(value='value1'), b'key2': CacheDictionaryValue(value='value2')}, result=<CacheGetStatus.HIT: 1>)

>>> stored_dictionary = dictionary_get_all_response.value()
>>> stored_dictionary
{'key1': CacheDictionaryValue(value='value1'), 'key2': CacheDictionaryValue(value='value2')}

>>> stored_dictionary["key1"].value()
'value1'
>>> stored_dictionary["key1"].value_as_bytes()
b'value1'
```

5. Lastly we may store and index the keys as bytes
```
>>> client.dictionary_set("my-example-cache", "my-dictionary", {b"key1": b"value1", b"key2": b"value2"})
CacheDictionarySetResponse(key=my-dictionary, value={b'key1': b'value1', b'key2': b'value2'})

>>> stored_dictionary = client.dictionary_get_all("my-example-cache", "my-dictionary").value(keys_as_bytes=True)
>>> stored_dictionary
{b'key1': CacheDictionaryValue(value='value1'), b'key2': CacheDictionaryValue(value='value2')}

>>> stored_dictionary[b"key1"].value_as_bytes()
b'value1'
```
