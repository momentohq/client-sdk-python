# Incubating

The `momento.incubating` package has work-in-progress features that may or may be be in the final version.

## Dictionary Methods

This demonstrates the methods and response types for a dictionary data type in the cache.

1. Initialize the cache

```python
>>> import momento.incubating.simple_cache_client as scc
>>> client = scc.init(auth_token=AUTH_TOKEN, item_default_ttl_seconds=DEFAULT_TTL)
```

2. Set one item in a dictionary

```python
>>> client.dictionary_set(cache_name="my-cache", dictionary_name="my-dictionary", key="key1", value="value1", refresh_ttl=False)
CacheDictionarySetUnaryResponse(dictionary_name='my-dictionary', key=b'key1', value=b'value1')
```

3. Set multiple items in a dictionary

```python
>>> client.dictionary_multi_set(cache_name="my-cache", dictionary_name="my-dictionary", dictionary={"key2": "value2", "key3": "value3"}, refresh_ttl=False)
CacheDictionarySetResponse(dictionary_name='my-dictionary', dictionary={b'key2': b'value2', b'key3': b'value3'})
```

4. Get one value from a dictionary

```python
>>> get_response = client.dictionary_get("my-cache", "my-dictionary", "key1")
>>> get_response
CacheDictionaryGetUnaryResponse(value=b'value1', result=<CacheGetStatus.HIT: 1>)

>>> get_response.status()
<CacheGetStatus.HIT: 1>

>>> get_response.value()
'value2'
```

5. Get multiple values from a dictionary

```python
>>> get_response = client.dictionary_get("my-cache", "my-dictionary", "key1", "key2", "key7")
>>> get_response
CacheDictionaryGetMultiResponse(values=[b'value1', b'value2', None], results=[<CacheGetStatus.HIT: 1>, <CacheGetStatus.HIT: 1>, <CacheGetStatus.MISS: 2>])

>>> get_response.to_list()
[CacheDictionaryGetUnaryResponse(value=b'value1', result=<CacheGetStatus.HIT: 1>),
 CacheDictionaryGetUnaryResponse(value=b'value2', result=<CacheGetStatus.HIT: 1>),
 CacheDictionaryGetUnaryResponse(value=None, result=<CacheGetStatus.MISS: 2>)]

 >>> get_response.status()
 [<CacheGetStatus.HIT: 1>, <CacheGetStatus.HIT: 1>, <CacheGetStatus.MISS: 2>]

 >>> get_response.values()
['value1', 'value2', None]
```

6. Get the entire dictionary

```python
>>> dictionary_get_all_response = client.dictionary_get_all(cache_name="my-cache", dictionary_name="my-dictionary")
>>> dictionary_get_all_response
CacheDictionaryGetAllResponse(value={b'key1': b'value1', b'key2': b'value2'}, result=<CacheGetStatus.HIT: 1>)

>>> dictionary_get_all_response.value()
{'key1': 'value1', 'key2': 'value2'}

>>> dictionary_get_all_response.value_as_bytes()
{b'key1': b'value1', b'key2': b'value2'}
```

7. Interact with a returned dictionary

```python
>>> my_dictionary = dictionary_get_all_response.value()
>>> my_dictionary["key1"]
'value1'
```
