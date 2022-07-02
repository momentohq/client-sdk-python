# Incubating

The `momento.incubating` package has work-in-progress features that may or may be be in the final version.

## Setup

Initialize the cache:

```python
>>> import momento.incubating.simple_cache_client as scc
>>> client = scc.init(auth_token=AUTH_TOKEN, item_default_ttl_seconds=DEFAULT_TTL)
```

## Dictionary Methods

This demonstrates the methods and response types for a dictionary data type in the cache.

1. Set one item in a dictionary

```python
>>> client.dictionary_set(cache_name="my-cache", dictionary_name="my-dictionary", key="key1", value="value1", refresh_ttl=False)
CacheDictionarySetUnaryResponse(dictionary_name='my-dictionary', key=b'key1', value=b'value1')
```

2. Set multiple items in a dictionary

```python
>>> client.dictionary_set_multi(cache_name="my-cache", dictionary_name="my-dictionary", dictionary={"key2": "value2", "key3": "value3"}, refresh_ttl=False)
CacheDictionarySetMultiResponse(dictionary_name='my-dictionary', dictionary={b'key2': b'value2', b'key3': b'value3'})
```

3. Get one value from a dictionary

```python
>>> get_response = client.dictionary_get(cache_name="my-cache", dictionary_name="my-dictionary", key="key1")
>>> get_response
CacheDictionaryGetUnaryResponse(value=b'value1', status=<CacheGetStatus.HIT: 1>)

>>> get_response.status()
<CacheGetStatus.HIT: 1>

>>> get_response.value()
'value2'
```

4. Get multiple values from a dictionary

```python
>>> get_response = client.dictionary_get_multi("my-cache", "my-dictionary", "key1", "key2", "key7")
>>> get_response
CacheDictionaryGetMultiResponse(values=[b'value1', b'value2', None], status=[<CacheGetStatus.HIT: 1>, <CacheGetStatus.HIT: 1>, <CacheGetStatus.MISS: 2>])

>>> get_response.to_list()
[CacheDictionaryGetUnaryResponse(value=b'value1', status=<CacheGetStatus.HIT: 1>),
 CacheDictionaryGetUnaryResponse(value=b'value2', status=<CacheGetStatus.HIT: 1>),
 CacheDictionaryGetUnaryResponse(value=None, status=<CacheGetStatus.MISS: 2>)]

 >>> get_response.status()
 [<CacheGetStatus.HIT: 1>, <CacheGetStatus.HIT: 1>, <CacheGetStatus.MISS: 2>]

 >>> get_response.values()
['value1', 'value2', None]
```

5. Get the entire dictionary

```python
>>> dictionary_get_all_response = client.dictionary_get_all(cache_name="my-cache", dictionary_name="my-dictionary")
>>> dictionary_get_all_response
CacheDictionaryGetAllResponse(value={b'key1': b'value1', b'key2': b'value2'}, status=<CacheGetStatus.HIT: 1>)

>>> dictionary_get_all_response.value()
{'key1': 'value1', 'key2': 'value2'}

>>> dictionary_get_all_response.value_as_bytes()
{b'key1': b'value1', b'key2': b'value2'}
```

6. Interact with a returned dictionary

```python
>>> my_dictionary = dictionary_get_all_response.value()
>>> my_dictionary["key1"]
'value1'
```

## Exists

This demonstrates the methods and response features for testing the existence of key(s).

1. Test a single key

```python
>>> if client.exists("my-cache", "key"):
... 	# Key exists
... else:
... 	# Handle missing keys
```

2. Test multiple keys

```python
>>> response = client.exists("my-cache", *keys)
>>> if not response.all():
... 	# At least one key is missing
```

3. Find number of keys that exist

```python
>>> response = client.exists("my-cache", *keys)
>>> response.num_exists()
```

4. Find keys that did not exist

```python
>>> response = client.exists("my-cache", *keys)
>>> response.missing_keys()
```

5. Find keys that exist

```python
>>> response = client.exists("my-cache", *keys)
>>> response.present_keys()
```

6. Iterate over all keys

```python
>>> for key, does_exist in client.exists("my-cache", *keys).zip_keys_and_results():
... 	print(f"{=key} {=does_exist}")
```

7. A full example

```python
>>> response = client.exists("my-cache", *keys)
>>> if not response.all():
... 	for key in response.missing_keys():
...			# handle missing keys
```

