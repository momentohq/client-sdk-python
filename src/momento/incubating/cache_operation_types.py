from typing import cast, Dict, Iterable, List, Optional, Tuple, Union

from ..cache_operation_types import CacheGetStatus
from ._utilities._serialization import _bytes_to_string, _bytes_dict_to_string_dict


# Dictionary related responses
DictionaryKey = Union[str, bytes]
DictionaryValue = Union[str, bytes]
StringDictionary = Dict[str, str]
BytesDictionary = Dict[bytes, bytes]
Dictionary = Union[StringDictionary, BytesDictionary]


class CacheDictionaryGetUnaryResponse:
    def __init__(self, value: Optional[DictionaryValue], status: CacheGetStatus):
        self._value = value
        self._status = status

    def value(self) -> Optional[str]:
        if self.status() == CacheGetStatus.MISS:
            return None
        return _bytes_to_string(cast(bytes, self._value))

    def value_as_bytes(self) -> Optional[bytes]:
        if self.status() == CacheGetStatus.MISS:
            return None
        return cast(bytes, self._value)

    def status(self) -> CacheGetStatus:
        return self._status

    def __eq__(self, other: object) -> bool:
        return (
            isinstance(other, CacheDictionaryGetUnaryResponse)
            and self._value == other._value
            and self._status == other._status
        )

    def __str__(self) -> str:
        return self.__repr__()

    def __repr__(self) -> str:
        return f"CacheDictionaryGetUnaryResponse(value={self._value!r}, status={self._status!r})"


class CacheDictionaryGetMultiResponse:
    def __init__(
        self, values: List[Optional[DictionaryValue]], status: List[CacheGetStatus]
    ):
        self._values = values
        self._status = status

    def to_list(self) -> List[CacheDictionaryGetUnaryResponse]:
        return [
            CacheDictionaryGetUnaryResponse(value, status)
            for value, status in zip(self._values, self._status)
        ]

    def values(self) -> List[Optional[str]]:
        return [
            _bytes_to_string(cast(bytes, value))
            if status == CacheGetStatus.HIT
            else None
            for value, status in zip(self._values, self._status)
        ]

    def values_as_bytes(self) -> List[Optional[bytes]]:
        return [
            cast(bytes, value) if status == CacheGetStatus.HIT else None
            for value, status in zip(self._values, self._status)
        ]

    def status(self) -> List[CacheGetStatus]:
        return self._status

    def __str__(self) -> str:
        return self.__repr__()

    def __repr__(self) -> str:
        return f"CacheDictionaryGetMultiResponse(values={self._values!r}, status={self._status!r})"


class CacheDictionarySetUnaryResponse:
    def __init__(self, dictionary_name: str, key: bytes, value: bytes):
        self._dictionary_name = dictionary_name
        self._key = key
        self._value = value

    def dictionary_name(self) -> str:
        return self._dictionary_name

    def key(self) -> str:
        return _bytes_to_string(self._key)

    def key_as_bytes(self) -> bytes:
        return self._key

    def value(self) -> str:
        return _bytes_to_string(self._value)

    def value_as_bytes(self) -> bytes:
        return self._value

    def __str__(self) -> str:
        return self.__repr__()

    def __repr__(self) -> str:
        return f"CacheDictionarySetUnaryResponse(dictionary_name={self._dictionary_name!r}, key={self._key!r}, value={self._value!r})"


class CacheDictionarySetMultiResponse:
    def __init__(self, dictionary_name: str, dictionary: BytesDictionary):
        self._dictionary_name = dictionary_name
        self._dictionary = dictionary

    def dictionary_name(self) -> str:
        return self._dictionary_name

    def dictionary(self) -> StringDictionary:
        return _bytes_dict_to_string_dict(self._dictionary)

    def dictionary_as_bytes(self) -> BytesDictionary:
        return self._dictionary

    def __str__(self) -> str:
        return self.__repr__()

    def __repr__(self) -> str:
        return f"CacheDictionarySetMultiResponse(dictionary_name={self._dictionary_name!r}, dictionary={self._dictionary!r})"


class CacheDictionaryGetAllResponse:
    def __init__(self, value: Optional[BytesDictionary], status: CacheGetStatus):
        self._value = value
        self._status = status

    def value(self) -> Optional[StringDictionary]:
        """Get the dictionary as stored in the cache.

        Returns:
            Optional[StringDictionary]: The dictionary if the cache get was a hit, else None.
        """
        if self.status() != CacheGetStatus.HIT:
            return None

        return _bytes_dict_to_string_dict(cast(BytesDictionary, self._value))

    def value_as_bytes(self) -> Optional[BytesDictionary]:
        """Get the dictionary as stored in the cache.

        Returns:
            Optional[BytesDictionary]: The dictionary if the cache get was a hit, else None.
        """
        if self.status() != CacheGetStatus.HIT:
            return None

        return self._value

    def status(self) -> CacheGetStatus:
        return self._status

    def __str__(self) -> str:
        return self.__repr__()

    def __repr__(self) -> str:
        return f"CacheDictionaryGetAllResponse(value={self._value!r}, status={self._status!r})"


class CacheExistsResponse:
    def __init__(self, keys: Tuple[Union[str, bytes], ...], results: List[bool]):
        self._keys = keys
        self._results = results

    def all(self) -> bool:
        """Test if all the keys exist.

        Examples:
        >>> if not client.exists("my-cache", "key1", "key2", "key3").all():
        ...     log.info("Some key(s) are missing.")
        >>> response = client.exists("my-cache", *keys)
        >>> if response.all()
        ...     log.info("All the keys exist")

        Returns:
            bool: True if all the keys exist. False is at least one does not.
        """
        return all(self._results)

    def num_exists(self) -> int:
        """Count the number of keys that exist.

        Example:
        >>> response = client.exists("my-cache", *keys))
        >>> if response.num_exists() != len(keys):
        ...     log.info("Missing some keys")

        Returns:
            int: The number of keys that exist.
        """
        return sum(self._results)

    def results(self) -> List[bool]:
        """Get a boolean array where each element indicates if the
        corresponding input key exists.

        Example:
        >>> repsonse = client.exists("my-cache", *keys)
        >>> if not response.all():
        ...     mask = response.results()
        ...     missing = [key for key, does_exist in zip(keys, mask) if not does_exist]
        ...     log.info(f"The following keys are missing: {missing}")

        Note the above example illustrates using the existence mask.
        Alternatively one could invoke `missing_keys` to get the missing keys.

        Returns:
            List[bool]: The existence mask.
        """
        return self._results

    def missing_keys(self) -> List[Union[str, bytes]]:
        """List the keys that do not exist.

        Returns:
            List[Union[str, bytes]]: List of queried keys that do not exist.
        """
        return [
            key for key, does_exist in zip(self._keys, self._results) if not does_exist
        ]

    def present_keys(self) -> List[Union[str, bytes]]:
        """List the keys that exist.

        Returns:
            List[Union[str, bytes]]: List of queried keys that exist.
        """
        return [key for key, does_exist in zip(self._keys, self._results) if does_exist]

    def zip_keys_and_results(self) -> Iterable[Tuple[Union[str, bytes], bool]]:
        """Zip the keys and existence mask.

        Example:
        >>> for key, status in client.exists("my-cache", *keys).zip_keys_and_results():
        ...     print(f"{=key} {=status}")

        Returns:
            Iterable[Tuple[Union[str, bytes], bool]]: An iterator of tuples `(key, exist)`
                where `exist` indicates whether the key exists or not.
        """
        return zip(self._keys, self._results)

    def __bool__(self) -> bool:
        """Test if all the keys exist.

        This is helpful for using the response object in an if-expression:

        >>> if client.exists("my-cache", "key"):
        ...     # Business logic

        Returns:
            bool: True if all the keys exist else False
        """
        return self.all()

    def __str__(self) -> str:
        return self.__repr__()

    def __repr__(self) -> str:
        return f"CacheExistsResponse(keys={self._keys!r}, results={self._results!r})"
