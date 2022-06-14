from typing import cast, Dict, List, Optional, Union

from ..cache_operation_types import CacheGetStatus
from ._utilities._serialization import _bytes_to_string, _bytes_dict_to_string_dict


# Dictionary related responses
DictionaryKey = Union[str, bytes]
DictionaryValue = Union[str, bytes]
StringDictionary = Dict[str, str]
BytesDictionary = Dict[bytes, bytes]
Dictionary = Union[StringDictionary, BytesDictionary]


class CacheDictionaryGetUnaryResponse:
    def __init__(self, value: Optional[DictionaryValue], result: CacheGetStatus):
        self._value = value
        self._result = result

    def value(self) -> Optional[str]:
        if self.status() == CacheGetStatus.MISS:
            return None
        return _bytes_to_string(cast(bytes, self._value))

    def value_as_bytes(self) -> Optional[bytes]:
        if self.status() == CacheGetStatus.MISS:
            return None
        return cast(bytes, self._value)

    def status(self) -> CacheGetStatus:
        return self._result

    def __eq__(self, other: object) -> bool:
        return (
            isinstance(other, CacheDictionaryGetUnaryResponse)
            and self._value == other._value
            and self._result == other._result
        )

    def __str__(self) -> str:
        return self.__repr__()

    def __repr__(self) -> str:
        return f"CacheDictionaryGetUnaryResponse(value={self._value!r}, result={self._result!r})"


class CacheDictionaryGetMultiResponse:
    def __init__(
        self, values: List[Optional[DictionaryValue]], results: List[CacheGetStatus]
    ):
        self._values = values
        self._results = results

    def to_list(self) -> List[CacheDictionaryGetUnaryResponse]:
        return [
            CacheDictionaryGetUnaryResponse(value, result)
            for value, result in zip(self._values, self._results)
        ]

    def values(self) -> List[Optional[str]]:
        return [
            _bytes_to_string(cast(bytes, value))
            if result == CacheGetStatus.HIT
            else None
            for value, result in zip(self._values, self._results)
        ]

    def values_as_bytes(self) -> List[Optional[bytes]]:
        return [
            cast(bytes, value) if result == CacheGetStatus.HIT else None
            for value, result in zip(self._values, self._results)
        ]

    def status(self) -> List[CacheGetStatus]:
        return self._results

    def __str__(self) -> str:
        return self.__repr__()

    def __repr__(self) -> str:
        return f"CacheDictionaryGetMultiResponse(values={self._values!r}, results={self._results!r})"


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
    def __init__(self, value: Optional[BytesDictionary], result: CacheGetStatus):
        self._value = value
        self._result = result

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
        return self._result

    def __str__(self) -> str:
        return self.__repr__()

    def __repr__(self) -> str:
        return f"CacheDictionaryGetAllResponse(value={self._value!r}, result={self._result!r})"
