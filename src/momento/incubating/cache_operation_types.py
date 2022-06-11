from typing import cast, Dict, List, Optional, Union

from ..cache_operation_types import CacheGetStatus


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
        return cast(bytes, self._value).decode("utf-8")

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
        value = self._value
        try:
            value = self.value()
        except UnicodeDecodeError:
            value = str(value)

        return (
            f"CacheDictionaryGetUnaryResponse(value={value!r}, result={self._result!r})"
        )


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
            cast(bytes, value).decode("utf-8") if result == CacheGetStatus.HIT else None
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
        values = self._values
        try:
            values = self.values()  # type: ignore
        except UnicodeDecodeError:
            values = [str(v) for v in values]

        return f"CacheDictionaryGetMultiResponse(values={values!r}, results={self._results!r})"


class CacheDictionarySetResponse:
    def __init__(self, key: bytes, value: Dictionary):
        self._key = key
        self._value = value

    def value(self) -> Dictionary:
        return self._value

    def key(self) -> str:
        """Decodes key of item set in cache to a utf-8 string."""
        return self._key.decode("utf-8")

    def key_as_bytes(self) -> bytes:
        """Returns key of item stored in cache as bytes."""
        return self._key

    def __str__(self) -> str:
        return self.__repr__()

    def __repr__(self) -> str:
        try:
            key = self.key()
        except UnicodeDecodeError:
            key = self.key_as_bytes()  # type: ignore

        return f"CacheDictionarySetResponse(key={key!r}, value={self._value!r})"


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

        return {
            k.decode("utf-8"): v.decode("utf-8")
            for k, v in cast(BytesDictionary, self._value).items()
        }

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
