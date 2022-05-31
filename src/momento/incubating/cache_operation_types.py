from typing import cast, Dict, Optional, Union

from ..cache_operation_types import CacheGetStatus


# Dictionary related responses
DictionaryKey = Union[str, bytes]
DictionaryValue = Union[str, bytes]
StringDictionary = Dict[str, str]
BytesDictionary = Dict[bytes, bytes]
Dictionary = Union[StringDictionary, BytesDictionary]


class CacheDictionaryGetResponse:
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

    def __str__(self) -> str:
        return self.__repr__()

    def __repr__(self) -> str:
        value = self._value
        try:
            value = self.value()
        except UnicodeDecodeError:
            value = str(value)

        return f"CacheDictionaryGetResponse(value={value!r}, result={self._result!r})"


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
