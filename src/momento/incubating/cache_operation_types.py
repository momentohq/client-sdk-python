from typing import cast, Dict, Optional, Union

from ..cache_operation_types import CacheGetStatus


# Dictionary related responses
DictionaryKey = Union[str, bytes]
DictionaryValue = Union[str, bytes]
Dictionary = Dict[DictionaryKey, DictionaryValue]


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

        return f"CacheDictionaryGetResponse(value={value}, result={self._result})"


class CacheDictionaryValue:
    def __init__(self, value: DictionaryValue) -> None:
        self._value = value

    def value(self) -> str:
        return cast(bytes, self._value).decode("utf-8")

    def value_as_bytes(self) -> bytes:
        return cast(bytes, self._value)

    def __eq__(self, other: object) -> bool:
        return type(other) == type(self) and self._value == other._value  # type: ignore

    def __str__(self) -> str:
        return self.__repr__()

    def __repr__(self) -> str:
        value = self._value
        try:
            value = self.value()
        except UnicodeDecodeError:
            pass

        return f"CacheDictionaryValue(value={value})"  # type: ignore


# Represents the type of a dictionary as stored in the cache.
# This is the type returned by dictionary_get_all and
# dictionary_set.
StoredDictionary = Dict[DictionaryKey, CacheDictionaryValue]


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

        return f"CacheDictionarySetResponse(key={key}, value={self._value})"


class CacheDictionaryGetAllResponse:
    def __init__(self, value: Optional[StoredDictionary], result: CacheGetStatus):
        self._value = value
        self._result = result

    def value(self, *, keys_as_bytes: bool = False) -> Optional[StoredDictionary]:
        """Get the dictionary as stored in the cache.

        By default unmarshals the keys to strings. To override this
        set `keys_as_bytes` to `True`.

        Values are of type `CacheDictionaryValue`, which implement `value` and
        `value_as_bytes` methods to unmarshal to string or bytes respectively.

        Args:
            keys_as_bytes (bool, optional): Leave the keys as uninterpreted bytes. Defaults to False.

        Returns:
            Optional[StoredDictionary]: The dictionary if the cache get was a hit, else None.
        """
        if self.status() != CacheGetStatus.HIT:
            return None

        if keys_as_bytes:
            return self._value

        return {cast(bytes, k).decode("utf-8"): v for k, v in self._value.items()}

    def status(self) -> CacheGetStatus:
        return self._result

    def __str__(self) -> str:
        return self.__repr__()

    def __repr__(self) -> str:
        return (
            f"CacheDictionaryGetAllResponse(value={self._value}, result={self._result})"
        )
