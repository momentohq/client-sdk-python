from typing import Iterable, List, Mapping, Union

TCacheName = str

# Scalar Types
TScalarKey = Union[str, bytes]
TScalarValue = Union[str, bytes]

# Collections
TCollectionName = str

# Dictionary Types
TDictionaryName = TCollectionName
TDictionaryField = Union[str, bytes]
TDictionaryValue = Union[str, bytes]
TDictionaryFields = Iterable[TDictionaryField]
TDictionaryItems = Union[
    Mapping[TDictionaryField, TDictionaryValue],
    # Mapping[Union[bytes, str], Union[bytes, str]] doesn't accept Mapping[str, str],
    # So we need to add those types here too[]
    Mapping[bytes, bytes],
    Mapping[bytes, str],
    Mapping[str, bytes],
    Mapping[str, str],
]

# List Types
TListName = TCollectionName
TListValue = Union[str, bytes]
TListValuesInputBytes = Iterable[bytes]
TListValuesInputStr = Iterable[str]
TListValuesInput = Iterable[TListValue]
TListValuesOutputBytes = List[bytes]
TListValuesOutputStr = List[str]
TLIstValuesOutput = Union[TListValuesOutputBytes, TListValuesOutputStr]

# Set Types
