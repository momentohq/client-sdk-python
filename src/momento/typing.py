from typing import Dict, Iterable, List, Mapping, Union

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
TDictionaryItems = Mapping[TDictionaryField, TDictionaryValue]
TDictionaryBytesBytes = Dict[bytes, bytes]
TDictionaryBytesStr = Dict[bytes, str]
TDictionaryStrBytes = Dict[str, bytes]
TDictionaryStrStr = Dict[str, str]

# List Types
TListName = TCollectionName
TListValuesBytes = List[bytes]
TListValuesStr = List[str]
TListValues = Union[TListValuesBytes, TListValuesStr]

# Set Types
