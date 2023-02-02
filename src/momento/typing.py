from typing import Dict, List, Mapping, Sequence, Union

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
TDictionaryFields = Sequence[TDictionaryField]
TDictionaryItems = Mapping[TDictionaryField, TDictionaryValue]
TDictionaryBytesBytes = Dict[bytes, bytes]
TDictionaryStrBytes = Dict[str, bytes]
TDictionaryStrStr = Dict[str, str]

# List Types
TListName = TCollectionName
TListValuesBytes = List[bytes]
TListValuesStr = List[str]
TListValues = Union[TListValuesBytes, TListValuesStr]

# Set Types
