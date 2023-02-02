from typing import List, Mapping, Sequence, Union

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

# List Types
TListName = TCollectionName
TListValuesBytes = List[bytes]
TListValuesStr = List[str]
TListValues = Union[TListValuesBytes, TListValuesStr]

# Set Types
