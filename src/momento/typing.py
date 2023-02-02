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
TListValue = Union[str, bytes]
TListValuesInputBytes = Iterable[bytes]
TListValuesInputStr = Iterable[str]
TListValuesInput = Iterable[TListValue]
TListValuesOutputBytes = List[bytes]
TListValuesOutputStr = List[str]
TLIstValuesOutput = Union[TListValuesOutputBytes, TListValuesOutputStr]

# Set Types
