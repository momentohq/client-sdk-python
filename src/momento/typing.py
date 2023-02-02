from typing import Dict, Iterable, Iterator, List, Mapping, Union

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
TListValuesInputBytes = Iterator[bytes]
TListValuesInputStr = Iterator[str]
TListValuesInput = Union[TListValuesInputBytes, TListValuesInputStr]
TListValuesOutputBytes = List[bytes]
TListValuesOutputStr = List[str]
TLIstValuesOutput = Union[TListValuesOutputBytes, TListValuesOutputStr]

# Set Types
