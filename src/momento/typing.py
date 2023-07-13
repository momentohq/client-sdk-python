from __future__ import annotations

from typing import Iterable, List, Mapping, Set, Union

TCacheName = str
TTopicName = str

TMomentoValue = Union[str, bytes]

# Scalar Types
TScalarKey = Union[str, bytes]
TScalarValue = TMomentoValue

# Collections
TCollectionName = str
TCollectionValue = Union[str, bytes]

# Dictionary Types
TDictionaryName = TCollectionName
TDictionaryField = Union[str, bytes]
TDictionaryValue = TMomentoValue
TDictionaryFields = Iterable[TDictionaryField]
TDictionaryItems = Union[
    Mapping[TDictionaryField, TDictionaryValue],
    # Mapping isn't covariant so we have to list out the types here
    Mapping[bytes, bytes],
    Mapping[bytes, str],
    Mapping[str, bytes],
    Mapping[str, str],
]

# List Types
TListName = TCollectionName
TListValue = TMomentoValue
TListValuesInputBytes = Iterable[bytes]
TListValuesInputStr = Iterable[str]
TListValuesInput = Iterable[TListValue]
TListValuesOutputBytes = List[bytes]
TListValuesOutputStr = List[str]
TLIstValuesOutput = Union[TListValuesOutputBytes, TListValuesOutputStr]

# Set Types
TSetName = TCollectionName
TSetElement = TMomentoValue
TSetElementsInputBytes = Iterable[bytes]
TSetElementsInputStr = Iterable[str]
TSetElementsInput = Iterable[TSetElement]
TSetElementsOutputStr = Set[str]
TSetElementsOutputBytes = Set[bytes]
TSetElementsOutput = Union[TSetElementsOutputBytes, TSetElementsOutputStr]

# Sorted Set Types
TSortedSetName = TCollectionName
TSortedSetValue = TMomentoValue
TSortedSetScore = float
TSortedSetValues = Iterable[TSortedSetValue]
TSortedSetElements = Union[
    Mapping[TSortedSetValue, TSortedSetScore],
    # Mapping isn't covariant, so we have to list out the types here
    Mapping[bytes, float],
    Mapping[str, float],
]
