from typing import List, Union

TCacheName = str

# Scalar Types
TScalarKey = Union[str, bytes]
TScalarValue = Union[str, bytes]

# Collections
TCollectionName = str

# Dictionary Types

# List Types
TListName = TCollectionName
TListValuesBytes = List[bytes]
TListValuesStr = List[str]
TListValues = Union[TListValuesBytes, TListValuesStr]

# Set Types
