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
TListValues = Union[List[str], List[bytes]]

# Set Types
