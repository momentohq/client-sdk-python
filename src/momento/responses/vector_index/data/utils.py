from __future__ import annotations

from typing import Iterable

from momento_wire_types import vectorindex_pb2 as pb

from momento.common_data.vector_index.item import Metadata
from momento.errors import UnknownException


def pb_metadata_to_dict(
    pb_metadata: Iterable[pb._Metadata],
) -> Metadata:
    metadata: Metadata = {}
    for item in pb_metadata:
        type = item.WhichOneof("value")
        field = item.field
        if type == "string_value":
            metadata[field] = item.string_value
        elif type == "integer_value":
            metadata[field] = item.integer_value
        elif type == "double_value":
            metadata[field] = item.double_value
        elif type == "boolean_value":
            metadata[field] = item.boolean_value
        elif type == "list_of_strings_value":
            metadata[field] = list(item.list_of_strings_value.values)
        else:
            raise UnknownException(f"Unknown metadata value: {type}")
    return metadata
