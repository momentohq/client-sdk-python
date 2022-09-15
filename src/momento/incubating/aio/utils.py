import pickle
from typing import cast

from ..._utilities._data_validation import _as_bytes
from ..cache_operation_types import BytesDictionary, Dictionary


def convert_dict_items_to_bytes(dictionary: Dictionary) -> BytesDictionary:
    return {
        _as_bytes(k, "Unsupported type for key: "): _as_bytes(v, "Unsupported type for value: ")
        for k, v in dictionary.items()
    }


def deserialize_dictionary(pickled_dictionary: bytes) -> BytesDictionary:
    return cast(BytesDictionary, pickle.loads(pickled_dictionary))


def serialize_dictionary(dictionary: BytesDictionary) -> bytes:
    return pickle.dumps(dictionary)
