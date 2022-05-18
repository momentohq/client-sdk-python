import pickle
from typing import cast

from ..cache_operation_types import CacheDictionaryValue, Dictionary, StoredDictionary
from ..._utilities._data_validation import _as_bytes


def convert_dict_values_to_bytes(dict_: Dictionary) -> Dictionary:
    return {_as_bytes(k, "Unsupported type for key: "): _as_bytes(v, "Unsupported type for value: ") for k, v in dict_.items()}


def dict_to_stored_hash(dict_: Dictionary) -> StoredDictionary:
    return {k: CacheDictionaryValue(v) for k, v in dict_.items()}


def deserialize_stored_hash(pickled_dict: bytes) -> StoredDictionary:
    d = cast(Dictionary, pickle.loads(pickled_dict))
    return dict_to_stored_hash(d)
