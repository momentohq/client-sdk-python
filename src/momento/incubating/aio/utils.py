import pickle
from typing import cast

from ..cache_operation_types import CacheDictionaryValue, Dictionary, StoredDictionary


def convert_dict_values_to_bytes(dict_: Dictionary) -> Dictionary:
    return {k: v if isinstance(v, bytes) else v.encode() for k, v in dict_.items()}


def dict_to_stored_hash(dict_: Dictionary) -> StoredDictionary:
    return {k: CacheDictionaryValue(v) for k, v in dict_.items()}


def deserialize_stored_hash(pickled_dict: bytes) -> StoredDictionary:
    d = cast(Dictionary, pickle.loads(pickled_dict))
    return dict_to_stored_hash(d)
