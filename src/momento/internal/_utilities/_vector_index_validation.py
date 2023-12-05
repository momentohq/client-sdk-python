from __future__ import annotations

from momento.errors import InvalidArgumentException
from momento.internal.services import Service

from ._data_validation import _validate_name


def _validate_index_name(cache_name: str) -> None:
    _validate_name(cache_name, "Vector index name", Service.INDEX)


def _validate_num_dimensions(num_dimensions: int) -> None:
    if num_dimensions < 1 or not isinstance(num_dimensions, int):
        raise InvalidArgumentException("Number of dimensions must be a positive integer.", Service.INDEX)


def _validate_top_k(top_k: int) -> None:
    if top_k < 1 or not isinstance(top_k, int):
        raise InvalidArgumentException("Top k must be a positive integer.", Service.INDEX)
