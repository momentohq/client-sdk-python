from __future__ import annotations

import collections.abc
from datetime import timedelta
from typing import Iterable, Optional, Tuple

from momento.errors import InvalidArgumentException

from ._data_validation import _validate_name


def _validate_index_name(cache_name: str) -> None:
    _validate_name(cache_name, "Index name")


def _validate_num_dimensions(num_dimensions: int) -> None:
    if num_dimensions < 1:
        raise InvalidArgumentException(f"num_dimensions must be a positive number.")
