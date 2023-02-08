from __future__ import annotations

from abc import ABC
from typing import Any, no_type_check


class Response(ABC):
    """Parent of all responses"""

    # We need to work with Any.
    @no_type_check
    def __repr__(self) -> str:
        class_name = self.__class__.__qualname__
        attributes = vars(self)

        if not attributes:
            return f"{class_name}()"

        message = {k: self._truncate_value(v) for k, v in attributes.items()}

        return f"{class_name}({message!r})"

    @no_type_check
    def _truncate_value(self, value: Any, max_length: int = 32) -> Any:
        if type(value) == bytes:
            if len(value) < max_length:
                return value

            return value[:max_length] + b"..."
        elif type(value) == str:
            if len(value) < max_length:
                return value

            return value[:max_length] + "..."
        else:
            return value


class CacheResponse(Response):
    ...


class ControlResponse(Response):
    ...
