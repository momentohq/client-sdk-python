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

        message = ", ".join(f"{k}={self._truncate_value(v)!r}" for k, v in attributes.items())

        return f"{class_name}({message})"

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

    @no_type_check
    def __eq__(self, other: object) -> bool:
        if other is None:
            return False

        if type(self) != type(other):
            return False

        return vars(self) == vars(other)

    @no_type_check
    def __hash__(self) -> int:
        state = [type(self)]
        state.extend(sorted(vars(self).items(), key=lambda x: x[0]))
        return hash(tuple(state))


class CacheResponse(Response):
    ...


class ControlResponse(Response):
    ...
