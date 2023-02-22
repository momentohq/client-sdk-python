from __future__ import annotations

from abc import ABC
from typing import Any, no_type_check


class Response(ABC):
    """Parent of all responses."""

    # We need to work with Any.
    @no_type_check
    def _render_as_str(self, max_value_length: int = 32, max_collection_length: int = 5) -> str:
        class_name = self.__class__.__qualname__
        attributes = vars(self)

        if not attributes:
            return f"{class_name}()"

        message_parts = []
        for attribute, value in attributes.items():
            appended = False

            # To truncate a collection, we must render it as a string all at once to account for the ellipsis
            if type(value) == list:
                if len(value) > max_collection_length:
                    message_parts.append(
                        Response._display_list(attribute, value, max_value_length, max_collection_length)
                    )
                    appended = True
                else:
                    value = [Response._truncate_value(v_i, max_value_length) for v_i in value]
            elif type(value) == set:
                if len(value) > max_collection_length:
                    message_parts.append(
                        Response._display_set(attribute, value, max_value_length, max_collection_length)
                    )
                    appended = True
                else:
                    value = {Response._truncate_value(v_i, max_value_length) for v_i in value}
            elif type(value) == dict:
                if len(value) > max_collection_length:
                    message_parts.append(
                        Response._display_dict(attribute, value, max_value_length, max_collection_length)
                    )
                    appended = True
                else:
                    value = {
                        Response._truncate_value(k_i, max_value_length): Response._truncate_value(v_i, max_value_length)
                        for k_i, v_i in value.items()
                    }
            else:
                value = Response._truncate_value(value, max_value_length)

            # appended == False <==> either the value was not a collection or it did not exceed the max length
            if not appended:
                message_parts.append(f"{attribute}={value!r}")

        message = ", ".join(message_parts)

        return f"{class_name}({message})"

    @staticmethod
    @no_type_check
    def _display_list(attribute: object, list_: list[bytes], max_value_length: int, max_collection_length: int) -> str:
        list_ = list_[:max_collection_length]
        return f"{attribute}=[{', '.join(str(Response._truncate_value(list_i, max_value_length)) for list_i in list_)}, ...]"  # noqa: E501

    @staticmethod
    @no_type_check
    def _display_set(attribute: object, set_: set[bytes], max_value_length: int, max_collection_length: int) -> str:
        set_ = list(set_)[:max_collection_length]
        return f"{attribute}={{{', '.join(str(Response._truncate_value(set_i, max_value_length)) for set_i in set_)}, ...}}"  # noqa: E501

    @staticmethod
    @no_type_check
    def _display_dict(
        attribute: object, dict_: dict[bytes, bytes], max_value_length: int, max_collection_length: int
    ) -> str:
        dict_ = dict(list(dict_.items())[:max_collection_length])
        return f"{attribute}={{{', '.join(str(Response._truncate_value(k_i, max_value_length)) + ': ' + str(Response._truncate_value(v_i, max_value_length)) for k_i, v_i in dict_.items())}, ...}}"  # noqa: E501

    @no_type_check
    def __repr__(self) -> str:
        return self._render_as_str(max_value_length=512, max_collection_length=1024)

    @no_type_check
    def __str__(self) -> str:
        return self._render_as_str()

    @staticmethod
    @no_type_check
    def _truncate_value(value: Any, max_length: int = 32) -> Any:
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
