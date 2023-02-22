import typing

# https://stackoverflow.com/questions/71889556/mypy-checking-typing-protocol-with-python-3-7-support
if typing.TYPE_CHECKING:
    from typing_extensions import Protocol
else:
    Protocol = object

from momento.errors import MomentoErrorCode, SdkException


class ValueStringMixin:
    """Renders `value_bytes` as a utf-8 string.

    Returns:
        str: the utf-8 encoding of the data
    """

    value_bytes: bytes

    @property
    def value_string(self) -> str:
        """Convert the bytes `value` to a UTF-8 string.

        Returns:
            str: UTF-8 representation of the `value`
        """
        return self.value_bytes.decode("utf-8")


class ErrorResponseMixin:
    _error: SdkException

    def __init__(self, _error: SdkException):
        self._error = _error

    @property
    def inner_exception(self) -> SdkException:
        """The SdkException object used to construct the response."""
        return self._error

    @property
    def error_code(self) -> MomentoErrorCode:
        """The `MomentoErrorCode` value for the particular error object."""
        return self._error.error_code

    @property
    def message(self) -> str:
        """An explanation of conditions that caused and potential ways to resolve the error."""
        return f"{self._error.message_wrapper}: {self._error.message}"

    def __str__(self) -> str:
        return f"{self.__class__.__qualname__} {self.error_code}: {self.message}"
