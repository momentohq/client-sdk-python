import typing

# https://stackoverflow.com/questions/71889556/mypy-checking-typing-protocol-with-python-3-7-support
if typing.TYPE_CHECKING:
    from typing_extensions import Protocol
else:
    Protocol = object

from momento.errors import SdkException, MomentoErrorCode


class AbstractClassMixin:
    """Mixin that simulates an abstract type. Disallows instantiating an object."""

    def __new__(cls, *args, **kwargs):  # type: ignore
        raise NotImplementedError("This is an abstract class. Cannot instantiate!")


class HasValueBytesProtocol(Protocol):
    @property
    def value_bytes(self) -> bytes:
        ...


class ValueStringMixin:
    """Renders `value_bytes` as a utf-8 string.

    Returns:
        str: the utf-8 encoding of the data
    """

    @property
    def value_string(self: HasValueBytesProtocol) -> str:
        return self.value_bytes.decode("utf-8")


class HasErrorProtocol(Protocol):
    @property
    def _error(self) -> SdkException:
        ...


class ErrorResponseMixin:
    @property
    def inner_exception(self: HasErrorProtocol) -> SdkException:
        """The SdkException object used to construct the response."""
        return self._error

    @property
    def error_code(self: HasErrorProtocol) -> MomentoErrorCode:
        """The `MomentoErrorCode` value for the particular error object."""
        return self._error.error_code

    @property
    def message(self: HasErrorProtocol) -> str:
        """An explanation of conditions that caused and potential ways to resolve the error."""
        return f"{self._error.message_wrapper}: {self._error.message}"