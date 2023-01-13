from typing import Optional

from momento.errors import MomentoErrorCode, MomentoErrorTransportDetails


class SdkException(Exception):
    """Base class for all Momento client exceptions"""

    message: str
    """Exception message"""
    error_code: MomentoErrorCode
    """Enumeration of all possible Momento error types.  Should be used in
    code to distinguish between different types of errors.
    """
    transport_details: Optional[MomentoErrorTransportDetails] = None
    """Low-level error details, from the transport layer.  Hopefully only needed
    in rare cases, by Momento engineers, for debugging.
    """
    message_wrapper: str
    """Prefix with basic information about the error class; this will be appended
    with specific information about the individual error instance at runtime."""

    def __init__(
        self,
        message: str,
        error_code: MomentoErrorCode,
        transport_details: Optional[MomentoErrorTransportDetails] = None,
        message_wrapper: str = "",
    ):
        super().__init__(message)
        self.message = message
        self.error_code = error_code
        self.transport_details = transport_details
        self.message_wrapper = message_wrapper

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}("
            f"message={self.message!r}, "
            f"error_code={self.error_code!r}, "
            f"transport_details={self.transport_details!r}, "
            f"message_wrapper={self.message_wrapper!r})"
        )

    def __str__(self) -> str:
        return repr(self)


class InvalidArgumentException(SdkException):
    """SDK client-side validation failed."""

    def __init__(
        self,
        message: str,
        transport_details: Optional[MomentoErrorTransportDetails] = None,
    ):
        super().__init__(
            message,
            MomentoErrorCode.INVALID_ARGUMENT_ERROR,
            transport_details,
            message_wrapper="Invalid argument passed to Momento client",
        )


class AlreadyExistsException(SdkException):
    """Resource already exists."""

    def __init__(
        self,
        message: str,
        transport_details: Optional[MomentoErrorTransportDetails] = None,
    ):
        super().__init__(
            message,
            MomentoErrorCode.ALREADY_EXISTS_ERROR,
            transport_details,
            message_wrapper="A cache with the specified name already exists. To resolve this error, either delete the existing cache and make a new one, or use a different name",  # noqa: E501
        )


class ServerUnavailableException(SdkException):
    """Server was unable to handle the request."""

    def __init__(
        self,
        message: str,
        transport_details: Optional[MomentoErrorTransportDetails] = None,
    ):
        super().__init__(
            message,
            MomentoErrorCode.SERVER_UNAVAILABLE,
            transport_details,
            message_wrapper="The server was unable to handle the request; consider retrying. If the error persists, please contact us at support@momentohq.com",  # noqa: E501
        )
