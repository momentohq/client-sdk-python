class SdkError(Exception):
    """Base exception for all errors raised by Sdk"""

    def __init__(self, message: str):
        super().__init__(message)


class ClientSdkError(SdkError):
    """For all errors raised by the client.

    Indicates that the request failed on the SDK. The request either did not
    make it to the service or if it did the response from the service could
    not be parsed successfully.
    """

    def __init__(self, message: str):
        super().__init__(message)


class InvalidArgumentError(ClientSdkError):
    """Error raised when provided input values to the SDK are invalid

    Some examples - missing required parameters, incorrect parameter
    types, malformed input.
    """

    def __init__(self, message: str):
        super().__init__(message)


class MomentoServiceError(SdkError):
    """Errors raised when Momento Service returned an error code"""

    def __init__(self, message: str):
        super().__init__(message)


class NotFoundError(MomentoServiceError):
    """Error raised for operations performed on non-existent cache"""

    def __init__(self, message: str):
        super().__init__(message)


class AlreadyExistsError(MomentoServiceError):
    """Error raised when attempting to create a cache with same name"""

    def __init__(self, message: str):
        super().__init__(message)


class BadRequestError(MomentoServiceError):
    """Error raised when service validation fails for provided values"""

    def __init__(self, message: str):
        super().__init__(message)


class PermissionError(MomentoServiceError):
    """Error for insufficient permissions to perform an operation with Cache Service."""

    def __init__(self, message: str):
        super().__init__(message)


class AuthenticationError(MomentoServiceError):
    """Error when authentication with Cache Service fails"""

    def __init__(self, message: str):
        super().__init__(message)


class CancelledError(MomentoServiceError):
    """Error when an operation with Cache Service was cancelled"""

    def __init__(self, message: str):
        super().__init__(message)


class TimeoutError(MomentoServiceError):
    """Error when an operation did not complete in time"""

    def __init__(self, message: str):
        super().__init__(message)


class LimitExceededError(MomentoServiceError):
    """Error when calls are throttled due to request limit rate"""

    def __init__(self, message: str):
        super().__init__(message)


class InternalServerError(MomentoServiceError):
    """Operation failed on the server with an unknown error"""

    def __init__(self, message: str):
        super().__init__(message)
