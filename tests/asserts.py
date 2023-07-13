from typing import Optional

from momento.errors.error_details import MomentoErrorCode
from momento.responses.mixins import ErrorResponseMixin
from momento.responses.response import Response


# Custom assertions
def assert_response_is_error(
    response: Response,
    *,
    error_code: Optional[MomentoErrorCode] = None,
    inner_exception_message: Optional[str] = None,
) -> None:
    assert isinstance(response, ErrorResponseMixin)
    if isinstance(response, ErrorResponseMixin):
        if error_code:
            assert response.error_code == error_code
        if inner_exception_message:
            assert response.inner_exception.message == inner_exception_message
