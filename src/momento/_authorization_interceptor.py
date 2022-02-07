from ._generic_client_interceptor import _GenericClientInterceptor

from . import _header_client_interceptor


def get_authorization_interceptor(auth_token: str) -> _GenericClientInterceptor:
    return _header_client_interceptor.header_adder_interceptor(
        "authorization", auth_token
    )
