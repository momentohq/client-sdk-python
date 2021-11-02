import jwt
from jwt.exceptions import DecodeError

from . import errors

_MOMENTO_CONTROL_ENDPOINT_PREFIX = 'control.'
_MOMENTO_CACHE_ENDPOINT_PREFIX = 'cache.'
_CONTROL_ENDPOINT_CLAIM_ID = 'cp'
_CACHE_ENDPOINT_CLAIM_ID = 'c'


class _Endpoints:
    def __init__(self, control_endpoint, cache_endpoint):
        self.control_endpoint = control_endpoint
        self.cache_endpoint = cache_endpoint


def resolve(auth_token, endpoint_override=None):
    if (endpoint_override is not None):
        return _Endpoints(_MOMENTO_CONTROL_ENDPOINT_PREFIX + endpoint_override,
                          _MOMENTO_CACHE_ENDPOINT_PREFIX + endpoint_override)
    return _getEndpointFromToken(auth_token)


def _getEndpointFromToken(auth_token):
    try:
        claims = jwt.decode(auth_token, options={'verify_signature': False})
        return _Endpoints(claims[_CONTROL_ENDPOINT_CLAIM_ID],
                          claims[_CACHE_ENDPOINT_CLAIM_ID])
    except (DecodeError, KeyError):
        raise errors.InvalidInputError('Invalid Auth token.') from None
