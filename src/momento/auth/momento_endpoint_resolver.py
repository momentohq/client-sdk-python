import base64
import json
from dataclasses import dataclass
from typing import Union

import jwt
from jwt.exceptions import DecodeError

from momento.errors import InvalidArgumentException
from momento.internal.services import Service

_MOMENTO_CONTROL_ENDPOINT_PREFIX = "control."
_MOMENTO_CACHE_ENDPOINT_PREFIX = "cache."
_MOMENTO_TOKEN_ENDPOINT_PREFIX = "token."
_CONTROL_ENDPOINT_CLAIM_ID = "cp"
_CACHE_ENDPOINT_CLAIM_ID = "c"
_API_KEY_TYPE_CLAIM_ID = "t"
_GLOBAL_API_KEY_TYPE = "g"


@dataclass
class _TokenAndEndpoints:
    control_endpoint: str
    cache_endpoint: str
    token_endpoint: str
    auth_token: str


@dataclass
class _Base64DecodedV1Token:
    api_key: str
    endpoint: str


def resolve(auth_token: str) -> _TokenAndEndpoints:
    """Helper function used by from_string and from_disposable_token to parse legacy and v1 auth tokens.

    Args:
        auth_token (str): The auth token to be resolved.

    Returns:
        _TokenAndEndpoints
    """
    if not auth_token:
        raise InvalidArgumentException("malformed auth token", Service.AUTH)

    if _is_base64(auth_token):
        decoded_b64_token = base64.b64decode(auth_token).decode("utf-8")
        info = json.loads(decoded_b64_token)  # type: ignore[misc]
        return _TokenAndEndpoints(
            control_endpoint=_MOMENTO_CONTROL_ENDPOINT_PREFIX + info["endpoint"],  # type: ignore[misc]
            cache_endpoint=_MOMENTO_CACHE_ENDPOINT_PREFIX + info["endpoint"],  # type: ignore[misc]
            token_endpoint=_MOMENTO_TOKEN_ENDPOINT_PREFIX + info["endpoint"],  # type: ignore[misc]
            auth_token=info["api_key"],  # type: ignore[misc]
        )
    else:
        if _is_v2_api_key(auth_token):
            raise InvalidArgumentException(
                "Unexpectedly received a v2 API key. Are you using the correct key and the correct CredentialProvider method?",
                Service.AUTH,
            )
        return _get_endpoint_from_token(auth_token)


def _get_endpoint_from_token(auth_token: str) -> _TokenAndEndpoints:
    try:
        claims = jwt.decode(auth_token, options={"verify_signature": False})  # type: ignore[misc]
        return _TokenAndEndpoints(
            control_endpoint=claims[_CONTROL_ENDPOINT_CLAIM_ID],  # type: ignore[misc]
            cache_endpoint=claims[_CACHE_ENDPOINT_CLAIM_ID],  # type: ignore[misc]
            token_endpoint=_MOMENTO_TOKEN_ENDPOINT_PREFIX + claims[_CACHE_ENDPOINT_CLAIM_ID],  # type: ignore[misc]
            auth_token=auth_token,
        )
    except (DecodeError, KeyError) as e:
        raise InvalidArgumentException("Invalid Auth token.", Service.AUTH) from e


def _is_base64(value: Union[bytes, str]) -> bool:
    try:
        if isinstance(value, str):
            value = value.encode("utf-8")
        return base64.b64encode(base64.b64decode(value)) == value
    except Exception:
        return False


def _is_v2_api_key(key: str) -> bool:
    if _is_base64(key):
        return False
    try:
        claims = jwt.decode(key, options={"verify_signature": False})  # type: ignore[misc]
        return _API_KEY_TYPE_CLAIM_ID in claims and claims[_API_KEY_TYPE_CLAIM_ID] == _GLOBAL_API_KEY_TYPE  # type: ignore[misc]
    except DecodeError:
        return False
