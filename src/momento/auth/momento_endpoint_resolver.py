import base64
import json
from dataclasses import dataclass
from typing import Union

import jwt
from jwt.exceptions import DecodeError

from momento.errors import InvalidArgumentException

_MOMENTO_CONTROL_ENDPOINT_PREFIX = "control."
_MOMENTO_CACHE_ENDPOINT_PREFIX = "cache."
_CONTROL_ENDPOINT_CLAIM_ID = "cp"
_CACHE_ENDPOINT_CLAIM_ID = "c"


@dataclass
class _TokenAndEndpoints:
    control_endpoint: str
    cache_endpoint: str
    auth_token: str


@dataclass
class _Base64DecodedV1Token:
    api_key: str
    endpoint: str


def resolve(auth_token: str) -> _TokenAndEndpoints:
    if not auth_token:
        raise InvalidArgumentException("malformed auth token")

    if _is_base64(auth_token):
        decoded_b64_token = base64.b64decode(auth_token).decode("utf-8")
        info = json.loads(decoded_b64_token)  # type: ignore[misc]
        return _TokenAndEndpoints(
            control_endpoint=_MOMENTO_CONTROL_ENDPOINT_PREFIX + info["endpoint"],  # type: ignore[misc]
            cache_endpoint=_MOMENTO_CACHE_ENDPOINT_PREFIX + info["endpoint"],  # type: ignore[misc]
            auth_token=info["api_key"],  # type: ignore[misc]
        )
    else:
        return _get_endpoint_from_token(auth_token)


def _get_endpoint_from_token(auth_token: str) -> _TokenAndEndpoints:
    try:
        claims = jwt.decode(auth_token, options={"verify_signature": False})  # type: ignore[misc]
        return _TokenAndEndpoints(
            control_endpoint=claims[_CONTROL_ENDPOINT_CLAIM_ID],  # type: ignore[misc]
            cache_endpoint=claims[_CACHE_ENDPOINT_CLAIM_ID],  # type: ignore[misc]
            auth_token=auth_token,
        )
    except (DecodeError, KeyError) as e:
        raise InvalidArgumentException("Invalid Auth token.") from e


def _is_base64(value: Union[bytes, str]) -> bool:
    try:
        if isinstance(value, str):
            value = value.encode("utf-8")
        return base64.b64encode(base64.b64decode(value)) == value
    except (Exception,):
        return False
