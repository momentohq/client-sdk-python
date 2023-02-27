from __future__ import annotations

from abc import ABC
from dataclasses import dataclass
from datetime import datetime
from typing import Any

import google

from momento.responses.response import ControlResponse

from ...mixins import ErrorResponseMixin


@dataclass
class SigningKey:
    """Signing keys returned from requesting list signing keys."""

    key_id: str
    """the ID of the signing key"""
    expires_at: datetime
    """when the key expires"""
    endpoint: str
    """endpoint of the signing key"""

    @staticmethod
    def from_grpc_response(grpc_listed_signing_key: Any, endpoint: str) -> SigningKey:  # type: ignore[misc]
        key_id: str = grpc_listed_signing_key.key_id
        expires_at: datetime = datetime.fromtimestamp(grpc_listed_signing_key.expires_at)
        return SigningKey(key_id, expires_at, endpoint)


class ListSigningKeysResponse(ControlResponse):
    """Parent response type for a cache `list_signing_key` request.

    Its subtypes are:
    - `ListSigningKeys.Success`
    - `ListSigningKeys.Error`

    See `CacheClient` for how to work with responses.
    """


class ListSigningKeys(ABC):
    """Groups all `ListSigningKeysResponse` derived types under a common namespace."""

    @dataclass
    class Success(ListSigningKeysResponse):
        """The response from listing signing keys."""

        signing_keys: list[SigningKey]
        """all signing keys in this page"""

        @staticmethod
        def from_grpc_response(  # type:ignore[misc]
            grpc_list_signing_keys_response: google.protobuf.message.Message, endpoint: str
        ) -> ListSigningKeys.Success:
            """Creates a ListSigningKeysResponse from a grpc response.

            Args:
                grpc_list_signing_keys_response (google.protobuf.message.Message):
                endpoint (str): _description_

            Returns:
                ListSigningKeysResponse
            """
            signing_keys: list[SigningKey] = [
                SigningKey.from_grpc_response(signing_key, endpoint)
                for signing_key in grpc_list_signing_keys_response.signing_key
            ]
            return ListSigningKeys.Success(signing_keys)

    class Error(ListSigningKeysResponse, ErrorResponseMixin):
        """Contains information about an error returned from a request.

        This includes:
        - `error_code`: `MomentoErrorCode` value for the error.
        - `messsage`: a detailed error message.
        """
