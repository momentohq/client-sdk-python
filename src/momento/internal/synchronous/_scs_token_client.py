from typing import Optional

from momento_wire_types import token_pb2 as token_pb
from momento_wire_types import token_pb2_grpc as token_grpc

from momento import logs
from momento.auth.access_control.disposable_token_scope import DisposableTokenProps, DisposableTokenScope
from momento.auth.credential_provider import CredentialProvider
from momento.config.auth_configuration import AuthConfiguration
from momento.errors.error_converter import convert_error
from momento.internal._utilities._permissions import permissions_from_disposable_token_scope
from momento.internal.services import Service
from momento.internal.synchronous._scs_grpc_manager import _TokenGrpcManager
from momento.responses.auth.generate_disposable_token import GenerateDisposableToken, GenerateDisposableTokenResponse
from momento.utilities.expiration import ExpiresIn
from momento.utilities.shared_sync_asyncio import validate_disposable_token_expiry


class _ScsTokenClient:
    """Momento Internal token client."""

    def __init__(self, configuration: AuthConfiguration, credential_provider: CredentialProvider):
        endpoint = credential_provider.token_endpoint
        self._logger = logs.logger
        self._logger.debug("Token client instantiated with endpoint: %s", endpoint)
        self._grpc_manager = _TokenGrpcManager(configuration, credential_provider)
        self._endpoint = endpoint

    @property
    def endpoint(self) -> str:
        return self._endpoint

    def generate_disposable_token(
        self,
        permission_scope: DisposableTokenScope,
        expires_in: ExpiresIn,
        credentialProvider: CredentialProvider,
        disposable_token_props: Optional[DisposableTokenProps] = None,
    ) -> GenerateDisposableTokenResponse:
        try:
            validate_disposable_token_expiry(expires_in)
            self._logger.info("Creating disposable token")

            token_id = disposable_token_props.token_id if disposable_token_props else None
            expires = token_pb._GenerateDisposableTokenRequest.Expires(valid_for_seconds=expires_in.valid_for_seconds())
            permissions = permissions_from_disposable_token_scope(permission_scope)

            request = token_pb._GenerateDisposableTokenRequest(
                auth_token=credentialProvider.get_auth_token(),
                expires=expires,
                permissions=permissions,
                token_id=token_id,
            )
            response = self._build_stub().GenerateDisposableToken(request)  # type: ignore[misc]
            return GenerateDisposableToken.Success.from_grpc_response(response)  # type: ignore[misc]
        except Exception as e:
            self._logger.debug("Failed to generate disposable token with exception: %s", e)
            return GenerateDisposableToken.Error(convert_error(e, Service.AUTH))

    def _build_stub(self) -> token_grpc.TokenStub:
        return self._grpc_manager.stub()

    def close(self) -> None:
        self._grpc_manager.close()