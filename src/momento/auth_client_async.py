from __future__ import annotations

from types import TracebackType
from typing import Optional, Type

from momento import logs
from momento.auth.access_control.disposable_token_scope import DisposableTokenProps, DisposableTokenScope
from momento.auth.credential_provider import CredentialProvider
from momento.config.auth_configuration import AuthConfiguration
from momento.internal.aio._scs_token_client import _ScsTokenClient
from momento.responses.auth.generate_disposable_token import GenerateDisposableTokenResponse
from momento.utilities.expiration import ExpiresIn


class AuthClientAsync:
    """Async Auth Client.

    Auth methods return a response object unique to each request.
    The response object is resolved to a type-safe object of one of several
    sub-types. See the documentation for each response type for details.

    Pattern matching can be used to operate on the appropriate subtype.
    For example, in python 3.10+ if you're generating a disposable auth token:

        response = await client.generateDisposableToken(...)
        match response:
            case GenerateDisposableToken.Success():
                ...the disposable auth token was generated...
            case GenerateDisposableToken.Error():
                ...there was an error trying to generate the auth token...

    or equivalently in earlier versions of python::

        response = await client.generateDisposableToken(...)
        if isinstance(response, GenerateDisposableToken.Success):
            ...
        elif isinstance(response, GenerateDisposableToken.Error):
            ...
        else:
            raise Exception("This should never happen")
    """

    def __init__(self, configuration: AuthConfiguration, credential_provider: CredentialProvider):
        """Instantiate a client.

        Args:
            configuration (AuthConfiguration): An object holding configuration settings for communication with the server.
            credential_provider (CredentialProvider): An object holding the auth token and endpoint information.

        Example::
            from momento import AuthConfigurations, CredentialProvider, AuthClientAsync

            configuration = AuthConfigurations.Laptop.latest()
            credential_provider = CredentialProvider.from_environment_variable("MOMENTO_API_KEY")
            client = AuthClientAsync(configuration, credential_provider)
        """
        self._logger = logs.logger
        self._token_client = _ScsTokenClient(configuration, credential_provider)
        self._credential_provider = credential_provider

    async def __aenter__(self) -> AuthClientAsync:
        return self

    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> None:
        await self._token_client.close()

    async def generate_disposable_token(
        self,
        scope: DisposableTokenScope,
        expires_in: ExpiresIn,
        disposable_token_props: Optional[DisposableTokenProps] = None,
    ) -> GenerateDisposableTokenResponse:
        """Generate a disposable auth token.

        Returns:
            GenerateDisposableTokenResponse
        """
        return await self._token_client.generate_disposable_token(
            scope, expires_in, self._credential_provider, disposable_token_props
        )

    async def close(self) -> None:
        await self._token_client.close()