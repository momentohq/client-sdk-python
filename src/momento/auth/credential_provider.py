from __future__ import annotations

import copy
import os
from dataclasses import dataclass
from typing import Dict, Optional
from warnings import warn

from momento.errors.exceptions import InvalidArgumentException
from momento.internal.services import Service

from . import momento_endpoint_resolver


@dataclass
class CredentialProvider:
    """Information the CacheClient needs to connect to and authenticate with the Momento service."""

    auth_token: str
    control_endpoint: str
    cache_endpoint: str
    token_endpoint: str
    port: int

    @staticmethod
    def from_environment_variable(
        env_var_name: str,
        control_endpoint: Optional[str] = None,
        cache_endpoint: Optional[str] = None,
        token_endpoint: Optional[str] = None,
    ) -> CredentialProvider:
        """Reads and parses a Momento auth token stored as an environment variable.

        Deprecated as of v1.28.0. Use from_environment_variables_v2 instead.

        Args:
            env_var_name (str): Name of the environment variable from which the API key will be read
            control_endpoint (Optional[str], optional): Optionally overrides the default control endpoint.
            Defaults to None.
            cache_endpoint (Optional[str], optional): Optionally overrides the default cache endpoint.
            Defaults to None.
            token_endpoint (Optional[str], optional): Optionally overrides the default token endpoint.
            Defaults to None.

        Raises:
            RuntimeError: if the environment variable is missing

        Returns:
            CredentialProvider
        """
        warn(
            "from_environment_variable is deprecated, use from_environment_variables_v2 instead",
            DeprecationWarning,
            stacklevel=2,
        )
        api_key = os.getenv(env_var_name)
        if not api_key:
            raise RuntimeError(f"Missing required environment variable {env_var_name}")
        return CredentialProvider.from_string(api_key, control_endpoint, cache_endpoint, token_endpoint)

    @staticmethod
    def from_string(
        auth_token: str,
        control_endpoint: Optional[str] = None,
        cache_endpoint: Optional[str] = None,
        token_endpoint: Optional[str] = None,
    ) -> CredentialProvider:
        """Reads and parses a Momento auth token.

        Deprecated as of v1.28.0. Use from_api_key_v2 or from_disposable_token instead.

        Args:
            auth_token (str): the Momento API key (previously: auth token)
            control_endpoint (Optional[str], optional): Optionally overrides the default control endpoint.
            Defaults to None.
            cache_endpoint (Optional[str], optional): Optionally overrides the default cache endpoint.
            Defaults to None.
            token_endpoint (Optional[str], optional): Optionally overrides the default token endpoint.
            Defaults to None.

        Returns:
            CredentialProvider
        """
        warn(
            "from_string is deprecated, use from_api_key_v2 or from_disposable_token instead",
            DeprecationWarning,
            stacklevel=2,
        )
        token_and_endpoints = momento_endpoint_resolver.resolve(auth_token)
        control_endpoint = control_endpoint or token_and_endpoints.control_endpoint
        cache_endpoint = cache_endpoint or token_and_endpoints.cache_endpoint
        token_endpoint = token_endpoint or token_and_endpoints.token_endpoint
        auth_token = token_and_endpoints.auth_token
        return CredentialProvider(auth_token, control_endpoint, cache_endpoint, token_endpoint, 443)

    @staticmethod
    def for_momento_local(
        host: str = "127.0.0.1",
        port: int = 8080,
    ) -> CredentialProvider:
        """Creates a credential provider for use with Momento Local.

        Args:
            host (str): the Momento Local host.
            port (int): the Momento Local port.

        Returns:
            CredentialProvider
        """
        return CredentialProvider("", host, host, host, port)

    def __repr__(self) -> str:
        attributes: Dict[str, str] = copy.copy(vars(self))  # type: ignore[misc]
        attributes["auth_token"] = self._obscure(attributes["auth_token"])
        message = ", ".join(f"{k}={v!r}" for k, v in attributes.items())
        return f"{self.__class__.__name__}({message})"

    def _obscure(self, value: str) -> str:
        return f"{value[:10]}...{value[-10:]}"

    def get_auth_token(self) -> str:
        return self.auth_token

    @staticmethod
    def from_api_key_v2(api_key: str, endpoint: str) -> CredentialProvider:
        """Creates a CredentialProvider from a v2 API key and endpoint.

        Args:
            api_key (str): The v2 API key.
            endpoint (str): The Momento service endpoint.

        Returns:
            CredentialProvider
        """
        if len(api_key) == 0:
            raise InvalidArgumentException("API key cannot be empty.", Service.AUTH)
        if len(endpoint) == 0:
            raise InvalidArgumentException("Endpoint cannot be empty.", Service.AUTH)

        if not momento_endpoint_resolver._is_v2_api_key(api_key):
            raise InvalidArgumentException(
                "Received an invalid v2 API key. Are you using the correct key and the correct CredentialProvider method?",
                Service.AUTH,
            )
        return CredentialProvider(
            auth_token=api_key,
            control_endpoint=momento_endpoint_resolver._MOMENTO_CONTROL_ENDPOINT_PREFIX + endpoint,
            cache_endpoint=momento_endpoint_resolver._MOMENTO_CACHE_ENDPOINT_PREFIX + endpoint,
            token_endpoint=momento_endpoint_resolver._MOMENTO_TOKEN_ENDPOINT_PREFIX + endpoint,
            port=443,
        )

    @staticmethod
    def from_environment_variables_v2(
        api_key_env_var: str = "MOMENTO_API_KEY", endpoint_env_var: str = "MOMENTO_ENDPOINT"
    ) -> CredentialProvider:
        """Creates a CredentialProvider from an endpoint and v2 API key stored in the environment variables MOMENTO_API_KEY and MOMENTO_ENDPOINT.

        Args:
            api_key_env_var (str): Optionally provide an alternate environment variable name from which the v2 API key will be read.
            endpoint_env_var (str): Optionally provide an alternate environment variable name from which the Momento service endpoint will be read.

        Returns:
            CredentialProvider
        """
        if len(api_key_env_var) == 0:
            raise InvalidArgumentException("API key environment variable name cannot be empty.", Service.AUTH)
        if len(endpoint_env_var) == 0:
            raise InvalidArgumentException("Endpoint environment variable name cannot be empty.", Service.AUTH)

        api_key = os.getenv(api_key_env_var)
        if not api_key:
            raise RuntimeError(f"Missing required environment variable {api_key_env_var}")
        endpoint = os.getenv(endpoint_env_var)
        if not endpoint:
            raise RuntimeError(f"Missing required environment variable {endpoint_env_var}")

        if not momento_endpoint_resolver._is_v2_api_key(api_key):
            raise InvalidArgumentException(
                "Received an invalid v2 API key. Are you using the correct key? Or did you mean to use `from_environment_variable()` with a legacy key instead?",
                Service.AUTH,
            )
        return CredentialProvider.from_api_key_v2(api_key, endpoint)

    @staticmethod
    def from_disposable_token(auth_token: str) -> CredentialProvider:
        """Reads and parses a Momento disposable auth token.

        Args:
            auth_token (str): the Momento disposable auth token

        Returns:
            CredentialProvider
        """
        if len(auth_token) == 0:
            raise InvalidArgumentException("Disposable token cannot be empty.", Service.AUTH)
        token_and_endpoints = momento_endpoint_resolver.resolve(auth_token)
        auth_token = token_and_endpoints.auth_token
        return CredentialProvider(
            auth_token,
            token_and_endpoints.control_endpoint,
            token_and_endpoints.cache_endpoint,
            token_and_endpoints.token_endpoint,
            443,
        )
