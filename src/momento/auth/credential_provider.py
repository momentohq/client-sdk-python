from __future__ import annotations

import copy
import os
from dataclasses import dataclass
from typing import Dict, Optional

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
