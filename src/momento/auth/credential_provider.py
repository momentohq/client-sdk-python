from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Optional

from . import momento_endpoint_resolver


@dataclass
class CredentialProvider:
    """Provides information that the SimpleCacheClient needs in order to establish a connection to and authenticate with
    the Momento service.
    """

    auth_token: str
    control_endpoint: str
    cache_endpoint: str

    @staticmethod
    def from_environment_variable(
        env_var_name: str,
        control_endpoint: Optional[str] = None,
        cache_endpoint: Optional[str] = None,
    ) -> CredentialProvider:
        """Reads and parses a Momento auth token stored as an environment variable.

        Args:
            env_var_name (str): Name of the environment variable from which the auth token will be read
            control_endpoint (Optional[str], optional): Optionally overrides the default control endpoint.
            Defaults to None.
            cache_endpoint (Optional[str], optional): Optionally overrides the default control endpoint.
            Defaults to None.

        Raises:
            RuntimeError: if the environment variable is missing

        Returns:
            CredentialProvider
        """
        auth_token = os.getenv(env_var_name)
        if not auth_token:
            raise RuntimeError(f"Missing required environment variable {env_var_name}")
        return CredentialProvider.from_string(auth_token, control_endpoint, cache_endpoint)

    @staticmethod
    def from_string(
        auth_token: str,
        control_endpoint: Optional[str] = None,
        cache_endpoint: Optional[str] = None,
    ) -> CredentialProvider:
        """Reads and parses a Momento auth token.

        Args:
            auth_token (str): the Momento auth token
            control_endpoint (Optional[str], optional): Optionally overrides the default control endpoint.
            Defaults to None.
            cache_endpoint (Optional[str], optional): Optionally overrides the default control endpoint.
            Defaults to None.

        Returns:
            CredentialProvider
        """
        endpoints = momento_endpoint_resolver.resolve(auth_token)
        control_endpoint = control_endpoint or endpoints.control_endpoint
        cache_endpoint = cache_endpoint or endpoints.cache_endpoint
        return CredentialProvider(auth_token, control_endpoint, cache_endpoint)
