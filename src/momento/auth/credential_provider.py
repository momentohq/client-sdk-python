import os
from abc import ABC, abstractmethod
from typing import Optional

from . import momento_endpoint_resolver


class CredentialProvider(ABC):
    """Provides information that the SimpleCacheClient needs in order to establish a connection to and authenticate with
    the Momento service.
    """

    @abstractmethod
    def get_auth_token(self) -> str:
        pass

    @abstractmethod
    def get_control_endpoint(self) -> str:
        pass

    @abstractmethod
    def get_cache_endpoint(self) -> str:
        pass


class EnvMomentoTokenProvider(CredentialProvider):
    def __init__(self, env_var_name: str, control_endpoint: Optional[str] = None, cache_endpoint: Optional[str] = None):
        """Reads and parses a Momento auth token stored as an environment variable.

        :param env_var_name: name of the environment variable from which the auth token will be read
        :param control_endpoint: optionally overrides the default control endpoint
        :param cache_endpoint: optionally overrides the default control endpoint
        """
        token = os.getenv(env_var_name)
        if not token:
            raise RuntimeError(f"Missing required environment variable {env_var_name}")
        self._auth_token = token
        endpoints = momento_endpoint_resolver.resolve(self._auth_token)
        self._control_endpoint = control_endpoint or endpoints.control_endpoint
        self._cache_endpoint = cache_endpoint or endpoints.cache_endpoint

    def get_auth_token(self) -> str:
        return self._auth_token

    def get_control_endpoint(self) -> str:
        return self._control_endpoint

    def get_cache_endpoint(self) -> str:
        return self._cache_endpoint
