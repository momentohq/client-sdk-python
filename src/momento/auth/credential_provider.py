import os
from abc import ABC, abstractmethod
from typing import Optional
from . import momento_endpoint_resolver


class CredentialProvider(ABC):
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
