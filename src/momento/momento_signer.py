import json
from enum import Enum
from typing import Optional, Dict
from jwt.api_jwk import PyJWK
from urllib.parse import quote
import jwt

from .errors import InvalidArgumentError


class CacheOperation(Enum):
    GET = 1
    SET = 2


class SigningRequest:
    def __init__(
        self,
        cache_name: str,
        cache_key: str,
        cache_operation: CacheOperation,
        expiry_epoch_seconds: int,
        ttl_seconds: Optional[int] = None,
    ):
        """Initializes a new SigningRequest.

        Args:
            cache_name: The name of the cache.
            cache_key: The key of the object.
            cache_operation: The operation performed on the item in the cache.
            expiry_epoch_seconds: The timestamp that the pre-signed URL is valid until.
            ttl_seconds: Time to Live for the item in Cache. This is an optional property that will only be used for CacheOperation.SET.
        """
        self._cache_name = cache_name
        self._cache_key = cache_key
        self._cache_operation = cache_operation
        self._expiry_epoch_seconds = expiry_epoch_seconds
        self._ttl_seconds = ttl_seconds

    def expiry_epoch_seconds(self) -> int:
        return self._expiry_epoch_seconds

    def cache_name(self) -> str:
        return self._cache_name

    def cache_key(self) -> str:
        return self._cache_key

    def cache_operation(self) -> CacheOperation:
        return self._cache_operation

    def ttl_seconds(self) -> Optional[int]:
        return self._ttl_seconds


class MomentoSigner:
    def __init__(self, jwk_json_string: str):
        """Initializes MomentoSigner with the specified private key.

        Args:
            jwk_json_string: the JSON string of the JWK key. This can be obtained from create_signing_key response.

        Raises:
            InvalidArgumentError: If the supplied private key is not valid.
        """
        try:
            self._jwk_json: Dict[str, str] = json.loads(jwk_json_string)
        except json.decoder.JSONDecodeError as e:
            raise InvalidArgumentError(
                f"Invalid JWK Json String: {jwk_json_string}"
            ) from e

        try:
            self._jwk: PyJWK = PyJWK.from_dict(self._jwk_json)  # type: ignore[no-untyped-call, misc]
        except jwt.exceptions.PyJWKError as e:
            raise InvalidArgumentError(f"Invalid JWK: {jwk_json_string}") from e
        except jwt.exceptions.InvalidKeyError as e:
            raise InvalidArgumentError(f"Invalid JWK: {jwk_json_string}") from e

        if self._jwk.key_id is None:  # type: ignore[misc]
            raise InvalidArgumentError(f"JWK missing kid attribute: {jwk_json_string}")

        self._alg = self._jwk_json.get("alg", None)
        if self._alg is None:
            self._alg = self._alg_fallback_logic()

    def sign_access_token(self, signing_request: SigningRequest) -> str:
        """Creates an access token to be used as a JWT token.

        Args:
            signing_request: Contains parameters for the generated token.

        Returns:
            str: the JWT token.
        """
        claims: Dict[str, object] = {
            "exp": signing_request.expiry_epoch_seconds(),
            "cache": signing_request.cache_name(),
            "key": signing_request.cache_key(),
        }

        if signing_request.cache_operation() == CacheOperation.GET:
            claims["method"] = ["get"]
        elif signing_request.cache_operation() == CacheOperation.SET:
            claims["method"] = ["set"]
            if signing_request.ttl_seconds() is None:
                raise InvalidArgumentError("ttl_seconds is required for SET operation.")
            claims["ttl"] = signing_request.ttl_seconds()
        else:
            raise NotImplementedError(
                f"Unrecognized Operation: {signing_request.cache_operation()}"
            )

        # jwt.encode will automatically insert "typ" and "alg" into the header for us.
        # We still need to specify "kid" to be included in the header however.
        return jwt.encode(
            claims, self._jwk.key, algorithm=self._alg, headers={"kid": self._jwk.key_id}  # type: ignore[misc]
        )

    def create_presigned_url(
        self, hostname: str, signing_request: SigningRequest
    ) -> str:
        """Creates a pre-signed HTTPS URL.

        Args:
            hostname: Hostname of SimpleCacheService. This value can be obtained from create_signing_key response.
            The SDK will prepend rest to it to get to the REST endpoint
            signing_request: Contains parameters for the generated URL.

        Returns:
            str: a pre-signed HTTPS URL with JWT token.
        """
        token = self.sign_access_token(signing_request)
        cache_name = quote(signing_request.cache_name(), safe="")
        cache_key = quote(signing_request.cache_key(), safe="")
        if signing_request.cache_operation() == CacheOperation.GET:
            return f"https://rest.{hostname}/cache/get/{cache_name}/{cache_key}?token={token}"
        elif signing_request.cache_operation() == CacheOperation.SET:
            ttl_seconds = signing_request.ttl_seconds()
            if ttl_seconds is None:
                raise InvalidArgumentError("ttl_seconds is required for SET operation.")
            url = f"https://rest.{hostname}/cache/set/{cache_name}/{cache_key}?token={token}&ttl_milliseconds={ttl_seconds * 1000}"
            return url
        else:
            raise NotImplementedError(
                f"Unrecognized Operation: {signing_request.cache_operation()}"
            )

    # Logic stolen from https://github.com/jpadilla/pyjwt/blob/master/jwt/api_jwk.py#L19
    # to handle the case when alg is missing from JWK.
    def _alg_fallback_logic(self) -> str:
        kty = self._jwk_json.get("kty", None)
        if kty is None:
            raise InvalidArgumentError("kty is not found: %s" % self._jwk_json)
        crv = self._jwk_json.get("crv", None)
        if kty == "EC":
            if crv == "P-256" or not crv:
                algorithm = "ES256"
            elif crv == "P-384":
                algorithm = "ES384"
            elif crv == "P-521":
                algorithm = "ES512"
            elif crv == "secp256k1":
                algorithm = "ES256K"
            else:
                raise InvalidArgumentError("Unsupported crv: %s" % crv)
        elif kty == "RSA":
            algorithm = "RS256"
        elif kty == "oct":
            algorithm = "HS256"
        elif kty == "OKP":
            if not crv:
                raise InvalidArgumentError("crv is not found: %s" % self._jwk_json)
            if crv == "Ed25519":
                algorithm = "EdDSA"
            else:
                raise InvalidArgumentError("Unsupported crv: %s" % crv)
        else:
            raise InvalidArgumentError("Unsupported kty: %s" % kty)

        return algorithm
