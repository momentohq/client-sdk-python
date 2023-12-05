from __future__ import annotations

from typing import Awaitable

from momento.errors import MomentoErrorCode
from momento.responses import PubsubResponse
from momento.typing import TCacheName, TTopicName
from typing_extensions import Protocol

from tests.asserts import assert_response_is_error
from tests.utils import uuid_str


class TCacheNameValidator(Protocol):
    def __call__(self, cache_name: TCacheName) -> Awaitable[PubsubResponse]:
        ...


def a_cache_name_validator() -> None:
    async def with_non_existent_cache_name_it_throws_not_found(cache_name_validator: TCacheNameValidator) -> None:
        cache_name = uuid_str()
        response = await cache_name_validator(cache_name=cache_name)
        assert_response_is_error(response, error_code=MomentoErrorCode.NOT_FOUND_ERROR)

    async def with_null_cache_name_it_throws_exception(cache_name_validator: TCacheNameValidator) -> None:
        response = await cache_name_validator(cache_name=None)  # type: ignore
        assert_response_is_error(
            response,
            error_code=MomentoErrorCode.INVALID_ARGUMENT_ERROR,
            inner_exception_message="Cache name must be a string",
        )

    async def with_empty_cache_name_it_throws_exception(cache_name_validator: TCacheNameValidator) -> None:
        response = await cache_name_validator(cache_name="")
        assert_response_is_error(
            response,
            error_code=MomentoErrorCode.INVALID_ARGUMENT_ERROR,
            inner_exception_message="Cache name must not be empty",
        )

    async def with_bad_cache_name_throws_exception(cache_name_validator: TCacheNameValidator) -> None:
        response = await cache_name_validator(cache_name=1)  # type: ignore
        assert_response_is_error(
            response,
            error_code=MomentoErrorCode.INVALID_ARGUMENT_ERROR,
            inner_exception_message="Cache name must be a string",
        )


class TTopicValidator(Protocol):
    def __call__(self, topic_name: TTopicName) -> Awaitable[PubsubResponse]:
        ...


def a_topic_validator() -> None:
    async def with_null_topic_throws_exception(cache_name: str, topic_validator: TTopicValidator) -> None:
        response = await topic_validator(topic_name=None)  # type: ignore
        assert_response_is_error(response, error_code=MomentoErrorCode.INVALID_ARGUMENT_ERROR)
