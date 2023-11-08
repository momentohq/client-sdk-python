from __future__ import annotations

import asyncio
import time
import uuid

from momento.requests.vector_index import Item
from momento.responses.vector_index import Search, SearchAndFetchVectors
from momento.responses.vector_index.data.search import SearchHit
from momento.responses.vector_index.data.search_and_fetch_vectors import (
    SearchAndFetchVectorsHit,
)


def unique_test_cache_name() -> str:
    return f"python-test-{uuid_str()}"


def unique_test_vector_index_name() -> str:
    return f"python-test-{uuid_str()}"


def uuid_str() -> str:
    """Generate a UUID as a string.

    Returns:
        str: A UUID
    """
    return str(uuid.uuid4())


def uuid_bytes() -> bytes:
    """Generate a UUID as bytes.

    Returns:
        bytes: A UUID
    """
    return uuid.uuid4().bytes


def str_to_bytes(string: str) -> bytes:
    """Convert a string to bytes.

    Args:
        string (str): The string to convert.

    Returns:
        bytes: A UTF-8 byte representation of the string.
    """
    return string.encode("utf-8")


def sleep(seconds: int) -> None:
    time.sleep(seconds)


async def sleep_async(seconds: int) -> None:
    await asyncio.sleep(seconds)


def when_fetching_vectors_apply_vectors_to_hits(
    response: Search.Success | SearchAndFetchVectors.Success,
    hits: list[SearchHit] | list[SearchAndFetchVectorsHit],
    items: list[Item],
) -> list[SearchHit]:
    if isinstance(response, Search.Success):
        return hits  # type: ignore
    item_index = {item.id: item for item in items}
    return [SearchAndFetchVectorsHit.from_search_hit(hit, item_index[hit.id].vector) for hit in hits]
