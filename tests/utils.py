import asyncio
import time
import uuid


def unique_test_cache_name() -> str:
    return f"python-test-{uuid_str()}"


def unique_test_vector_index_name() -> str:
    name = f"python-test-{uuid_str()}"
    # TODO remove this
    return name.replace("-", "_")


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
