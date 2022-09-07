import uuid


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
