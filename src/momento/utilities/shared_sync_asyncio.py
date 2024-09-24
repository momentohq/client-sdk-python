DEFAULT_EAGER_CONNECTION_TIMEOUT_SECONDS = 30


def str_to_bytes(string: str) -> bytes:
    """Convert a string to bytes.

    Args:
        string (str): The string to convert.

    Returns:
        bytes: A UTF-8 byte representation of the string.
    """
    return string.encode("utf-8")
