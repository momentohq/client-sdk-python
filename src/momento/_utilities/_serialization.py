from typing import Dict


def _bytes_to_string(value: bytes) -> str:
    """Decode bytes to a UTF-8 string.

    Args:
        value (bytes): The bytes to decode

    Returns:
        str: UTF-8 representation of bytes

    Raises:
        UnicodeDecodeError
    """
    return value.decode(encoding="utf-8")


def _bytes_dict_to_string_dict(dictionary: Dict[bytes, bytes]) -> Dict[str, str]:
    return {_bytes_to_string(k): _bytes_to_string(v) for k, v in dictionary.items()}
