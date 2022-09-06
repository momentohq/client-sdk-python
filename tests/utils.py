import uuid


def uuid_str() -> str:
    return str(uuid.uuid4())


def uuid_bytes() -> bytes:
    return uuid.uuid4().bytes


def str_to_bytes(string: str) -> bytes:
    return string.encode("utf-8")
