"""Enumerates the types of clients that can be used.

Used to populate the agent header in gRPC requests.
"""

from enum import Enum


class ClientType(Enum):
    """Describes the type of client that is being used."""

    CACHE = "cache"
    TOPIC = "topic"
