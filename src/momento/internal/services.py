from enum import Enum


class Service(Enum):
    CACHE = "cache"
    TOPICS = "topics"
    INDEX = "index"
    AUTH = "auth"  # not really a service we provide, but we have APIs for auth that applies to all services
