from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import timedelta
from pathlib import Path
from typing import Optional


class GrpcConfiguration(ABC):
    @abstractmethod
    def get_deadline(self) -> timedelta:
        pass

    @abstractmethod
    def with_deadline(self, deadline: timedelta) -> GrpcConfiguration:
        pass

    @abstractmethod
    def with_root_certificates_pem(self, root_certificates_pem_path: Path) -> GrpcConfiguration:
        pass

    @abstractmethod
    def get_root_certificates_pem(self) -> Optional[bytes]:
        pass

    @abstractmethod
    def get_max_send_message_length(self) -> Optional[int]:
        pass

    @abstractmethod
    def get_max_receive_message_length(self) -> Optional[int]:
        pass

    @abstractmethod
    def get_keepalive_permit_without_calls(self) -> Optional[int]:
        pass

    @abstractmethod
    def get_keepalive_time(self) -> Optional[timedelta]:
        pass

    @abstractmethod
    def get_keepalive_timeout(self) -> Optional[timedelta]:
        pass
