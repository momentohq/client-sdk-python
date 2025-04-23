from dataclasses import dataclass
from datetime import datetime
from typing import Optional

import grpc


# The lack of type hints in the grpc code causes a lint failure in combination with the dataclass annotation
@dataclass  # type: ignore[misc]
class RetryableProps:
    grpc_status: grpc.StatusCode
    grpc_method: str
    attempt_number: int
    overall_deadline: Optional[datetime] = None
