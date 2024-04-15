from dataclasses import dataclass
from enum import Enum


class ContentType(Enum):
    JSON = 0
    RAW = 1
    GTFS_RT = 2
    CSV = 3
    GTFS = 4


@dataclass(frozen=True)
class BridgeResult:
    """
    A result of a bridge operation.
    """

    content_type: ContentType
    size: int
    original_size: int
