from enum import Enum


class DocumentStatus(str, Enum):
    PROCESSED = "PROCESSED"
    UNPROCESSED = "UNPROCESSED"
    FAILED = "FAILED"
