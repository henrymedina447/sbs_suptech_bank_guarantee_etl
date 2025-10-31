from enum import Enum


class DocumentType(str, Enum):
    BANK_GUARANTEE = "BANK_GUARANTEE"
    LETTER = "LETTER"
