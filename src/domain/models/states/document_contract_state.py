from pydantic import BaseModel, Field

from domain.enums.document_status import DocumentStatus
from domain.enums.document_type import DocumentType


class DocumentContractState(BaseModel):
    record_id: str = Field(...)
    parent_id: str = Field(...)
    key: str = Field(...)
    session_id: str = Field(...)
    document_type: DocumentType = Field(default=DocumentType.BANK_GUARANTEE)
    period_month: str = Field(...)
    period_year: str = Field(...)
    status: DocumentStatus = Field(default=DocumentStatus.UNPROCESSED)
