from abc import ABC, abstractmethod

from application.dto.financial_metadata_result import FinancialMetadataResult


class TransformDocumentPort(ABC):
    @abstractmethod
    def get_financial_metadata(self, **kwargs)->FinancialMetadataResult:
        ...
