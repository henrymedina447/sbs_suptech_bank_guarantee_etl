from domain.enums.document_type import DocumentType
from domain.models.states.document_contract_state import DocumentContractState


class WorkflowOrchestratorServiceDomain:
    @staticmethod
    def transform_to_document_contract(file_name: str) -> DocumentContractState:
        return DocumentContractState(
            record_id="1",
            parent_id="",
            key=file_name,
            session_id="",
            document_type=DocumentType.BANK_GUARANTEE,
            period_month="",
            period_year=""
        )
