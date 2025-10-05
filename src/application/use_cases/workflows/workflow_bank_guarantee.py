from typing import Any
from application.ports.extractor_document_port import ExtractorDocumentPort
from application.ports.loader_metadata_port import LoaderMetadataPort
from application.ports.notification_port import NotificationPort
from application.ports.transform_document_port import TransformDocumentPort
from application.use_cases.workflows.workflow_base import WorkflowBase
from domain.models.states.document_contract_state import DocumentContractState
from domain.models.states.etl_bank_guarantee_state import EtlBankGuaranteeState


class WorkflowBankGuarantee(WorkflowBase):
    def __init__(self,
                 extractor: ExtractorDocumentPort,
                 transformer: TransformDocumentPort,
                 loader_metadata: LoaderMetadataPort,
                 notification: NotificationPort,
                 ):
        super().__init__(extractor, transformer, loader_metadata, notification)
        self.doc: DocumentContractState | None = None

    async def _extract(self, state: EtlBankGuaranteeState) -> dict[str, Any]:
        extract_results = await self._extractor.extract_pipeline(document_data=self.doc)
        print("result", extract_results)
        return {}

    def _transform(self, state: EtlBankGuaranteeState) -> dict[str, Any]:
        return {}

    def _load(self, state: EtlBankGuaranteeState) -> dict[str, Any]:
        return {}

    def _final_task(self, state: EtlBankGuaranteeState) -> dict[str, Any]:
        return {}

    async def execute(self, doc: DocumentContractState) -> DocumentContractState:
        self.doc = doc
        state = EtlBankGuaranteeState(record_id=doc.record_id)
        output = await self._graph.ainvoke(state)
        return doc
