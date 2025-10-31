import logging
from typing import Any

from application.dto.financial_metadata_result import FinancialMetadataResult
from application.dto.textract_pipeline_result import TextractPipelineResult
from application.ports.extractor_document_port import ExtractorDocumentPort
from application.ports.loader_metadata_port import LoaderMetadataPort
from application.ports.notification_port import NotificationPort
from application.ports.transform_document_port import TransformDocumentPort
from application.use_cases.workflows.workflow_base import WorkflowBase
from domain.models.states.document_contract_state import DocumentContractState
from domain.models.states.etl_bank_guarantee_state import EtlBankGuaranteeState
from domain.services.workflow_bank_guarantee_service import WorkflowBankGuaranteeServiceDomain


class WorkflowBankGuarantee(WorkflowBase):
    def __init__(self,
                 extractor: ExtractorDocumentPort,
                 transformer: TransformDocumentPort,
                 loader_metadata: LoaderMetadataPort,
                 notification: NotificationPort,
                 ):
        super().__init__(extractor, transformer, loader_metadata, notification)
        self.doc: DocumentContractState | None = None
        self.grid: list[list[str]] = []
        self.logger = logging.getLogger("app.workflows")

    async def _extract(self, state: EtlBankGuaranteeState) -> dict[str, Any]:
        try:
            extract_results: TextractPipelineResult | None = await self._extractor.extract_pipeline(
                document_data=self.doc)
            if extract_results is None:
                raise ValueError("No se consiguió extraer la metadata")
            self.grid = extract_results.grid
            return {
                "period_month": self.doc.period_month,
                "period_year": self.doc.period_year,
                "promotor": extract_results.promotor_text,
                "letter_date": WorkflowBankGuaranteeServiceDomain.transform_date(extract_results.date_text),
                "project_text": extract_results.project_text,
                "letter_text": extract_results.letter_text,
                "extract_success": True
            }
        except Exception as e:
            self.logger.error(f"Error en la extracción en bank guarantee: {str(e)}")
            return {
                "extract_success": False
            }

    def _transform(self, state: EtlBankGuaranteeState) -> dict[str, Any]:
        try:
            extract_success: bool = state.extract_success

            if not extract_success:
                raise ValueError("El proceso de extracción fue fallido en bank guarantee")
            aux: FinancialMetadataResult | None = self._transformer.get_financial_metadata(grid=self.grid)
            if aux is None:
                raise ValueError("La transformación debió un resultado nulo en bank guarantee")
            return {
                "transform_success": True,
                "disbursed_amount": aux.disbursed_amount,
                "reduced_amount": aux.reduced_amount,
                "total_amount": aux.total_amount
            }

        except Exception as e:
            self.logger.error(f"Error en la transformación en bank guarantee: {str(e)}")
            return {
                "transform_success": False
            }

    def _load(self, state: EtlBankGuaranteeState) -> dict[str, Any]:
        try:
            transform_success: bool = state.transform_success
            if not transform_success:
                raise ValueError("El proceso de transformación fue fallido en bank guarantee")

            entity = WorkflowBankGuaranteeServiceDomain.transform_in_entity_to_dynamo(self.doc, state)
            self.logger.info("Grabar Metadada")
            self._loader_metadata.save_metadata(entity)
            return {
                "load_success": True
            }
        except Exception as e:
            self.logger.error(f"Error en la carga en bank guarantee: {str(e)}")
            return {
                "load_success": False
            }

    def _final_task(self, state: EtlBankGuaranteeState) -> dict[str, Any]:
        return {}

    async def execute(self, doc: DocumentContractState) -> DocumentContractState:
        self.doc = doc
        state = EtlBankGuaranteeState(record_id=doc.record_id)
        output = await self._graph.ainvoke(state)
        return doc
