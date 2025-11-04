import asyncio
import logging
import uuid

from typing import Any, Literal

from langgraph.constants import START, END
from langgraph.graph import StateGraph
from langgraph.graph.state import CompiledStateGraph

from application.ports.extractor_document_port import ExtractorDocumentPort
from application.ports.loader_document_port import LoaderDocumentPort
from application.ports.loader_metadata_port import LoaderMetadataPort
from application.ports.notification_port import NotificationPort
from application.ports.poller_document_port import PollerDocumentPort
from application.ports.transform_document_port import TransformDocumentPort
from application.use_cases.workflows.workflow_bank_guarantee import (
    WorkflowBankGuarantee,
)
from domain.models.states.document_contract_state import DocumentContractState
from domain.models.states.etl_orchestrator_state import EtlOrchestratorState
from domain.services.workflow_orchestrator_service import (
    WorkflowOrchestratorServiceDomain,
)
from domain.models.notification import Notification, NotificationData

from infrastructure.config.app_settings import AppSettings, get_app_settings
from application.services.workflow_orchestrator_service import (
    WorkflowOrchestratorServiceApplication as wfa,
)


class WorkflowOrchestrator:
    def __init__(
        self,
        extractor: ExtractorDocumentPort,
        poller: PollerDocumentPort,
        transformer: TransformDocumentPort,
        loader_metadata: LoaderMetadataPort,
        notification: NotificationPort,
    ):
        self.logger = logging.getLogger("app.workflows")
        self._extractor = extractor
        self._transformer = transformer
        self._loader_metadata = loader_metadata
        self._notification = notification
        self._poller = poller
        self.bank_guarantee_wf = WorkflowBankGuarantee(
            extractor=extractor,
            transformer=transformer,
            loader_metadata=loader_metadata,
            notification=notification,
        )
        self._graph = self._build()
        self.batch_size: int = 1
        self.strategy: Literal["dynamo", "bucket"] = "dynamo"
        self.app_settings: AppSettings = get_app_settings()

    def _start_task(self, state: EtlOrchestratorState) -> dict[str, Any]:
        try:
            if self.strategy == "bucket":
                bucket_name: str = self.app_settings.s3_settings.bucket
                prefix: str = "cartas_fmv/"
                documents_str: list[str] = self._poller.get_file_names(
                    bucket_name, prefix
                )
                documents: list[DocumentContractState] = [
                    WorkflowOrchestratorServiceDomain.transform_to_document_contract(d)
                    for d in documents_str
                ]
                return {"total_documents_to_process": documents}
            return {}
        except Exception as e:
            self.logger.error(e)
            return {}

    async def _run_etl(self, state: EtlOrchestratorState) -> dict[str, Any]:
        """
        Nodo principal:
        - Divide documentos en batches
        - Dentro de cada batch: lanza documentos en paralelo (limitados por semáforo)
        - Agrega métricas y resultados
        :param state:
        :return:
        """
        total_documents_to_process: list[DocumentContractState] = (
            state.total_documents_to_process
        )
        # Si no hay documentos por procesar; el ETL finaliza
        if len(total_documents_to_process) == 0:
            self.logger.warning("No hay documentos que procesar")
            return {"total_documents_processed": [], "total_documents_failed": []}
        sem = asyncio.Semaphore(self.batch_size)

        total_documents_processed: list[DocumentContractState] = []
        for idx, batch_docs in enumerate(
            wfa.create_batch(total_documents_to_process, self.batch_size)
        ):
            batch_result = await wfa.run_one_batch(
                batch_docs, sem, self.bank_guarantee_wf
            )
            total_documents_processed.extend(batch_result)

        return {
            "total_documents_processed": total_documents_processed,
        }

    def _final_task(self, state: EtlOrchestratorState) -> dict[str, Any]:
        self.logger.info("Finalizando ETL")
        print("State", state)

        metadata_notification_type = "regulatory-compliance-prompts.insert-metadata"
        notifications = [
            Notification(
                id=str(uuid.uuid4()),
                message=NotificationData(
                    session_id=result.session_id,
                    type=metadata_notification_type,
                    data={"recordId": result.record_id, "parentId": result.parent_id},
                ),
            )
            for result in state.total_documents_processed
        ]

        self._notification.notify(notifications)
        return {}

    def _build(self) -> CompiledStateGraph[EtlOrchestratorState]:
        g = StateGraph(state_schema=EtlOrchestratorState)
        g.add_node("start_task", self._start_task)
        g.add_node("run_etl", self._run_etl)
        g.add_node("final_task", self._final_task)

        g.add_edge(START, "start_task")
        g.add_edge("start_task", "run_etl")
        g.add_edge("run_etl", "final_task")
        g.add_edge("final_task", END)
        return g.compile()

    async def execute(self, documents: list[DocumentContractState]):
        print("documents", documents)
        state = EtlOrchestratorState(total_documents_to_process=documents)
        await self._graph.ainvoke(state)
