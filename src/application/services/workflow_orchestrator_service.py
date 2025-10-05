import asyncio
import logging
from asyncio import Task
from typing import Iterable, Any

from application.use_cases.workflows.workflow_bank_guarantee import WorkflowBankGuarantee
from domain.enums.document_status import DocumentStatus
from domain.models.states.document_contract_state import DocumentContractState

app_logger = logging.getLogger("app.workflows")


class WorkflowOrchestratorServiceApplication:

    @staticmethod
    def create_batch(
            total_elements: list[DocumentContractState],
            batch_size: int
    ) -> Iterable[list[DocumentContractState]]:
        """
        Crea un iterador para generar batches de procesamiento
        :param total_elements: Total de elementos que se deben recorrer
        :param batch_size: Tamaño del lote por el cual se segmenta
        :return:
        """
        for i in range(0, len(total_elements), batch_size):
            yield total_elements[i:i + batch_size]

    @staticmethod
    async def run_one_batch(
            batch: list[DocumentContractState],
            sem: asyncio.Semaphore,
            wf: WorkflowBankGuarantee
    ) -> list[DocumentContractState]:
        """
        Ejecuta un batch completo en paralelo (por documento)
        :param wf: Workflow de garantías
        :param batch: Cantidad de documentos a procesar
        :param sem: Cantidad de documentos permitidos trabajar por batch
        :return:
        """
        tasks: list[Task[DocumentContractState]] = [
            asyncio.create_task(WorkflowOrchestratorServiceApplication.process_one_document(b, sem, wf)) for b in
            batch]
        results: list[DocumentContractState] = await asyncio.gather(*tasks, return_exceptions=False)
        return results

    @staticmethod
    async def process_one_document(doc: DocumentContractState, sem: asyncio.Semaphore,
                                   wf: WorkflowBankGuarantee) -> DocumentContractState:
        async with sem:
            print("doc", doc)
            try:

                new_doc: DocumentContractState = await wf.execute(doc)
                return new_doc
            except Exception as e:
                app_logger.info(f"Error en process_one_document: {str(e)}")
                doc.status = DocumentStatus.FAILED
                return doc
