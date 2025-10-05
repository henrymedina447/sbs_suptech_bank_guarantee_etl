import logging
from fastapi import FastAPI, Depends
from pydantic import ValidationError
from application.use_cases.workflows.workflow_orchestrator import WorkflowOrchestrator
from domain.models.states.document_contract_state import DocumentContractState
from infrastructure.bootstrap.container import build_workflow
from presentation.dtos.requests.process_document import (
    ProcessDocumentRequest,
)

app = FastAPI(title="SBS ETL API")

app_logger = logging.getLogger("app.environment")


def get_factory() -> WorkflowOrchestrator:
    return build_workflow()


@app.post("/start-etl")
async def run_etl(
        process_document: ProcessDocumentRequest,
        wf: WorkflowOrchestrator = Depends(get_factory),
):
    try:
        documents = [DocumentContractState(**d.model_dump()) for d in process_document.documents]

        for document in documents:
            app_logger.info(f"Ejecutando flow de: {document}")
            await wf.execute(documents=documents)
        return {"status": "success"}
    except ValidationError as e:
        return {"status": "error", "message": "Error de campos al validar el body del request"}
    except Exception as e:
        return {"status": "error", "message": str(e)}


@app.get("/start-etl")
async def run_etl_local(
        wf: WorkflowOrchestrator = Depends(get_factory),
):
    try:
        await wf.execute([])
        return {"status": "success"}
    except Exception as e:
        return {"status": "error", "message": str(e)}
