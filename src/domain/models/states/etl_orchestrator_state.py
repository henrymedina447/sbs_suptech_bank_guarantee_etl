import operator
from pydantic import BaseModel, Field
from typing import Annotated

from domain.models.states.document_contract_state import DocumentContractState


class EtlOrchestratorState(BaseModel):
    total_documents_to_process: list[DocumentContractState] = Field(description="El total de "
                                                                                "documentos "
                                                                                "a procesar",
                                                                    default_factory=list)
    total_documents_processed: Annotated[list[DocumentContractState], operator.add] = Field(description="El total de "
                                                                                                        "documentos "
                                                                                                        "que se"
                                                                                                        "procesaron",
                                                                                            default_factory=list)
    total_documents_failed: Annotated[list[DocumentContractState], operator.add] = Field(description="El total de "
                                                                                                     "documentos que "
                                                                                                     "no se pudieron"
                                                                                                     "procesar",
                                                                                         default_factory=list)
