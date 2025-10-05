from pydantic import Field
from domain.models.states.etl_base_state import EtlBaseState


class EtlBankGuaranteeState(EtlBaseState):
    promotor: str | None = Field(
        description="Contiene el nombre del promotor que se entiende es el cliente",
        default=None
    )
    letter_date: str | None = Field(description="Contiene la fecha en que se realizó la carta", default=None)
    disbursed_amount: float | None = Field(description="Contiene el monto desembolsado", default=None)
    reduced_amount: float | None = Field(description="Contiene el monto disminuido", default=None)
    total_amount: float | None = Field(description="Contiene el monto total", default=None)
