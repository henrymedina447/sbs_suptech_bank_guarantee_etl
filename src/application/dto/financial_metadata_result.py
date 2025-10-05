from pydantic import BaseModel, Field


class FinancialMetadataResult(BaseModel):
    disbursed_amount: float | None = Field(description="Contiene el monto desembolsado", default=None)
    reduced_amount: float | None = Field(description="Contiene el monto disminuido", default=None)
    total_amount: float | None = Field(description="Contiene el monto total", default=None)
