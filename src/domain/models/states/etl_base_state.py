from pydantic import BaseModel, Field


class EtlBaseState(BaseModel):
    record_id: str = Field(description="ID del documento")
    period_month: str | None = Field(description="Contiene el mes de donde pertenece el archivo", default=None)
    period_year: str | None = Field(description="Contiene el año donde pertenece el archivo", default=None)
    extract_success: bool | None = Field(description="Indica si su procesamiento fue exitoso o no", default=None)
    transform_success: bool | None = Field(description="Indica si su procesamiento fue exitoso o no", default=None)
    load_success: bool | None = Field(description="Indica si su procesamiento fue exitoso o no", default=None)
