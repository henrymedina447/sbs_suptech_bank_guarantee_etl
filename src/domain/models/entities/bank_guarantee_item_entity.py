from pydantic import BaseModel, Field


class BankGuaranteeMetadata(BaseModel):
    letter_date: str | None = Field(description="Contiene la fecha en que se realizó la carta", default=None)
    disbursed_amount: str | None = Field(description="Contiene el monto desembolsado", default=None)
    reduced_amount: str | None = Field(description="Contiene el monto disminuido", default=None)
    total_amount: str | None = Field(description="Contiene el monto total", default=None)
    letter_text: str | None = Field(description="Contiene el número de la carta", default=None)
    project_text: str | None = Field(description="Contiene el nombre del proyecto", default=None)
    promotor: str | None = Field(
        description="Contiene el nombre del promotor que se entiende es el cliente",
        default=None
    )
    file_name: str = Field(description="Indica el nombre del archivo")
    type_document: str = Field(description="Indica el tipo de documento", default="carta fianza")
    period_month: str = Field(description="Contiene el mes de donde pertenece el archivo")
    period_year: str = Field(description="Contiene el año donde pertenece el archivo")


class BankGuaranteeItemEntity(BaseModel):
    id: str = Field(description="Bank guarantee item id")
    metadata: BankGuaranteeMetadata = Field(description="Contiene toda la metadata extraída del documento")
    supervisory_record_id: str = Field(description="Indica el id del archivo")
