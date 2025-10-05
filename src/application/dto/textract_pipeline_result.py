from pydantic import BaseModel, Field


class TextractPipelineResult(BaseModel):
    date_text: str = Field(description="Contiene la fecha en que se realizó la carta")
    promotor_text: str = Field(description="Contiene el nombre del promotor que se entiende es el cliente")
    grid: list[list[str]] = Field(description="Contiene la grilla del contenido de la tabla")
