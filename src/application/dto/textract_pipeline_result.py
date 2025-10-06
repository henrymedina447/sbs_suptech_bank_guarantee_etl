from pydantic import BaseModel, Field


class TextractPipelineResult(BaseModel):
    date_text: str | None = Field(description="Contiene la fecha en que se realizó la carta")
    promotor_text: str | None = Field(description="Contiene el nombre del promotor que se entiende es el cliente")
    letter_text: str | None = Field(description="Contiene el número de la carta")
    project_text: str | None = Field(description="Contiene el nombre del proyecto")
    grid: list[list[str]] = Field(description="Contiene la grilla del contenido de la tabla")
