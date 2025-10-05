from mypy_boto3_textract.type_defs import BlockTypeDef
from pydantic import BaseModel, Field


class BuildTablesResult(BaseModel):
    page: int = Field(description="Página a la que pertenece la tabla")
    table_block: BlockTypeDef = Field(description="Contiene todos los bloques de la tabla")
    grid: list[list[str]] = Field(description="Contiene todo el texto extraído de la tabla")
