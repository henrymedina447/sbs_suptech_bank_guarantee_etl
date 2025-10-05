from mypy_boto3_textract.type_defs import BlockTypeDef, RelationshipTypeDef

from infrastructure.internal_models.build_tables_result import BuildTablesResult


class TextractCellsUtils:

    @staticmethod
    def extract_cell_text(cell_block: BlockTypeDef, block_by_id: dict[str, BlockTypeDef]):
        """
         Extrae el contenido de la celda de turno; una celda puede tener relaciones de tipo CHILD
        los cuales deben conformar el contenido completo requerido; por ello buscamos sus relaciones
        de tipo CHILD; obtenemos los IDs y los buscamos en el diccionario de IDs concatenando la información
        :param cell_block: Bloque de celda a obtener su contenido
        :param block_by_id: Todos los bloques agrupados por el ID
        :return: El texto de la celda
        """
        text_parts: list[str] = []
        # Obtenemos todas las relaciones que sean de tipo CHILD
        child_relations: list[RelationshipTypeDef] = [
            rel for rel in cell_block.get("Relationships", []) or []
            if rel.get("Type") == "CHILD"
        ]
        for rel in child_relations:
            for rid in rel.get("Ids", []) or []:
                child: BlockTypeDef | None = block_by_id.get(rid)
                if not child:
                    continue
                bt = child.get("BlockType")
                if bt == "WORD":
                    txt = child.get("Text", "")
                    if txt:
                        text_parts.append(txt)
        return " ".join(text_parts).strip()

    @staticmethod
    def check_if_keyword_is_present(
            keywords: list[str],
            table_block: BlockTypeDef,
            block_by_id: dict[str, BlockTypeDef]
    ) -> bool:
        """
        Evalúa si una lista de palabras clave (keywords) se encuentran dentro de al menos
        una de las celdas
        :param keywords: Palabras a buscar
        :param table_block: Tabla
        :param block_by_id: Contiene todos los bloques agrupados por el ID
        :return: True en caso exista al menos una de las palabras clave
        """
        # Obtenemos todas las relaciones que sean de tipo CHILD
        child_relations: list[RelationshipTypeDef] = [
            rel for rel in table_block.get("Relationships", []) or []
            if rel.get("Type") == "CHILD"
        ]
        for rel in child_relations:
            for rid in rel.get("Ids", []) or []:
                cell: BlockTypeDef | None = block_by_id.get(rid)
                if cell and cell.get("BlockType") == "CELL":
                    cell_text = TextractCellsUtils.extract_cell_text(cell, block_by_id)
                    for kw in keywords:
                        if kw.lower() in cell_text.lower():
                            return True

        return False
