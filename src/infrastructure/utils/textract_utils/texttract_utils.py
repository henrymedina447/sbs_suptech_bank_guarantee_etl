from typing import Sequence

from mypy_boto3_textract.literals import BlockTypeType
from mypy_boto3_textract.type_defs import GetDocumentAnalysisResponseTypeDef, BlockTypeDef, BoundingBoxTypeDef, \
    RelationshipTypeDef

from infrastructure.internal_models.build_tables_result import BuildTablesResult
from infrastructure.utils.textract_utils.textract_cells_utils import TextractCellsUtils


class TextractUtils:

    @staticmethod
    def group_by_page(
            response: list[GetDocumentAnalysisResponseTypeDef]
    ) -> tuple[list[BlockTypeDef], list[BlockTypeDef]]:
        """
          Agrupa los blocks por página y retorna (pages, blocks).
          """
        blocks: list[BlockTypeDef] = [b for r in response for b in r.get("Blocks", [])]
        pages: list[BlockTypeDef] = [p for p in blocks if p.get("BlockType") == "PAGE"]
        return pages, blocks

    @staticmethod
    def get_letter_block(results: list[BlockTypeDef]) -> BlockTypeDef | None:
        # Encontramos las coordenadas de las páginas
        patter_to_search: str = "carta n°"
        element: BlockTypeDef | None = next(
            (e for e in results if e["BlockType"] == "LINE" and patter_to_search in e.get("Text", "").lower()), None)
        return element

    @staticmethod
    def _check_condition(top: float, left: float, width: float, r: BlockTypeDef) -> bool:
        """
        Revisa todos los bloques candidatos que se encuentren por encima del bounding box de la carta;
        se descarta bloques de página y query ya qué son de carácter informativo
        :param top: Coordenada superior del bounding box de la carta
        :param left:Coordenada izquierda del bounding box de la carta
        :param width: Ancho total del bounding box
        :param r:Bloque que será auditado contra las coordenadas de bounding box de la carta
        :return:
        """
        if r["BlockType"] == "PAGE" or r["BlockType"] == "QUERY":
            return False
        if r["BlockType"] != "LINE":
            return False
        bounding_box = r["Geometry"]["BoundingBox"]
        condition_1: bool = bounding_box["Top"] < top
        condition_2: bool = bounding_box["Left"] < left + width
        return condition_1 and condition_2

    @staticmethod
    def _get_all_blocks_by_letter_position(
            letter_block: BlockTypeDef,
            results: list[BlockTypeDef]
    ) -> list[BlockTypeDef]:
        letter_bounding_box: BoundingBoxTypeDef | None = letter_block.get("Geometry", {}).get("BoundingBox", None)
        if letter_bounding_box is None:
            return []
        letter_bounding_box_top = letter_bounding_box["Top"]
        letter_bounding_box_left = letter_bounding_box["Left"]
        letter_bounding_box_width = letter_bounding_box["Width"]
        rx: list[BlockTypeDef] = [r for r in results if
                                  TextractUtils._check_condition(letter_bounding_box_top, letter_bounding_box_left,
                                                                 letter_bounding_box_width, r) and r[
                                      "BlockType"] != "PAGE"]
        return rx

    @staticmethod
    def get_date_by_letter_position(letter_block: BlockTypeDef, results: list[BlockTypeDef]) -> BlockTypeDef | None:
        """
        Determina la ubicación de la fecha en la carta basada en los candidatos que se encuentran
        en la parte superior a la palabra carat n.º; de los candidatos se selecciona el último, ya que el
        proceso OCR obtiene los resultados leyendo de arriba hacia abajo y de izquierda a derecha
        :param letter_block: Bloque que contiene el bounding box de la carta
        :param results: Lista total de bloques existentes que serán evaluados
        :return:
        """
        letter_bounding_box: BoundingBoxTypeDef | None = letter_block.get("Geometry", {}).get("BoundingBox", None)
        if letter_bounding_box is None:
            return None
        letter_bounding_box_top = letter_bounding_box["Top"]
        letter_bounding_box_left = letter_bounding_box["Left"]
        letter_bounding_box_width = letter_bounding_box["Width"]
        all_blocks_candidate = TextractUtils._get_all_blocks_by_letter_position(letter_block, results)
        # print(all_blocks_candidate[-1])
        rx: BlockTypeDef | None = next((r for r in reversed(all_blocks_candidate) if
                                        TextractUtils._check_condition(letter_bounding_box_top,
                                                                       letter_bounding_box_left,
                                                                       letter_bounding_box_width, r) and r[
                                            "BlockType"] != "PAGE"),
                                       None)
        return rx

    @staticmethod
    def get_promotor_by_query_result(results: list[BlockTypeDef]) -> BlockTypeDef | None:
        """
        Busca la respuesta de la consulta sobre el promotor; textract si encuentra la respuesta retorna un tipo
        de bloque QUERY_RESULT; caso contrario no lo devuelve; siendo la única pregunta no es necesario el filtro
        por alias
        :param results:
        :return:
        """
        promotor_block: BlockTypeDef | None = next((p for p in results if p["BlockType"] == "QUERY_RESULT"), None)
        return promotor_block

    @staticmethod
    def get_text_from_block(block: BlockTypeDef | None) -> str | None:
        if block is None:
            return None
        return block.get("Text", None)

    @staticmethod
    def get_query_response_block(alias: str, results: list[BlockTypeDef]) -> BlockTypeDef | None:
        # Obtenemos la query
        query_blocks: BlockTypeDef | None = next(
            (b for b in results if b["BlockType"] == "QUERY" and b["Query"]["Alias"] == alias), None)
        if query_blocks is None:
            return None
        id_to_search_block: list[str] = next(
            (r["Ids"] for r in query_blocks.get("Relationships", []) if r.get("Type") == "ANSWER"),
            []
        )
        if id_to_search_block is None:
            return None
        if len(id_to_search_block) == 0:
            return None
        block_found: BlockTypeDef | None = next((b for b in results if b["Id"] == id_to_search_block[0]), None)
        return block_found

    @staticmethod
    def _index_blocks_by_id(blocks: list[BlockTypeDef]) -> dict[str, BlockTypeDef]:
        """
        Cread un diccionario que tiene la estructura:
        {
        "Id": {*BlockTypeDef}
        }
        :param blocks: lista de bloques a transformar en diccionario por ID
        :return:
        """
        return {b["Id"]: b for b in blocks}

    @staticmethod
    def _get_children_ids(
            table_block: BlockTypeDef,
            block_by_id: dict[str, BlockTypeDef],
            types: tuple[str, ...]
    ) -> list[str]:
        """
        Obtiene todos los ID asociados a una tabla (children)
        :param table_block:El bloque de la tabla
        :param types: Que tipos son requeridos
        :param block_by_id: Todos los ids en formato de diccionario
        :return:
        """
        ids: list[str] = []
        # Obtenemos todas las relaciones que sean de tipo CHILD
        child_relations: list[RelationshipTypeDef] = [
            rel for rel in table_block.get("Relationships", []) or []
            if rel.get("Type") == "CHILD"
        ]
        # Obtenemos todos los ids en bruto
        ids.extend([rid for rel in child_relations for rid in rel.get("Ids", []) or []])
        # Filtramos por los tipos deseados
        # Por cada rid en ids obtenemos su valor en el diccionario general de ids y revisamos si el block type está
        # dentro de los tipos deseados
        return [rid for rid in ids if block_by_id.get(rid, {}).get("BlockType") in types]

    @staticmethod
    def _calculate_matrix_dimension(cell_blocks: list[BlockTypeDef]) -> tuple[int, int]:
        """
        Calcula la dimensión total de las filas y columnas de la tabla; una celda representa
        una unidad; sin embargo, si está fusionada ocupará más de una unidad sea en x o en y
        :param cell_blocks:
        :return:
        """
        max_row = 0
        max_col = 0
        for c in cell_blocks:
            cell_x_position = c.get("RowIndex", 1)  # Posición en X de la celda
            cell_y_position = c.get("ColumnIndex", 1)  # Posición en Y de la celda
            cell_y_units = c.get("RowSpan", 1)  # Cantidad de celdas en Y que ocupa (filas)
            cell_x_units = c.get("ColumnSpan", 1)  # Cantidad de celdas en X que ocupa (columnas)
            max_row = max(max_row, cell_x_position + cell_y_units - 1)
            max_col = max(max_col, cell_y_position + cell_x_units - 1)
        return max_row, max_col

    @staticmethod
    def _get_grid(
            cell_blocks: list[BlockTypeDef],
            blocks_dict_by_id: dict[str, BlockTypeDef],
            broadcast_spans: bool = False
    ) -> list[list[str]]:
        """
        Obtiene el contenido de la tabla en una matriz (lista de listas de strings).
        Cuando una celda no tiene contenido se rellena con un string vacío
        :param cell_blocks: Todos los bloques que pertenecen a la tabla
        :param blocks_dict_by_id:  El diccionario de todos los bloques por ID
        :param broadcast_spans: Indica la estrategia cuando existen celdas que ocupan más de un espacio
        en X o Y
        :return: La matriz de contenido de la tabla
        """
        max_row, max_col = TextractUtils._calculate_matrix_dimension(cell_blocks)
        grid: list[list[str | None]] = [[None for _ in range(max_col)] for _ in range(max_row)]
        for c in cell_blocks:
            cell_x_position_in_list = c.get("RowIndex", 1) - 1  # la posición en x de la celda en la lista
            cell_y_position_in_list = c.get("ColumnIndex", 1) - 1  # La posición en y de la celda en la lista
            cell_y_units = c.get("RowSpan", 1)  # Cantidad de celdas en Y (filas) que abarca
            cell_x_units = c.get("ColumnSpan", 1)  # Cantidad de celdas en X (columnas) que abarca
            text = TextractCellsUtils.extract_cell_text(c, blocks_dict_by_id)
            if cell_y_units == 1 and cell_x_units == 1:
                # Simple
                grid[cell_x_position_in_list][cell_y_position_in_list] = text
            else:
                # Spans
                if broadcast_spans:
                    for rr in range(cell_x_position_in_list, cell_x_position_in_list + cell_y_units):
                        for cc in range(cell_y_position_in_list, cell_y_position_in_list + cell_x_units):
                            grid[rr][cc] = text
                else:
                    # Solo en la esquina superior izquierda
                    grid[cell_x_position_in_list][cell_y_position_in_list] = text
        return [[cell if cell is not None else "" for cell in row] for row in grid]

    @staticmethod
    def build_tables_from_textract_blocks(
            blocks: list[BlockTypeDef],
            broadcast_spans: bool = False
    ) -> list[BuildTablesResult]:
        """
        Obtiene una lista de con todas las tablas reconocidas en textract; tal que el contenido se encuentra
        dentro del atributo grid
        :param blocks: Todos los bloques detectados por textract
        :param broadcast_spans: Indica la estrategia en caso de celdas fusionadas
        :return: Una lista de objetos que contienen el atributo grid con el contenido de la tabla
        """
        results: list[BuildTablesResult] = []
        # Agrupamos los bloques por id en un diccionario
        blocks_dict_by_id: dict[str, BlockTypeDef] = TextractUtils._index_blocks_by_id(blocks)
        # Agrupamos todas las tablas existentes
        tables: list[BlockTypeDef] = [b for b in blocks if b.get("BlockType") == "TABLE"]
        # Iniciamos la reconstrucción por cada tabla existente
        for table in tables:
            # Obtenemos la página a la cual pertenece la tabla
            page: int = table.get("Page", 1)
            # Obtenemos todos los ids de tipo CELDA asociados a la tabla
            cell_ids: list[str] = TextractUtils._get_children_ids(table, block_by_id=blocks_dict_by_id, types=("CELL",))
            # Obtenemos todos los bloques de las celdas
            cell_blocks: list[BlockTypeDef] = [blocks_dict_by_id[cid] for cid in cell_ids if cid in blocks_dict_by_id]
            # Calculamos la dimensión de la tabla (matrix)
            # Inicializamos la matriz de contenido
            # Estamos creando una tabla con solo None
            grid: list[list[str]] = TextractUtils._get_grid(cell_blocks, blocks_dict_by_id, broadcast_spans)
            aux = BuildTablesResult(
                page=page,
                table_block=table,
                grid=grid
            )
            results.append(aux)
        return results

    @staticmethod
    def get_text_for_promotor_and_date(
            promotor_block: BlockTypeDef | None,
            start_date_block: BlockTypeDef | None
    ) -> tuple[str | None, str | None]:
        """
        Retorna una tupla de promotor y fecha
        :param promotor_block:
        :param start_date_block:
        :return:
        """
        promotor_text: str | None = None
        start_date_text: str | None = None
        if promotor_block is not None:
            promotor_text = promotor_block["Text"]
        if start_date_block is not None:
            start_date_text = start_date_block["Text"]

        return promotor_text, start_date_text

    @staticmethod
    def filter_tables_keyword(tables: list[BuildTablesResult], blocks: list[BlockTypeDef]) -> list[BuildTablesResult]:
        blocks_dict_by_id: dict[str, BlockTypeDef] = TextractUtils._index_blocks_by_id(blocks)
        keywords: list[str] = ["ADENDA ACTUAL", "DESEMBOLSADOS"]
        return [t for t in tables if TextractCellsUtils.check_if_keyword_is_present(
            keywords,
            t.table_block,
            blocks_dict_by_id)
                ]
