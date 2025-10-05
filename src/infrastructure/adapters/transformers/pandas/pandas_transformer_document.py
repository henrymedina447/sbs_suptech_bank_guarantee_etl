import logging
from typing import Optional
from IPython.display import display
import pandas as pd
from application.dto.financial_metadata_result import FinancialMetadataResult
from application.ports.transform_document_port import TransformDocumentPort
from infrastructure.utils.pandas_utils.pandas_utils import PandasUtils


class PandasTransformerDocument(TransformDocumentPort):
    def __init__(self):
        self.logger = logging.getLogger("app.pandas")

    def get_financial_metadata(self, grid: list[list[str]]) -> FinancialMetadataResult | None:
        raw_df: Optional[pd.DataFrame] = self._df_from_grid_with_two_header_rows(grid)
        df: Optional[pd.DataFrame] = self._filter_df_by_columns(raw_df)
        # display(df) # ver la tabla para debug
        df_normalized: Optional[pd.DataFrame] = self._normalize_df(df)
        return self._get_metadata(df_normalized)


    def _get_metadata(self, df_normalized: Optional[pd.DataFrame]) -> FinancialMetadataResult | None:
        """
        Obtiene la metadata acorde al modelo requerido; en caso de ser nulo el df se retorna un None como
        respuesta
        :param df_normalized:
        :return:
        """
        try:
            if df_normalized is None:
                raise ValueError("df normalized es Nulo ")
            df_transformed: pd.DataFrame = PandasUtils.transform_df_values_to_number(df_normalized)
            return PandasUtils.get_metadata_from_df(df_transformed)
        except Exception as e:
            self.logger.error(f"Error en get_metadata: {str(e)}")
            return None

    def _df_from_grid_with_two_header_rows(self, grid: list[list[str]]) -> Optional[pd.DataFrame]:
        """
        Transforma la grid en un dataframe de pandas
        :param grid: Lista de listas de strings que representa el contenido de la tabla
        :return:
        """
        try:
            if len(grid) < 3:
                raise ValueError("Se necesitan al menos 2 filas (header y datos)")
            # Segunda fila como encabezado
            header = list(grid[1])
            # Datos desde la tercera fila
            data_rows = grid[2:]
            # Normaliza ancho de cada fila al número de columnas
            n_cols = len(header)
            data_rows: list[list[str]] = [PandasUtils.normalize_len(r, n_cols) for r in data_rows]
            df = pd.DataFrame(data_rows, columns=header)
            return df
        except Exception as e:
            self.logger.error(f"Error en df_from_grid_with_two_header_rows: {str(e)}")
            return None

    def _filter_df_by_columns(self, raw_df: Optional[pd.DataFrame]) -> Optional[pd.DataFrame]:
        """
        Filtra un dataframe sobre columnas de interés y elimina columnas que no requieres
        :param raw_df: Dataframe de pandas a filtrar
        :return:
        """
        try:
            if raw_df is None:
                raise ValueError("El dataframe de entrada es nulo")
            keywords: list[str] = ["monto", "importe"]
            columns_to_drop: list[str] = ["N°"]
            df = PandasUtils.clean_df(keywords, columns_to_drop, raw_df)
            return df
        except Exception as e:
            self.logger.error(f"Error en df_from_grid_with_two_header_rows: {str(e)}")
            return None

    def _normalize_df(self, df: Optional[pd.DataFrame]) -> Optional[pd.DataFrame]:
        """
        Normaliza el dataframe; renombrando las columnas a disbursed_amount, reduced_amount y amount;
        luego de ello convierte todos los valores del df en numérico.
        :param df: DataFrame original
        :return:
        """
        try:
            df_columns_renamed = PandasUtils.transform_df_rename_columns(
                ["disbursed_amount", "reduced_amount", "total_amount"],
                df
            )
            df_values_to_numbers = PandasUtils.transform_df_values_to_number(df_columns_renamed)
            return df_values_to_numbers

        except Exception as e:
            self.logger.error(f"Error en normalize_df: {str(e)}")
            return None
