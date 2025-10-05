import re
from typing import Optional
import pandas as pd

from pandas import DataFrame

from application.dto.financial_metadata_result import FinancialMetadataResult


class PandasUtils:

    @staticmethod
    def normalize_len(row_data: list[str], n_cols: int) -> list[str]:
        row = list(row_data)
        if len(row) < n_cols:
            row += [""] * (n_cols - len(row))
        elif len(row) > n_cols:
            row = row[:n_cols]
        return row

    @staticmethod
    def clean_df(keywords: list[str], drop_columns: list[str], df: DataFrame) -> DataFrame:
        # 1) eliminar "N°" si existe (REASIGNANDO)
        df = df.drop(columns=["N°", *drop_columns], errors="ignore")

        # 2) columnas de interés
        cols = [c for c in df.columns if any(k in c.lower() for k in keywords)]
        sub = df.loc[:, cols].copy()

        # 3) normalizar strings por columna (iteración por posición, no por nombre)
        sub = sub.apply(lambda s: s.astype(str).str.strip() if s.dtype == "object" else s)

        # 4) quitar columnas duplicadas por CONTENIDO (deja solo una por cada serie idéntica)
        sub = sub.T.drop_duplicates().T

        return sub

    @staticmethod
    def transform_df_rename_columns(name_columns: list[str], df: Optional[DataFrame]) -> DataFrame:
        if df is None:
            raise ValueError("El Dataframe es nulo")
        if df is not None and df.shape[1] >3:
            raise ValueError(f"Se esperaban 3 columnas, pero se encontraron {df.shape[1]}: {list(df.columns)}")
        if df.shape[1] == 3:
            df.columns = [*name_columns]
        return df

    @staticmethod
    def _to_float_from_str(x: str) -> float | None:
        if pd.isna(x):
            return None
        s = str(x).strip()

        # quitar prefijos comunes: S/, S/., S, SI, etc.
        s = re.sub(r"^(S\/\.?|SI|S)\s*", "", s, flags=re.IGNORECASE)

        # detectar negativo por paréntesis
        neg = s.startswith("(") and s.endswith(")")
        if neg:
            s = s[1:-1]

        # quitar todo lo que no sea dígito, coma, punto o signo
        s = re.sub(r"[^0-9,.\-]", "", s)

        if not re.search(r"\d", s):
            return None

        # lógica separadores
        if "," in s and "." in s:
            if s.rfind(".") > s.rfind(","):  # 1,234.56 → '.' decimal
                s = s.replace(",", "")
            else:  # 1.234,56 → ',' decimal
                s = s.replace(".", "").replace(",", ".")
        elif "," in s:
            parts = s.split(",")
            if len(parts) == 2 and len(parts[1]) in (1, 2):
                s = s.replace(",", ".")  # decimal
            else:
                s = s.replace(",", "")  # miles
        # else: solo '.', Python ya lo entiende como decimal

        try:
            val = float(s)
            return -val if neg else val
        except ValueError:
            return None

    @staticmethod
    def transform_df_values_to_number(df: DataFrame) -> DataFrame:
        out = df.copy()
        for c in out.columns:
            out[c] = out[c].apply(PandasUtils._to_float_from_str)
        return out

    @staticmethod
    def get_metadata_from_df(df_transformed: DataFrame) -> FinancialMetadataResult:
        """
        Transforma la primera fila del dataframe al modelo requerido
        :param df_transformed:  Data frame con valores numéricos
        :return:
        """
        first_row = df_transformed.iloc[0].to_dict()
        return FinancialMetadataResult(**first_row)

