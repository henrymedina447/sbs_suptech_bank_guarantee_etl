import re
from datetime import datetime
import uuid

from domain.models.entities.bank_guarantee_item_entity import BankGuaranteeItemEntity, BankGuaranteeMetadata
from domain.models.states.document_contract_state import DocumentContractState
from domain.models.states.etl_bank_guarantee_state import EtlBankGuaranteeState


class WorkflowBankGuaranteeServiceDomain:

    @staticmethod
    def _get_mont_and_date_regx() -> tuple[dict[str, int], re.Pattern[str]]:
        """
        Obtiene los regx necesarios para obtener los datos de fecha y mes
        :return: Un diccionario de meses y un regx de fechas
        """
        _MONTHS_ES = {
            "ene": 1, "enero": 1,
            "feb": 2, "febrero": 2,
            "mar": 3, "marzo": 3,
            "abr": 4, "abril": 4,
            "may": 5, "mayo": 5,
            "jun": 6, "junio": 6,
            "jul": 7, "julio": 7,
            "ago": 8, "agosto": 8,
            # septiembre tiene variantes en ES/LA
            "sep": 9, "sept": 9, "septiembre": 9, "set": 9, "setiembre": 9,
            "oct": 10, "octubre": 10,
            "nov": 11, "noviembre": 11,
            "dic": 12, "diciembre": 12,
        }
        # Día (1 o 2 dígitos) + opcional "de" + mes (abr./completo) + opcional "de" + año (4 dígitos)
        # admite puntos en abreviaturas (ej. "sept."), espacios múltiples, y texto alrededor.
        _DATE_RE = re.compile(
            r"(?P<d>\d{1,2})\s*(?:(?:de|del)\s+)?"
            r"(?P<m>ene(?:ro)?|feb(?:rero)?|mar(?:zo)?|abr(?:il)?|may(?:o)?|jun(?:io)?|jul(?:io)?|ago(?:sto)?|"
            r"sept(?:iembre)?|sep|set(?:iembre)?|oct(?:ubre)?|nov(?:iembre)?|dic(?:iembre)?)\.?\s*"
            r"(?:(?:de|del)\s+)?(?P<y>\d{4})",
            flags=re.IGNORECASE
        )
        return _MONTHS_ES, _DATE_RE

    @staticmethod
    def transform_date(raw_date: str) -> str | None:
        """
        Transforma la fecha a formato dd/mm/aaaa
        :param raw_date: Fecha en bruto; puede incluir texto
        :return:
        """
        _MONTHS_ES, _DATE_RE = WorkflowBankGuaranteeServiceDomain._get_mont_and_date_regx()
        match = _DATE_RE.search(raw_date)
        if not match:
            return None

        day = int(match.group("d"))
        month_key = match.group("m").lower().rstrip(".")
        # normaliza 'sept' y similares a claves del diccionario
        if month_key not in _MONTHS_ES:
            # Por si llegara algo raro tipo 'set.' o 'sept.'
            month_key = month_key.rstrip(".")
        month = _MONTHS_ES.get(month_key)
        year = int(match.group("y"))
        if not month:
            return None

        # valida la fecha (ej. 31/02 no pasa)
        try:
            dt = datetime(year, month, day)
        except ValueError:
            return None

        return dt.strftime("%d/%m/%Y")

    @staticmethod
    def transform_in_entity_to_dynamo(
            origin_doc: DocumentContractState,
            bank_guarantee_state: EtlBankGuaranteeState
    ) -> BankGuaranteeItemEntity:
        return BankGuaranteeItemEntity(
            id=str(uuid.uuid4()),
            file_name=origin_doc.key,
            period_year=bank_guarantee_state.period_year,
            period_month=bank_guarantee_state.period_month,
            supervisory_record_id=bank_guarantee_state.record_id,
            metadata=BankGuaranteeMetadata(
                letter_date=bank_guarantee_state.letter_date,
                disbursed_amount=str(bank_guarantee_state.disbursed_amount) if bank_guarantee_state.disbursed_amount else None,
                reduced_amount=str(bank_guarantee_state.reduced_amount) if bank_guarantee_state.reduced_amount else None,
                total_amount=str(bank_guarantee_state.total_amount) if bank_guarantee_state.total_amount else None,
                letter_text=bank_guarantee_state.letter_text,
                project_text=bank_guarantee_state.project_text,
                promotor=bank_guarantee_state.promotor
            )
        )
