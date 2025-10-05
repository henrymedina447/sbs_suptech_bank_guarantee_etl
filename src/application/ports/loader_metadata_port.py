from abc import ABC, abstractmethod

from domain.models.entities.bank_guarantee_item_entity import BankGuaranteeItemEntity


class LoaderMetadataPort(ABC):
    @abstractmethod
    def save_metadata(self, data: BankGuaranteeItemEntity) -> None:
        ...
