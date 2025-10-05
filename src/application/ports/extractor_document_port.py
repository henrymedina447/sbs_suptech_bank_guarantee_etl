from abc import ABC, abstractmethod
from typing import Any


class ExtractorDocumentPort(ABC):

    @abstractmethod
    async def extract_pipeline(self, **kwargs) -> list[Any]:
        ...
