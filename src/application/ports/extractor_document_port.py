from abc import ABC, abstractmethod
from typing import Any

from application.dto.textract_pipeline_result import TextractPipelineResult


class ExtractorDocumentPort(ABC):

    @abstractmethod
    async def extract_pipeline(self, **kwargs) -> TextractPipelineResult | None:
        ...
