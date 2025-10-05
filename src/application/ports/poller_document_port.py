from abc import ABC, abstractmethod


class PollerDocumentPort(ABC):
    @abstractmethod
    def get_file_names(self, bucket_name: str, prefix_path: str, document_type: str = "pdf",
                       position: int | None = None) -> list[str]:
        ...
