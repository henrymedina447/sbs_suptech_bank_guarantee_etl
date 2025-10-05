from application.ports.loader_metadata_port import LoaderMetadataPort
from domain.models.states.etl_base_state import EtlBaseState


class DynamoLoaderDocument(LoaderMetadataPort):

    def save_metadata(self, document_type: str, data: list[EtlBaseState]) -> None:
        pass
