import boto3
from botocore.config import Config
from mypy_boto3_dynamodb.service_resource import DynamoDBServiceResource
from mypy_boto3_dynamodb.service_resource import Table

from application.ports.loader_metadata_port import LoaderMetadataPort
from domain.models.states.etl_bank_guarantee_state import EtlBankGuaranteeState
from infrastructure.config.app_settings import get_app_settings, AppSettings


class DynamoLoaderDocument(LoaderMetadataPort):
    def __init__(self):
        self.app_settings: AppSettings = get_app_settings()
        dynamo_resource = self._get_configuration()
        self.si_table: Table = dynamo_resource.Table(
            self.app_settings.table_settings.si_table
        )

    def _get_configuration(self) -> DynamoDBServiceResource:
        _cfg = Config(
            retries={"max_attempts": 10, "mode": "standard"},
            connect_timeout=3,
            read_timeout=5,
        )
        return boto3.resource(
            "dynamodb", config=_cfg, region_name=self.app_settings.aws_settings.region
        )

    def save_metadata(self, data: EtlBankGuaranteeState) -> None:
        raw_data = data.model_dump(mode="json", exclude_none=True)
        self.si_table.put_item(Item=raw_data)
