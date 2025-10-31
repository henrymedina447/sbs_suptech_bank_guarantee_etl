import boto3
from botocore.config import Config
from boto3.dynamodb.conditions import Key, Attr
from mypy_boto3_dynamodb.service_resource import DynamoDBServiceResource
from mypy_boto3_dynamodb.service_resource import Table
from mypy_boto3_dynamodb.type_defs import QueryOutputTableTypeDef

from application.ports.loader_metadata_port import LoaderMetadataPort
from domain.models.entities.bank_guarantee_item_entity import BankGuaranteeItemEntity
from domain.models.states.etl_bank_guarantee_state import EtlBankGuaranteeState
from infrastructure.config.app_settings import get_app_settings, AppSettings
from typing import Any


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

    def save_metadata(self, data: BankGuaranteeItemEntity) -> None:
        raw_data = data.model_dump(mode="json", exclude_none=True)
        query_output: QueryOutputTableTypeDef = self.si_table.query(
            KeyConditionExpression=Key("supervisoryRecordId").eq(
                data.supervisory_record_id
            ),
            IndexName="supervisoryRecordId-index",
            Limit=1,
        )
        existing_metadata: dict[str, Any] = (query_output.get("Items") or [{}])[0]

        metadata = raw_data["metadata"]
        if existing_metadata.get("metadata"):
            metadata.update(existing_metadata["metadata"])

        self.si_table.update_item(
            Key={
                "id": existing_metadata["id"],
            },
            UpdateExpression="set metadata = :metadata",
            ExpressionAttributeValues={
                ":metadata": metadata,
            },
            ReturnValues="UPDATED_NEW",
        )
