import boto3
from mypy_boto3_s3.client import S3Client

from application.ports.poller_document_port import PollerDocumentPort
from infrastructure.config.app_settings import AppSettings, get_app_settings


class S3PollerDocument(PollerDocumentPort):
    def __init__(self):
        self.app_settings: AppSettings = get_app_settings()
        self.s3_client: S3Client = boto3.client("s3", self.app_settings.aws_settings.region)

    def get_file_names(self, bucket_name: str, prefix_path: str, document_type: str = "pdf",
                       position: int | None = None) -> list[str]:
        paginator = self.s3_client.get_paginator('list_objects_v2')
        results: list[str] = []
        for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix_path):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if key.endswith("/"):
                    continue
                if not key.lower().endswith(f".{document_type.lower()}"):
                    continue
                results.append(key)
        if position is None:
            return results
        return [results[position]]
