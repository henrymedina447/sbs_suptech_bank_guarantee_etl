from application.use_cases.workflows.workflow_orchestrator import WorkflowOrchestrator
from infrastructure.adapters.extractors.textract.textract_extractor_document import TextractExtractorDocument
from infrastructure.adapters.loaders.dynamo.dynamo_loader_document import DynamoLoaderDocument
from infrastructure.adapters.notificators.sqs_notification import SqsNotification
from infrastructure.adapters.poller.s3_poller_document import S3PollerDocument
from infrastructure.adapters.transformers.pandas.pandas_transformer_document import PandasTransformerDocument


def build_workflow() -> WorkflowOrchestrator:
    extractor = TextractExtractorDocument()
    poller = S3PollerDocument()
    transformer = PandasTransformerDocument()
    loader_metadata = DynamoLoaderDocument()
    notification = SqsNotification()
    return WorkflowOrchestrator(
        extractor=extractor,
        poller=poller,
        transformer=transformer,
        loader_metadata=loader_metadata,
        notification=notification
    )
