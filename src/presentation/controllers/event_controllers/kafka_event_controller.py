import asyncio
import json
import logging
from typing import Any
from aiokafka import AIOKafkaConsumer
from pydantic import ValidationError
from domain.models.states.document_contract_state import DocumentContractState
from infrastructure.bootstrap.container import build_workflow
from infrastructure.config.app_settings import KafkaSettings, get_app_settings
from presentation.dtos.requests.process_document import ProcessDocumentRequest, ProcessDocument

app_logger = logging.getLogger("app.environment")


class KafkaEventController:
    def __init__(self, max_concurrency: int = 8):
        self._wf = build_workflow()
        self._consumer: AIOKafkaConsumer | None = None
        self._stopping = asyncio.Event()
        self._sem = asyncio.Semaphore(max_concurrency)

    @staticmethod
    def _get_kafka_config() -> dict[str, Any]:
        k: KafkaSettings = get_app_settings().kafka_settings
        cfg: dict[str, Any] = {
            "bootstrap_servers": k.bootstrap_servers,
            "group_id": k.group_id,
            "security_protocol": k.security_protocol,
        }
        return cfg

    @staticmethod
    def _get_kafka_topic() -> str:
        k: KafkaSettings = get_app_settings().kafka_settings
        return k.topic

    @staticmethod
    async def create_consumer() -> AIOKafkaConsumer:
        cfg = KafkaEventController._get_kafka_config()
        topic = KafkaEventController._get_kafka_topic()
        consumer = AIOKafkaConsumer(topic, **cfg)
        await consumer.start()
        return consumer

    async def start(self) -> None:
        self._consumer = await KafkaEventController.create_consumer()
        asyncio.create_task(self._loop())

    async def stop(self) -> None:
        self._stopping.set()
        if self._consumer:
            await self._consumer.stop()

    async def _loop(self) -> None:
        c = self._consumer
        try:
            while not self._stopping.is_set():
                batches = await c.getmany(timeout_ms=1000, max_records=10)
                tasks = []
                for _tp, msgs in batches.items():
                    document_requests: list[ProcessDocumentRequest] = []
                    for m in msgs:
                        text = m.value.decode("utf-8", errors="ignore")
                        # Por negocio se decidió que solo debe procesarse un documento a la vez
                        one_document_data = json.loads(text) if text else {}  # Esto representa un solo documento
                        print(one_document_data)
                        one_document: ProcessDocument = ProcessDocument.model_validate(one_document_data)
                        process_document = ProcessDocumentRequest(documents=[one_document])
                        document_requests.append(process_document)
                    tasks.append(
                        asyncio.create_task(
                            self._handle(document_requests=document_requests)
                        )
                    )
                if tasks:
                    await asyncio.gather(*tasks, return_exceptions=True)
        except Exception as e:
            print(f"error: {str(e)}")
        finally:
            await c.stop()

    async def _handle(self, document_requests: list[ProcessDocumentRequest]) -> None:

        app_logger.info(f"Procesando mensajes", document_requests)

        async with self._sem:
            try:
                documents_to_wf: list[DocumentContractState] = []
                for doc in document_requests:
                    for item in doc.documents:
                        document_contract_state = DocumentContractState(
                            record_id=item.record_id,
                            parent_id=item.parent_id,
                            key=item.key,
                            session_id=item.session_id,
                            document_type=item.document_type,
                            period_month=item.period_month,
                            period_year=item.period_year,
                        )
                        documents_to_wf.append(document_contract_state)
                await self._wf.execute(documents=documents_to_wf)
            except ValidationError as e:
                app_logger.exception(f"Error de validación: {str(e)}")
            except Exception as e:
                app_logger.exception(f"Error procesando mensaje kafka {str(e)}")
