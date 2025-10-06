import logging
from typing import Any, Sequence
import boto3
import asyncio
from botocore.exceptions import ClientError
from mypy_boto3_textract.client import TextractClient
from mypy_boto3_textract.type_defs import StartDocumentAnalysisResponseTypeDef, QueryTypeDef, \
    GetDocumentAnalysisResponseTypeDef, BlockTypeDef

from application.dto.textract_pipeline_result import TextractPipelineResult
from application.ports.extractor_document_port import ExtractorDocumentPort
from domain.models.states.document_contract_state import DocumentContractState
from infrastructure.config.app_settings import get_app_settings, AppSettings
from infrastructure.internal_models.build_tables_result import BuildTablesResult
from infrastructure.utils.textract_utils.texttract_utils import TextractUtils

app_logger = logging.getLogger("app.workflows")


class TextractExtractorDocument(ExtractorDocumentPort):
    def __init__(self):
        self.app_settings: AppSettings = get_app_settings()
        self.logger = logging.getLogger("app.workflows")
        self.textract: TextractClient = boto3.client("textract", region_name=self.app_settings.aws_settings.region)

    async def extract_pipeline(self, document_data: DocumentContractState) -> TextractPipelineResult | None:
        """
        Obtiene la metadata requerida para las cartas fianza
        :param document_data: Información del documento obtenido de S3 o de dynamo
        :return: El nombre del promotor, la fecha en bruto de la carta y la grilla con el contenido de la tabla
        """
        # Obtención del job id
        try:
            job_id: str | None = await self._start_analysis(document_data.key)
            if job_id is None:
                return None
            # Obtención de todos los datos para el análisis
            results_from_analysis: list[GetDocumentAnalysisResponseTypeDef] = await self._get_analysis_result(
                job_id=job_id)
            pages, blocks = TextractUtils.group_by_page(response=results_from_analysis)
            letter_block: BlockTypeDef | None = TextractUtils.get_letter_block(blocks)
            date_block: BlockTypeDef | None = TextractUtils.get_date_by_letter_position(letter_block, blocks)
            promotor_block: BlockTypeDef | None = TextractUtils.get_query_response_block("Promotor", blocks)
            project_block: BlockTypeDef | None = TextractUtils.get_query_response_block("Project", blocks)
            #promotor_text, date_text = TextractUtils.get_text_for_promotor_and_date(promotor_block, date_block)
            letter_text: str | None = TextractUtils.get_text_from_block(letter_block)
            date_text: str | None = TextractUtils.get_text_from_block(date_block)
            promotor_text: str | None = TextractUtils.get_text_from_block(promotor_block)
            project_text: str | None = TextractUtils.get_text_from_block(project_block)
            # Obtención de la tabla
            tables: list[BuildTablesResult] = TextractUtils.build_tables_from_textract_blocks(blocks, False)
            tables_filtered: list[BuildTablesResult] = TextractUtils.filter_tables_keyword(tables, blocks)
            grid_selected: list[list[str]] = next((t.grid for t in tables_filtered if t.page == 1), [])
            return TextractPipelineResult(
                date_text=date_text,
                promotor_text=promotor_text,
                grid=grid_selected,
                letter_text=letter_text,
                project_text=project_text
            )
        except Exception as e:
            app_logger.error(f"Error en extract_pipeline: {str(e)}")
            return None

    async def _start_analysis(self, file_key: str) -> str | None:
        try:
            def _call() -> StartDocumentAnalysisResponseTypeDef:
                promotor_query: QueryTypeDef = {
                    "Text": "Who is the promotor?",
                    "Alias": "Promotor",
                    "Pages": ["1"]
                }
                project_query: QueryTypeDef = {
                    "Text": "What is the project name?",
                    "Alias": "Project",
                    "Pages": ["1"]
                }
                queries: Sequence[QueryTypeDef] = [
                    promotor_query,
                    project_query
                ]
                return self.textract.start_document_analysis(
                    DocumentLocation={
                        "S3Object": {
                            "Bucket": self.app_settings.s3_settings.bucket,
                            "Name": file_key
                        }
                    },
                    QueriesConfig={
                        "Queries": [*queries]
                    },
                    FeatureTypes=["TABLES", "QUERIES"],
                )

            resp = await asyncio.to_thread(_call)
            return resp.get("JobId", None)

        except ClientError as e:
            self.logger.error(f"Error en start_analysis: {str(e)}")
            return None

    async def _get_analysis_result(self, job_id: str) -> list[GetDocumentAnalysisResponseTypeDef]:
        """
        Polling asíncrono hasta que el Job termine, luego une el resultado
        :param job_id: el ID del procesamiento que realiza textract
        :return:
        """
        resp = await self._get_document_analysis_page(job_id)
        self.logger.info(f"Job status: {resp.get('JobStatus', 'sin status')}")
        while resp.get("JobStatus", "IN_PROGRESS") == "IN_PROGRESS":
            await asyncio.sleep(5)
            resp = await self._get_document_analysis_page(job_id)
            self.logger.info(f"Job status: {resp.get('JobStatus', 'sin status')}")
        all_responses: list[GetDocumentAnalysisResponseTypeDef] = [resp]
        while "NexToken" in all_responses[-1]:
            next_token = all_responses[-1]["NextToken"]  # type: ignore[index]
            next_page = await self._get_document_analysis_page(job_id, next_token)
            all_responses.append(next_page)
        return all_responses

    async def _get_document_analysis_page(self, job_id: str,
                                          next_token: str | None = None) -> GetDocumentAnalysisResponseTypeDef:
        """ Retorna una página de resultados (maneja nextToken)"""

        def _call() -> GetDocumentAnalysisResponseTypeDef:
            kwargs = {"JobId": job_id}
            if next_token is not None:
                kwargs["NextToken"] = next_token
            return self.textract.get_document_analysis(**kwargs)

        resp = await asyncio.to_thread(_call)
        return resp
