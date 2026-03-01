"""Data contracts for parsebox.

All modules communicate through these Pydantic models.
They serialize to/from JSON cleanly and carry no framework-specific state.
"""

import uuid
import logging
from datetime import datetime
from typing import Literal, Optional

from pydantic import BaseModel, Field, field_validator

logger = logging.getLogger(__name__)

VALID_FIELD_TYPES = {"string", "number", "boolean", "date", "array", "object"}


class ContentBlock(BaseModel):
    block_type: str
    content: str
    metadata: dict = Field(default_factory=dict)


class Document(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    filename: str
    file_type: str
    content: str
    content_blocks: list[ContentBlock] = Field(default_factory=list)
    metadata: dict = Field(default_factory=dict)
    raw_bytes: bytes = Field(default=b"", exclude=True)


class FieldDefinition(BaseModel):
    name: str
    type: str
    description: str = ""
    required: bool = False
    items: Optional["FieldDefinition"] = None
    properties: Optional[list["FieldDefinition"]] = None

    @field_validator("type")
    @classmethod
    def validate_type(cls, v: str) -> str:
        if v not in VALID_FIELD_TYPES:
            raise ValueError(
                f"Invalid field type '{v}'. Must be one of: {', '.join(sorted(VALID_FIELD_TYPES))}"
            )
        return v


class Schema(BaseModel):
    name: str
    description: str = ""
    fields: list[FieldDefinition] = Field(default_factory=list)
    version: int = 1


class ExtractionConfig(BaseModel):
    llm_provider: str = "openai/gpt-4o"
    api_key: str = ""
    instructions: str = ""
    temperature: float = 0.0
    max_retries: int = 3


class ExtractionSummary(BaseModel):
    total: int = 0
    succeeded: int = 0
    failed: int = 0


class Record(BaseModel):
    document_id: str
    document_filename: str
    data: dict = Field(default_factory=dict)
    status: Literal["success", "failed", "partial"] = "success"
    errors: list[str] = Field(default_factory=list)


class ExtractionResult(BaseModel):
    records: list[Record] = Field(default_factory=list)
    schema_: Optional[Schema] = Field(default=None, alias="schema")
    config: Optional[ExtractionConfig] = Field(default_factory=ExtractionConfig)
    summary: Optional[ExtractionSummary] = Field(default_factory=ExtractionSummary)

    model_config = {"populate_by_name": True}


class Dataset(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    name: str
    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: datetime = Field(default_factory=datetime.now)
    schema_: Optional[Schema] = Field(default=None, alias="schema")
    documents: list[Document] = Field(default_factory=list)
    extraction_config: Optional[ExtractionConfig] = None
    extraction_result: Optional[ExtractionResult] = None
    parquet_path: Optional[str] = None
    source_folder: Optional[str] = None
    status: Literal["draft", "schema_ready", "previewed", "extracted", "failed"] = "draft"
    metadata: dict = Field(default_factory=dict)

    model_config = {"populate_by_name": True}


class ColumnInfo(BaseModel):
    name: str
    type: str


class QueryRequest(BaseModel):
    dataset_id: str | list[str]
    mode: Literal["natural_language", "sql"]
    input: str
    conversation: list[dict] = Field(default_factory=list)


class QueryResult(BaseModel):
    columns: list[ColumnInfo] = Field(default_factory=list)
    rows: list[list] = Field(default_factory=list)
    total_rows: int = 0
    sql: str = ""
    clarifying_question: Optional[str] = None
