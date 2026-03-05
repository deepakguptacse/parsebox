"""Tests for parsebox data models."""

import json

import pytest

from parsebox.models import (
    ColumnInfo,
    ContentBlock,
    Dataset,
    Document,
    ExtractionConfig,
    ExtractionResult,
    ExtractionSummary,
    FieldDefinition,
    QueryRequest,
    QueryResult,
    Record,
    Schema,
)


class TestContentBlock:
    def test_create(self):
        block = ContentBlock(block_type="paragraph", content="Hello world")
        assert block.block_type == "paragraph"
        assert block.content == "Hello world"
        assert block.metadata == {}

    def test_with_metadata(self):
        block = ContentBlock(block_type="table", content="col1|col2", metadata={"page": 1})
        assert block.metadata["page"] == 1

    def test_json_roundtrip(self):
        block = ContentBlock(block_type="header", content="Title", metadata={"level": 1})
        data = json.loads(block.model_dump_json())
        restored = ContentBlock.model_validate(data)
        assert restored == block


class TestDocument:
    def test_create_with_defaults(self):
        doc = Document(filename="test.pdf", file_type="pdf", content="some text")
        assert doc.filename == "test.pdf"
        assert doc.file_type == "pdf"
        assert doc.content == "some text"
        assert doc.content_blocks == []
        assert doc.metadata == {}
        assert doc.raw_bytes == b""
        assert doc.id  # uuid generated

    def test_id_is_uuid(self):
        doc = Document(filename="a.txt", file_type="txt", content="")
        assert len(doc.id) == 36  # uuid4 string length

    def test_raw_bytes_excluded_from_json(self):
        doc = Document(filename="a.txt", file_type="txt", content="hello", raw_bytes=b"rawdata")
        data = doc.model_dump(mode="json")
        assert "raw_bytes" not in data

    def test_json_roundtrip(self):
        doc = Document(filename="test.md", file_type="md", content="# Title")
        data = json.loads(doc.model_dump_json())
        restored = Document.model_validate(data)
        assert restored.filename == doc.filename
        assert restored.id == doc.id


class TestFieldDefinition:
    def test_valid_types(self):
        for t in ("string", "number", "boolean", "date", "array", "object"):
            fd = FieldDefinition(name="f", type=t)
            assert fd.type == t

    def test_invalid_type_raises(self):
        with pytest.raises(ValueError, match="Invalid field type"):
            FieldDefinition(name="f", type="integer")

    def test_nested_items(self):
        fd = FieldDefinition(
            name="tags",
            type="array",
            items=FieldDefinition(name="tag", type="string"),
        )
        assert fd.items.name == "tag"

    def test_nested_properties(self):
        fd = FieldDefinition(
            name="address",
            type="object",
            properties=[
                FieldDefinition(name="street", type="string"),
                FieldDefinition(name="zip", type="string"),
            ],
        )
        assert len(fd.properties) == 2

    def test_json_roundtrip(self):
        fd = FieldDefinition(name="amount", type="number", required=True, description="Total")
        data = json.loads(fd.model_dump_json())
        restored = FieldDefinition.model_validate(data)
        assert restored == fd


class TestSchema:
    def test_create(self):
        s = Schema(name="invoice_data")
        assert s.name == "invoice_data"
        assert s.description == ""
        assert s.fields == []
        assert s.version == 1

    def test_with_fields(self):
        s = Schema(
            name="test",
            fields=[FieldDefinition(name="name", type="string", required=True)],
        )
        assert len(s.fields) == 1

    def test_json_roundtrip(self):
        s = Schema(
            name="test",
            description="A test schema",
            fields=[FieldDefinition(name="x", type="number")],
            version=3,
        )
        data = json.loads(s.model_dump_json())
        restored = Schema.model_validate(data)
        assert restored == s


class TestExtractionConfig:
    def test_defaults(self):
        cfg = ExtractionConfig()
        assert cfg.llm_provider == "openai/gpt-4o"
        assert cfg.api_key == ""
        assert cfg.instructions == ""
        assert cfg.temperature == 0.0
        assert cfg.max_retries == 3

    def test_custom_values(self):
        cfg = ExtractionConfig(llm_provider="anthropic/claude-sonnet-4-20250514", api_key="sk-test")
        assert cfg.llm_provider == "anthropic/claude-sonnet-4-20250514"
        assert cfg.api_key == "sk-test"


class TestExtractionSummary:
    def test_defaults(self):
        s = ExtractionSummary()
        assert s.total == 0
        assert s.succeeded == 0
        assert s.failed == 0


class TestRecord:
    def test_create(self):
        r = Record(document_id="abc", document_filename="test.pdf")
        assert r.document_id == "abc"
        assert r.data == {}
        assert r.status == "success"
        assert r.errors == []

    def test_failed_record(self):
        r = Record(
            document_id="abc",
            document_filename="test.pdf",
            status="failed",
            errors=["LLM timeout"],
        )
        assert r.status == "failed"
        assert len(r.errors) == 1


class TestExtractionResult:
    def test_defaults(self):
        er = ExtractionResult()
        assert er.records == []
        assert er.config is not None
        assert er.summary is not None

    def test_with_schema_alias(self):
        s = Schema(name="test")
        er = ExtractionResult(schema=s)
        assert er.schema_ == s

    def test_json_roundtrip(self):
        er = ExtractionResult(
            records=[Record(document_id="1", document_filename="a.txt", data={"x": 1})],
            schema=Schema(name="s"),
            summary=ExtractionSummary(total=1, succeeded=1),
        )
        data = json.loads(er.model_dump_json(by_alias=True))
        restored = ExtractionResult.model_validate(data)
        assert len(restored.records) == 1
        assert restored.schema_.name == "s"


class TestDataset:
    def test_create_with_defaults(self):
        ds = Dataset(name="Test Dataset")
        assert ds.name == "Test Dataset"
        assert ds.status == "draft"
        assert ds.documents == []
        assert ds.schema_ is None
        assert ds.id
        assert ds.created_at
        assert ds.updated_at

    def test_with_schema_alias(self):
        s = Schema(name="test")
        ds = Dataset(name="ds", schema=s)
        assert ds.schema_ == s

    def test_json_roundtrip(self):
        ds = Dataset(name="round trip test")
        data = json.loads(ds.model_dump_json(by_alias=True))
        restored = Dataset.model_validate(data)
        assert restored.name == ds.name
        assert restored.id == ds.id

    def test_status_literal(self):
        for status in ("draft", "schema_ready", "previewed", "extracted", "failed"):
            ds = Dataset(name="t", status=status)
            assert ds.status == status


class TestColumnInfo:
    def test_create(self):
        c = ColumnInfo(name="amount", type="float64")
        assert c.name == "amount"
        assert c.type == "float64"


class TestQueryRequest:
    def test_single_dataset_id(self):
        qr = QueryRequest(dataset_id="abc", mode="sql", input="SELECT * FROM t")
        assert qr.dataset_id == "abc"

    def test_multiple_dataset_ids(self):
        qr = QueryRequest(dataset_id=["a", "b"], mode="natural_language", input="show totals")
        assert qr.dataset_id == ["a", "b"]

    def test_conversation_default(self):
        qr = QueryRequest(dataset_id="x", mode="sql", input="SELECT 1")
        assert qr.conversation == []


class TestQueryResult:
    def test_defaults(self):
        qr = QueryResult()
        assert qr.columns == []
        assert qr.rows == []
        assert qr.total_rows == 0
        assert qr.sql == ""
        assert qr.clarifying_question is None

    def test_with_data(self):
        qr = QueryResult(
            columns=[ColumnInfo(name="x", type="int")],
            rows=[[1], [2]],
            total_rows=2,
            sql="SELECT x FROM t",
        )
        assert len(qr.rows) == 2
