"""Tests for the extract module: prompt builder, extraction engine, sample, and batch."""

from unittest.mock import MagicMock, patch

import pytest

from parsebox.extract.engine import extract_batch, extract_sample
from parsebox.extract.prompt import _format_field, _format_schema, build_extraction_prompt
from parsebox.extract.strategies.default import _schema_to_pydantic_fields, extract_single
from parsebox.models import (
    Document,
    ExtractionConfig,
    FieldDefinition,
    Record,
    Schema,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def simple_schema():
    return Schema(
        name="invoice",
        description="Invoice data",
        fields=[
            FieldDefinition(name="vendor", type="string", required=True, description="Vendor name"),
            FieldDefinition(name="amount", type="number", required=True, description="Total amount"),
            FieldDefinition(name="paid", type="boolean"),
        ],
    )


@pytest.fixture
def nested_schema():
    return Schema(
        name="order",
        description="Order data",
        fields=[
            FieldDefinition(name="customer", type="string", required=True),
            FieldDefinition(
                name="items",
                type="array",
                items=FieldDefinition(name="item", type="object", properties=[
                    FieldDefinition(name="name", type="string"),
                    FieldDefinition(name="qty", type="number"),
                ]),
            ),
            FieldDefinition(
                name="address",
                type="object",
                properties=[
                    FieldDefinition(name="street", type="string"),
                    FieldDefinition(name="city", type="string"),
                ],
            ),
        ],
    )


@pytest.fixture
def sample_document():
    return Document(
        id="doc-1",
        filename="invoice_001.pdf",
        file_type="pdf",
        content="Invoice from Acme Corp. Total: $500.00. Paid: Yes.",
    )


@pytest.fixture
def sample_documents():
    return [
        Document(
            id=f"doc-{i}",
            filename=f"invoice_{i:03d}.pdf",
            file_type="pdf",
            content=f"Invoice #{i} from Vendor{i}. Total: ${i * 100}.00",
        )
        for i in range(1, 8)
    ]


@pytest.fixture
def extraction_config():
    return ExtractionConfig(
        llm_provider="openai/gpt-4o",
        api_key="test-key",
        instructions="",
        temperature=0.0,
        max_retries=2,
    )


# ---------------------------------------------------------------------------
# Prompt builder tests
# ---------------------------------------------------------------------------

class TestBuildExtractionPrompt:
    def test_returns_system_and_user_keys(self, simple_schema):
        result = build_extraction_prompt("some content", simple_schema)
        assert "system" in result
        assert "user" in result

    def test_system_contains_schema_name(self, simple_schema):
        result = build_extraction_prompt("content", simple_schema)
        assert "invoice" in result["system"]

    def test_system_contains_field_descriptions(self, simple_schema):
        result = build_extraction_prompt("content", simple_schema)
        assert "vendor" in result["system"]
        assert "amount" in result["system"]
        assert "paid" in result["system"]
        assert "Vendor name" in result["system"]
        assert "Total amount" in result["system"]

    def test_user_contains_document_content(self, simple_schema):
        result = build_extraction_prompt("My document text here", simple_schema)
        assert "My document text here" in result["user"]

    def test_instructions_included_when_provided(self, simple_schema):
        result = build_extraction_prompt("content", simple_schema, instructions="Use ISO dates")
        assert "Use ISO dates" in result["system"]
        assert "Additional instructions from the user" in result["system"]

    def test_instructions_not_included_when_empty(self, simple_schema):
        result = build_extraction_prompt("content", simple_schema, instructions="")
        assert "Additional instructions from the user" not in result["system"]

    def test_required_fields_marked(self, simple_schema):
        result = build_extraction_prompt("content", simple_schema)
        assert "(REQUIRED)" in result["system"]

    def test_schema_description_included(self, simple_schema):
        result = build_extraction_prompt("content", simple_schema)
        assert "Invoice data" in result["system"]


class TestFormatSchema:
    def test_formats_all_fields(self, simple_schema):
        output = _format_schema(simple_schema)
        assert "vendor" in output
        assert "amount" in output
        assert "paid" in output

    def test_nested_array_items_formatted(self, nested_schema):
        output = _format_schema(nested_schema)
        assert "items" in output
        assert "name" in output
        assert "qty" in output

    def test_nested_object_properties_formatted(self, nested_schema):
        output = _format_schema(nested_schema)
        assert "address" in output
        assert "street" in output
        assert "city" in output


class TestFormatField:
    def test_simple_field(self):
        field = FieldDefinition(name="title", type="string", description="The title")
        result = _format_field(field, indent=0)
        assert "- title: string" in result
        assert "-- The title" in result

    def test_required_field(self):
        field = FieldDefinition(name="id", type="string", required=True)
        result = _format_field(field, indent=0)
        assert "(REQUIRED)" in result

    def test_indented_field(self):
        field = FieldDefinition(name="sub", type="string")
        result = _format_field(field, indent=2)
        assert result.startswith("    ")

    def test_field_with_items(self):
        field = FieldDefinition(
            name="tags",
            type="array",
            items=FieldDefinition(name="tag", type="string"),
        )
        result = _format_field(field, indent=0)
        assert "tags" in result
        assert "tag" in result

    def test_field_with_properties(self):
        field = FieldDefinition(
            name="meta",
            type="object",
            properties=[
                FieldDefinition(name="key", type="string"),
                FieldDefinition(name="value", type="string"),
            ],
        )
        result = _format_field(field, indent=0)
        assert "key" in result
        assert "value" in result


# ---------------------------------------------------------------------------
# Schema to Pydantic fields tests
# ---------------------------------------------------------------------------

class TestSchemaToPydanticFields:
    def test_string_field(self):
        fields = [FieldDefinition(name="name", type="string", required=True)]
        result = _schema_to_pydantic_fields(fields)
        assert "name" in result
        assert result["name"] == (str, ...)

    def test_number_field(self):
        fields = [FieldDefinition(name="amount", type="number")]
        result = _schema_to_pydantic_fields(fields)
        assert result["amount"] == (float | None, None)

    def test_boolean_field(self):
        fields = [FieldDefinition(name="active", type="boolean", required=True)]
        result = _schema_to_pydantic_fields(fields)
        assert result["active"] == (bool, ...)

    def test_date_field(self):
        fields = [FieldDefinition(name="created", type="date")]
        result = _schema_to_pydantic_fields(fields)
        assert result["created"] == (str | None, None)

    def test_array_field(self):
        fields = [FieldDefinition(name="tags", type="array")]
        result = _schema_to_pydantic_fields(fields)
        assert result["tags"] == (list | None, None)

    def test_object_field(self):
        fields = [FieldDefinition(name="meta", type="object")]
        result = _schema_to_pydantic_fields(fields)
        assert result["meta"] == (dict | None, None)


# ---------------------------------------------------------------------------
# extract_single tests (mocked LLM)
# ---------------------------------------------------------------------------

class TestExtractSingle:
    def test_success(self, sample_document, simple_schema, extraction_config):
        mock_result = MagicMock()
        mock_result.model_dump.return_value = {
            "vendor": "Acme Corp",
            "amount": 500.0,
            "paid": True,
        }

        mock_client = MagicMock()
        mock_client.chat.completions.create.return_value = mock_result

        with patch("parsebox.extract.strategies.default.instructor") as mock_instructor:
            mock_instructor.from_litellm.return_value = mock_client
            record = extract_single(sample_document, simple_schema, extraction_config)

        assert record.status == "success"
        assert record.document_id == "doc-1"
        assert record.document_filename == "invoice_001.pdf"
        assert record.data["vendor"] == "Acme Corp"
        assert record.data["amount"] == 500.0
        assert record.data["paid"] is True
        assert record.errors == []

    def test_failure_returns_failed_record(self, sample_document, simple_schema, extraction_config):
        with patch("parsebox.extract.strategies.default.instructor") as mock_instructor:
            mock_instructor.from_litellm.side_effect = RuntimeError("LLM unavailable")
            record = extract_single(sample_document, simple_schema, extraction_config)

        assert record.status == "failed"
        assert record.document_id == "doc-1"
        assert record.data == {}
        assert len(record.errors) == 1
        assert "LLM unavailable" in record.errors[0]

    def test_llm_called_with_correct_params(self, sample_document, simple_schema, extraction_config):
        mock_result = MagicMock()
        mock_result.model_dump.return_value = {"vendor": "X", "amount": 0.0, "paid": None}

        mock_client = MagicMock()
        mock_client.chat.completions.create.return_value = mock_result

        with patch("parsebox.extract.strategies.default.instructor") as mock_instructor:
            mock_instructor.from_litellm.return_value = mock_client
            extract_single(sample_document, simple_schema, extraction_config)

        call_kwargs = mock_client.chat.completions.create.call_args
        assert call_kwargs.kwargs["model"] == "openai/gpt-4o"
        assert call_kwargs.kwargs["temperature"] == 0.0
        assert call_kwargs.kwargs["max_retries"] == 2
        assert call_kwargs.kwargs["api_key"] == "test-key"
        messages = call_kwargs.kwargs["messages"]
        assert len(messages) == 2
        assert messages[0]["role"] == "system"
        assert messages[1]["role"] == "user"


# ---------------------------------------------------------------------------
# extract_sample tests (mocked extract_single)
# ---------------------------------------------------------------------------

class TestExtractSample:
    def _mock_extract_single(self, doc, schema, config):
        return Record(
            document_id=doc.id,
            document_filename=doc.filename,
            data={"vendor": "Test", "amount": 100.0, "paid": True},
            status="success",
        )

    def test_takes_first_n_documents(self, sample_documents, simple_schema, extraction_config):
        with patch("parsebox.extract.engine.extract_single", side_effect=self._mock_extract_single):
            result = extract_sample(sample_documents, simple_schema, extraction_config, sample_size=3)

        assert len(result.records) == 3
        assert result.summary.total == 3

    def test_default_sample_size(self, sample_documents, simple_schema, extraction_config):
        with patch("parsebox.extract.engine.extract_single", side_effect=self._mock_extract_single):
            result = extract_sample(sample_documents, simple_schema, extraction_config)

        assert len(result.records) == 5

    def test_sample_size_larger_than_docs(self, simple_schema, extraction_config):
        docs = [
            Document(id="d1", filename="a.txt", file_type="txt", content="hello"),
            Document(id="d2", filename="b.txt", file_type="txt", content="world"),
        ]
        with patch("parsebox.extract.engine.extract_single", side_effect=self._mock_extract_single):
            result = extract_sample(docs, simple_schema, extraction_config, sample_size=10)

        assert len(result.records) == 2

    def test_summary_counts(self, sample_documents, simple_schema, extraction_config):
        call_count = 0

        def alternating_extract(doc, schema, config):
            nonlocal call_count
            call_count += 1
            if call_count % 2 == 0:
                return Record(
                    document_id=doc.id,
                    document_filename=doc.filename,
                    data={},
                    status="failed",
                    errors=["test error"],
                )
            return Record(
                document_id=doc.id,
                document_filename=doc.filename,
                data={"vendor": "V"},
                status="success",
            )

        with patch("parsebox.extract.engine.extract_single", side_effect=alternating_extract):
            result = extract_sample(sample_documents, simple_schema, extraction_config, sample_size=4)

        assert result.summary.total == 4
        assert result.summary.succeeded == 2
        assert result.summary.failed == 2

    def test_result_has_schema_and_config(self, sample_documents, simple_schema, extraction_config):
        with patch("parsebox.extract.engine.extract_single", side_effect=self._mock_extract_single):
            result = extract_sample(sample_documents, simple_schema, extraction_config, sample_size=1)

        assert result.schema_ == simple_schema
        assert result.config == extraction_config


# ---------------------------------------------------------------------------
# extract_batch tests (mocked extract_single)
# ---------------------------------------------------------------------------

class TestExtractBatch:
    def _mock_extract_single(self, doc, schema, config):
        return Record(
            document_id=doc.id,
            document_filename=doc.filename,
            data={"vendor": "Test"},
            status="success",
        )

    def test_processes_all_documents(self, sample_documents, simple_schema, extraction_config):
        with patch("parsebox.extract.engine.extract_single", side_effect=self._mock_extract_single):
            result = extract_batch(sample_documents, simple_schema, extraction_config)

        assert len(result.records) == 7
        assert result.summary.total == 7
        assert result.summary.succeeded == 7
        assert result.summary.failed == 0

    def test_progress_callback_called(self, sample_documents, simple_schema, extraction_config):
        callback = MagicMock()

        with patch("parsebox.extract.engine.extract_single", side_effect=self._mock_extract_single):
            extract_batch(
                sample_documents, simple_schema, extraction_config, progress_callback=callback
            )

        assert callback.call_count == 7
        # Each call should be (completed, total) where total is 7
        for call_args in callback.call_args_list:
            assert call_args[0][1] == 7

    def test_maintains_document_order(self, simple_schema, extraction_config):
        docs = [
            Document(id=f"doc-{i}", filename=f"file_{i}.txt", file_type="txt", content=f"content {i}")
            for i in range(5)
        ]

        with patch("parsebox.extract.engine.extract_single", side_effect=self._mock_extract_single):
            result = extract_batch(docs, simple_schema, extraction_config, max_workers=3)

        record_doc_ids = [r.document_id for r in result.records]
        assert record_doc_ids == [f"doc-{i}" for i in range(5)]

    def test_handles_failures_in_batch(self, simple_schema, extraction_config):
        docs = [
            Document(id="ok-1", filename="ok1.txt", file_type="txt", content="good"),
            Document(id="fail-1", filename="fail1.txt", file_type="txt", content="bad"),
            Document(id="ok-2", filename="ok2.txt", file_type="txt", content="good"),
        ]

        def mixed_extract(doc, schema, config):
            if "fail" in doc.id:
                return Record(
                    document_id=doc.id,
                    document_filename=doc.filename,
                    data={},
                    status="failed",
                    errors=["extraction error"],
                )
            return Record(
                document_id=doc.id,
                document_filename=doc.filename,
                data={"vendor": "OK"},
                status="success",
            )

        with patch("parsebox.extract.engine.extract_single", side_effect=mixed_extract):
            result = extract_batch(docs, simple_schema, extraction_config)

        assert result.summary.total == 3
        assert result.summary.succeeded == 2
        assert result.summary.failed == 1

    def test_result_has_schema_and_config(self, sample_documents, simple_schema, extraction_config):
        with patch("parsebox.extract.engine.extract_single", side_effect=self._mock_extract_single):
            result = extract_batch(sample_documents, simple_schema, extraction_config)

        assert result.schema_ == simple_schema
        assert result.config == extraction_config

    def test_no_callback_does_not_fail(self, sample_documents, simple_schema, extraction_config):
        with patch("parsebox.extract.engine.extract_single", side_effect=self._mock_extract_single):
            result = extract_batch(
                sample_documents, simple_schema, extraction_config, progress_callback=None
            )

        assert len(result.records) == 7
