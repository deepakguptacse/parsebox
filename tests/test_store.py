"""Tests for the Store module: writer registry, Parquet writer, and CSV writer."""

import csv
import json

import pyarrow.parquet as pq
import pytest

from parsebox.models import FieldDefinition, Record, Schema
from parsebox.store import write_records
from parsebox.store.registry import Writer, WriterRegistry, get_registry
from parsebox.store.writers.csv_writer import CsvWriter
from parsebox.store.writers.parquet import ParquetWriter


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def sample_schema():
    return Schema(
        name="test_schema",
        fields=[
            FieldDefinition(name="name", type="string", required=True),
            FieldDefinition(name="amount", type="number", required=True),
            FieldDefinition(name="active", type="boolean"),
        ],
    )


@pytest.fixture
def sample_records():
    return [
        Record(
            document_id="doc1",
            document_filename="a.pdf",
            data={"name": "Alice", "amount": 100.5, "active": True},
        ),
        Record(
            document_id="doc2",
            document_filename="b.pdf",
            data={"name": "Bob", "amount": 200.0, "active": False},
        ),
    ]


@pytest.fixture
def records_with_failure(sample_records):
    failed = Record(
        document_id="doc3",
        document_filename="c.pdf",
        status="failed",
        errors=["LLM timeout"],
        data={},
    )
    return sample_records + [failed]


@pytest.fixture
def complex_schema():
    return Schema(
        name="complex_schema",
        fields=[
            FieldDefinition(name="title", type="string"),
            FieldDefinition(name="tags", type="array"),
            FieldDefinition(name="metadata", type="object"),
        ],
    )


@pytest.fixture
def complex_records():
    return [
        Record(
            document_id="doc1",
            document_filename="a.pdf",
            data={
                "title": "Test",
                "tags": ["a", "b", "c"],
                "metadata": {"key": "value", "count": 42},
            },
        ),
    ]


# ---------------------------------------------------------------------------
# WriterRegistry tests
# ---------------------------------------------------------------------------

class TestWriterRegistry:
    def test_register_and_get(self):
        registry = WriterRegistry()
        writer = ParquetWriter()
        registry.register(writer)
        assert registry.get_writer("parquet") is writer

    def test_unknown_format_raises(self):
        registry = WriterRegistry()
        with pytest.raises(ValueError, match="No writer registered for format 'xyz'"):
            registry.get_writer("xyz")

    def test_available_formats(self):
        registry = WriterRegistry()
        registry.register(ParquetWriter())
        registry.register(CsvWriter())
        formats = registry.available_formats()
        assert "parquet" in formats
        assert "csv" in formats

    def test_write_delegates_to_writer(self, tmp_path, sample_schema, sample_records):
        registry = WriterRegistry()
        registry.register(ParquetWriter())
        path = registry.write(
            sample_records, sample_schema, "parquet", tmp_path / "out.parquet"
        )
        assert path.exists()

    def test_global_registry_has_writers(self):
        """After auto-discover, the global registry should have parquet and csv."""
        from parsebox.store.registry import auto_discover

        auto_discover()
        registry = get_registry()
        assert "parquet" in registry.available_formats()
        assert "csv" in registry.available_formats()


# ---------------------------------------------------------------------------
# ParquetWriter tests
# ---------------------------------------------------------------------------

class TestParquetWriter:
    def test_format_name(self):
        assert ParquetWriter().format_name() == "parquet"

    def test_write_basic(self, tmp_path, sample_schema, sample_records):
        writer = ParquetWriter()
        path = writer.write(sample_records, sample_schema, tmp_path / "data.parquet")
        assert path.exists()
        assert path.suffix == ".parquet"

        table = pq.read_table(path)
        assert table.num_rows == 2
        assert table.column("name").to_pylist() == ["Alice", "Bob"]
        assert table.column("amount").to_pylist() == [100.5, 200.0]
        assert table.column("active").to_pylist() == [True, False]

    def test_adds_parquet_extension(self, tmp_path, sample_schema, sample_records):
        writer = ParquetWriter()
        path = writer.write(sample_records, sample_schema, tmp_path / "data")
        assert path.suffix == ".parquet"
        assert path.exists()

    def test_skips_failed_records(self, tmp_path, sample_schema, records_with_failure):
        writer = ParquetWriter()
        path = writer.write(
            records_with_failure, sample_schema, tmp_path / "data.parquet"
        )
        table = pq.read_table(path)
        assert table.num_rows == 2

    def test_complex_types_serialized(self, tmp_path, complex_schema, complex_records):
        writer = ParquetWriter()
        path = writer.write(
            complex_records, complex_schema, tmp_path / "complex.parquet"
        )
        table = pq.read_table(path)
        assert table.num_rows == 1

        tags_val = table.column("tags").to_pylist()[0]
        assert json.loads(tags_val) == ["a", "b", "c"]

        meta_val = table.column("metadata").to_pylist()[0]
        assert json.loads(meta_val) == {"key": "value", "count": 42}

    def test_creates_parent_dirs(self, tmp_path, sample_schema, sample_records):
        writer = ParquetWriter()
        nested = tmp_path / "sub" / "dir" / "data.parquet"
        path = writer.write(sample_records, sample_schema, nested)
        assert path.exists()

    def test_none_values(self, tmp_path):
        """Records with missing field values should produce None in the output."""
        schema = Schema(
            name="nullable_schema",
            fields=[
                FieldDefinition(name="name", type="string", required=True),
                FieldDefinition(name="amount", type="number", required=False),
                FieldDefinition(name="active", type="boolean", required=False),
            ],
        )
        records = [
            Record(
                document_id="doc1",
                document_filename="a.pdf",
                data={"name": "Alice"},
            ),
        ]
        writer = ParquetWriter()
        path = writer.write(records, schema, tmp_path / "nulls.parquet")
        table = pq.read_table(path)
        assert table.column("amount").to_pylist() == [None]
        assert table.column("active").to_pylist() == [None]


# ---------------------------------------------------------------------------
# CsvWriter tests
# ---------------------------------------------------------------------------

class TestCsvWriter:
    def test_format_name(self):
        assert CsvWriter().format_name() == "csv"

    def test_write_basic(self, tmp_path, sample_schema, sample_records):
        writer = CsvWriter()
        path = writer.write(sample_records, sample_schema, tmp_path / "data.csv")
        assert path.exists()
        assert path.suffix == ".csv"

        with open(path, newline="") as f:
            reader = csv.DictReader(f)
            rows = list(reader)

        assert len(rows) == 2
        assert rows[0]["name"] == "Alice"
        assert rows[0]["amount"] == "100.5"
        assert rows[1]["name"] == "Bob"

    def test_adds_csv_extension(self, tmp_path, sample_schema, sample_records):
        writer = CsvWriter()
        path = writer.write(sample_records, sample_schema, tmp_path / "data")
        assert path.suffix == ".csv"
        assert path.exists()

    def test_headers_match_schema(self, tmp_path, sample_schema, sample_records):
        writer = CsvWriter()
        path = writer.write(sample_records, sample_schema, tmp_path / "data.csv")

        with open(path, newline="") as f:
            reader = csv.reader(f)
            headers = next(reader)

        expected = [f.name for f in sample_schema.fields]
        assert headers == expected

    def test_skips_failed_records(self, tmp_path, sample_schema, records_with_failure):
        writer = CsvWriter()
        path = writer.write(
            records_with_failure, sample_schema, tmp_path / "data.csv"
        )
        with open(path, newline="") as f:
            rows = list(csv.DictReader(f))
        assert len(rows) == 2

    def test_complex_types_serialized(self, tmp_path, complex_schema, complex_records):
        writer = CsvWriter()
        path = writer.write(
            complex_records, complex_schema, tmp_path / "complex.csv"
        )
        with open(path, newline="") as f:
            rows = list(csv.DictReader(f))

        assert len(rows) == 1
        assert json.loads(rows[0]["tags"]) == ["a", "b", "c"]
        assert json.loads(rows[0]["metadata"]) == {"key": "value", "count": 42}

    def test_creates_parent_dirs(self, tmp_path, sample_schema, sample_records):
        writer = CsvWriter()
        nested = tmp_path / "sub" / "dir" / "data.csv"
        path = writer.write(sample_records, sample_schema, nested)
        assert path.exists()


# ---------------------------------------------------------------------------
# write_records entry point tests
# ---------------------------------------------------------------------------

class TestWriteRecords:
    def test_write_parquet(self, tmp_path, sample_schema, sample_records):
        path = write_records(
            sample_records, sample_schema, "parquet", tmp_path / "out.parquet"
        )
        assert path.exists()
        assert path.suffix == ".parquet"
        table = pq.read_table(path)
        assert table.num_rows == 2

    def test_write_csv(self, tmp_path, sample_schema, sample_records):
        path = write_records(
            sample_records, sample_schema, "csv", tmp_path / "out.csv"
        )
        assert path.exists()
        assert path.suffix == ".csv"

    def test_unknown_format_raises(self, tmp_path, sample_schema, sample_records):
        with pytest.raises(ValueError, match="No writer registered"):
            write_records(
                sample_records, sample_schema, "excel", tmp_path / "out.xlsx"
            )

    def test_accepts_string_path(self, tmp_path, sample_schema, sample_records):
        path = write_records(
            sample_records, sample_schema, "csv", str(tmp_path / "string_path.csv")
        )
        assert path.exists()
