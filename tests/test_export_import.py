"""Tests for parsebox dataset export and import."""

import io
import json
import zipfile

import pytest

from parsebox.models import Dataset, Schema, FieldDefinition, Document
from parsebox.storage import LocalStorage


@pytest.fixture
def storage(tmp_path):
    return LocalStorage(data_dir=tmp_path)


@pytest.fixture
def user_id():
    return "test-user-export"


@pytest.fixture
def sample_dataset():
    schema = Schema(
        name="test_schema",
        fields=[
            FieldDefinition(name="title", type="string", required=True),
            FieldDefinition(name="amount", type="number"),
        ],
    )
    doc = Document(filename="test.txt", file_type="txt", content="sample content")
    return Dataset(
        name="Export Test Dataset",
        schema=schema,
        documents=[doc],
        status="extracted",
    )


class TestExportDataset:
    def test_export_returns_bytes(self, storage, user_id, sample_dataset):
        storage.save(user_id, sample_dataset)
        result = storage.export_dataset(user_id, sample_dataset.id)
        assert isinstance(result, bytes)
        assert len(result) > 0

    def test_export_is_valid_zip(self, storage, user_id, sample_dataset):
        storage.save(user_id, sample_dataset)
        result = storage.export_dataset(user_id, sample_dataset.id)
        buffer = io.BytesIO(result)
        assert zipfile.is_zipfile(buffer)

    def test_export_contains_dataset_json(self, storage, user_id, sample_dataset):
        storage.save(user_id, sample_dataset)
        result = storage.export_dataset(user_id, sample_dataset.id)
        with zipfile.ZipFile(io.BytesIO(result), "r") as zf:
            assert "dataset.json" in zf.namelist()

    def test_export_dataset_json_valid(self, storage, user_id, sample_dataset):
        storage.save(user_id, sample_dataset)
        result = storage.export_dataset(user_id, sample_dataset.id)
        with zipfile.ZipFile(io.BytesIO(result), "r") as zf:
            data = json.loads(zf.read("dataset.json"))
            assert data["name"] == "Export Test Dataset"
            assert data["id"] == sample_dataset.id

    def test_export_nonexistent_raises(self, storage, user_id):
        with pytest.raises(FileNotFoundError):
            storage.export_dataset(user_id, "does-not-exist")

    def test_export_includes_parquet_if_present(self, storage, user_id, sample_dataset):
        storage.save(user_id, sample_dataset)
        ds_dir = storage._dataset_dir(user_id, sample_dataset.id)
        parquet_path = ds_dir / "data.parquet"
        parquet_path.write_bytes(b"fake parquet data")

        result = storage.export_dataset(user_id, sample_dataset.id)
        with zipfile.ZipFile(io.BytesIO(result), "r") as zf:
            assert "data.parquet" in zf.namelist()
            assert zf.read("data.parquet") == b"fake parquet data"

    def test_export_includes_documents_if_present(self, storage, user_id, sample_dataset):
        storage.save(user_id, sample_dataset)
        ds_dir = storage._dataset_dir(user_id, sample_dataset.id)
        docs_dir = ds_dir / "documents"
        docs_dir.mkdir(parents=True, exist_ok=True)
        (docs_dir / "file1.txt").write_text("hello")
        (docs_dir / "file2.txt").write_text("world")

        result = storage.export_dataset(user_id, sample_dataset.id)
        with zipfile.ZipFile(io.BytesIO(result), "r") as zf:
            names = zf.namelist()
            assert "documents/file1.txt" in names
            assert "documents/file2.txt" in names


class TestImportDataset:
    def _make_bundle(self, dataset_data: dict) -> bytes:
        """Helper to create a zip bundle from dataset dict."""
        buffer = io.BytesIO()
        with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as zf:
            zf.writestr("dataset.json", json.dumps(dataset_data))
        buffer.seek(0)
        return buffer.getvalue()

    def test_import_returns_dataset_id(self, storage, user_id):
        data = {"id": "imported-001", "name": "Imported Dataset", "status": "draft"}
        bundle = self._make_bundle(data)
        result = storage.import_dataset(user_id, bundle)
        assert result == "imported-001"

    def test_import_creates_dataset_file(self, storage, user_id):
        data = {"id": "imported-002", "name": "Imported Dataset 2", "status": "draft"}
        bundle = self._make_bundle(data)
        storage.import_dataset(user_id, bundle)
        ds_dir = storage._dataset_dir(user_id, "imported-002")
        assert (ds_dir / "dataset.json").exists()

    def test_import_missing_dataset_json_raises(self, storage, user_id):
        buffer = io.BytesIO()
        with zipfile.ZipFile(buffer, "w") as zf:
            zf.writestr("random.txt", "not a dataset")
        bundle = buffer.getvalue()
        with pytest.raises(ValueError, match="missing dataset.json"):
            storage.import_dataset(user_id, bundle)

    def test_import_missing_id_raises(self, storage, user_id):
        data = {"name": "No ID Dataset"}
        bundle = self._make_bundle(data)
        with pytest.raises(ValueError, match="missing 'id'"):
            storage.import_dataset(user_id, bundle)

    def test_import_preserves_extra_files(self, storage, user_id):
        buffer = io.BytesIO()
        with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as zf:
            zf.writestr("dataset.json", json.dumps({"id": "imp-003", "name": "test"}))
            zf.writestr("data.parquet", b"parquet bytes")
            zf.writestr("documents/file.txt", "doc content")
        bundle = buffer.getvalue()

        storage.import_dataset(user_id, bundle)
        ds_dir = storage._dataset_dir(user_id, "imp-003")
        assert (ds_dir / "data.parquet").exists()
        assert (ds_dir / "documents" / "file.txt").exists()
        assert (ds_dir / "documents" / "file.txt").read_text() == "doc content"


class TestExportImportRoundTrip:
    def test_round_trip_preserves_data(self, storage, user_id, sample_dataset):
        storage.save(user_id, sample_dataset)
        bundle = storage.export_dataset(user_id, sample_dataset.id)

        other_user = "test-user-import"
        imported_id = storage.import_dataset(other_user, bundle)
        assert imported_id == sample_dataset.id

        loaded = storage.load(other_user, imported_id)
        assert loaded.name == sample_dataset.name
        assert loaded.id == sample_dataset.id
        assert loaded.status == sample_dataset.status
        assert loaded.schema_.name == sample_dataset.schema_.name
        assert len(loaded.schema_.fields) == len(sample_dataset.schema_.fields)

    def test_round_trip_with_parquet(self, storage, user_id, sample_dataset):
        storage.save(user_id, sample_dataset)
        ds_dir = storage._dataset_dir(user_id, sample_dataset.id)
        (ds_dir / "data.parquet").write_bytes(b"fake parquet content")

        bundle = storage.export_dataset(user_id, sample_dataset.id)

        other_user = "test-user-import-2"
        imported_id = storage.import_dataset(other_user, bundle)
        imported_dir = storage._dataset_dir(other_user, imported_id)
        assert (imported_dir / "data.parquet").exists()
        assert (imported_dir / "data.parquet").read_bytes() == b"fake parquet content"
