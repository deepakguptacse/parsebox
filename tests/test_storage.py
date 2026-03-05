"""Tests for parsebox dataset storage."""

import json

import pytest

from parsebox.models import Dataset, Document, Schema, FieldDefinition
from parsebox.storage import LocalStorage


@pytest.fixture
def storage(tmp_path):
    return LocalStorage(data_dir=tmp_path)


@pytest.fixture
def user_id():
    return "test-user-001"


@pytest.fixture
def sample_dataset():
    return Dataset(name="Test Dataset")


class TestLocalStorageInit:
    def test_creates_data_dir(self, tmp_path):
        target = tmp_path / "custom_dir"
        assert not target.exists()
        LocalStorage(data_dir=target)
        assert target.exists()

    def test_default_dir_is_home(self):
        storage = LocalStorage()
        assert ".parsebox" in str(storage.data_dir)


class TestLocalStorageSave:
    def test_save_creates_file(self, storage, user_id, sample_dataset):
        storage.save(user_id, sample_dataset)
        ds_dir = storage._dataset_dir(user_id, sample_dataset.id)
        assert (ds_dir / "dataset.json").exists()

    def test_save_content_is_valid_json(self, storage, user_id, sample_dataset):
        storage.save(user_id, sample_dataset)
        ds_dir = storage._dataset_dir(user_id, sample_dataset.id)
        data = json.loads((ds_dir / "dataset.json").read_text())
        assert data["name"] == "Test Dataset"
        assert data["status"] == "draft"

    def test_save_excludes_raw_bytes(self, storage, user_id):
        doc = Document(filename="a.txt", file_type="txt", content="hello", raw_bytes=b"raw")
        ds = Dataset(name="with docs", documents=[doc])
        storage.save(user_id, ds)
        ds_dir = storage._dataset_dir(user_id, ds.id)
        data = json.loads((ds_dir / "dataset.json").read_text())
        for doc_data in data["documents"]:
            assert "raw_bytes" not in doc_data


class TestLocalStorageLoad:
    def test_load_roundtrip(self, storage, user_id, sample_dataset):
        storage.save(user_id, sample_dataset)
        loaded = storage.load(user_id, sample_dataset.id)
        assert loaded.name == sample_dataset.name
        assert loaded.id == sample_dataset.id
        assert loaded.status == sample_dataset.status

    def test_load_not_found(self, storage, user_id):
        with pytest.raises(FileNotFoundError):
            storage.load(user_id, "nonexistent-id")

    def test_load_with_schema(self, storage, user_id):
        schema = Schema(
            name="invoice",
            fields=[FieldDefinition(name="amount", type="number", required=True)],
        )
        ds = Dataset(name="with schema", schema=schema)
        storage.save(user_id, ds)
        loaded = storage.load(user_id, ds.id)
        assert loaded.schema_.name == "invoice"
        assert loaded.schema_.fields[0].name == "amount"


class TestLocalStorageList:
    def test_list_empty(self, storage, user_id):
        result = storage.list(user_id)
        assert result == []

    def test_list_returns_summaries(self, storage, user_id):
        ds1 = Dataset(name="First")
        ds2 = Dataset(name="Second")
        storage.save(user_id, ds1)
        storage.save(user_id, ds2)
        result = storage.list(user_id)
        assert len(result) == 2
        names = {r["name"] for r in result}
        assert names == {"First", "Second"}

    def test_list_includes_expected_fields(self, storage, user_id):
        ds = Dataset(name="Test")
        storage.save(user_id, ds)
        result = storage.list(user_id)
        assert len(result) == 1
        item = result[0]
        assert "id" in item
        assert "name" in item
        assert "status" in item
        assert "created_at" in item
        assert "document_count" in item


class TestLocalStorageDelete:
    def test_delete_removes_directory(self, storage, user_id, sample_dataset):
        storage.save(user_id, sample_dataset)
        ds_dir = storage._dataset_dir(user_id, sample_dataset.id)
        assert ds_dir.exists()
        storage.delete(user_id, sample_dataset.id)
        assert not ds_dir.exists()

    def test_delete_nonexistent_does_not_raise(self, storage, user_id):
        storage.delete(user_id, "does-not-exist")

    def test_delete_then_load_raises(self, storage, user_id, sample_dataset):
        storage.save(user_id, sample_dataset)
        storage.delete(user_id, sample_dataset.id)
        with pytest.raises(FileNotFoundError):
            storage.load(user_id, sample_dataset.id)
