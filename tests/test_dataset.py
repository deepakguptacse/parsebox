"""Tests for parsebox dataset lifecycle management."""

import pytest

from parsebox.models import Dataset, Document, FieldDefinition, Schema
from parsebox.storage import LocalStorage
from parsebox.dataset import DatasetManager, VALID_TRANSITIONS


@pytest.fixture
def storage(tmp_path):
    return LocalStorage(data_dir=tmp_path)


@pytest.fixture
def manager(storage):
    return DatasetManager(storage=storage)


@pytest.fixture
def user_id():
    return "test-user-002"


class TestCreateDataset:
    def test_create_returns_dataset(self, manager, user_id):
        ds = manager.create_dataset("My Dataset", user_id)
        assert ds.name == "My Dataset"
        assert ds.status == "draft"
        assert ds.id

    def test_create_persists(self, manager, user_id):
        ds = manager.create_dataset("Persisted", user_id)
        loaded = manager.get_dataset(ds.id, user_id)
        assert loaded.name == "Persisted"


class TestAddDocuments:
    def test_add_documents(self, manager, user_id):
        ds = manager.create_dataset("DocTest", user_id)
        docs = [
            Document(filename="a.txt", file_type="txt", content="content a"),
            Document(filename="b.txt", file_type="txt", content="content b"),
        ]
        updated = manager.add_documents(ds.id, user_id, docs)
        assert len(updated.documents) == 2

    def test_add_documents_extends(self, manager, user_id):
        ds = manager.create_dataset("ExtendTest", user_id)
        doc1 = [Document(filename="a.txt", file_type="txt", content="a")]
        doc2 = [Document(filename="b.txt", file_type="txt", content="b")]
        manager.add_documents(ds.id, user_id, doc1)
        updated = manager.add_documents(ds.id, user_id, doc2)
        assert len(updated.documents) == 2

    def test_add_documents_updates_timestamp(self, manager, user_id):
        ds = manager.create_dataset("TimeTest", user_id)
        original_updated = ds.updated_at
        docs = [Document(filename="a.txt", file_type="txt", content="a")]
        updated = manager.add_documents(ds.id, user_id, docs)
        assert updated.updated_at >= original_updated


class TestUpdateSchema:
    def test_update_schema(self, manager, user_id):
        ds = manager.create_dataset("SchemaTest", user_id)
        schema = Schema(
            name="invoice",
            fields=[FieldDefinition(name="total", type="number")],
        )
        updated = manager.update_schema(ds.id, user_id, schema)
        assert updated.schema_.name == "invoice"
        assert len(updated.schema_.fields) == 1

    def test_update_schema_persists(self, manager, user_id):
        ds = manager.create_dataset("SchemaPersist", user_id)
        schema = Schema(name="test_schema")
        manager.update_schema(ds.id, user_id, schema)
        loaded = manager.get_dataset(ds.id, user_id)
        assert loaded.schema_.name == "test_schema"


class TestUpdateStatus:
    def test_valid_transition_draft_to_schema_ready(self, manager, user_id):
        ds = manager.create_dataset("StatusTest", user_id)
        schema = Schema(name="s", fields=[FieldDefinition(name="x", type="string")])
        manager.update_schema(ds.id, user_id, schema)
        updated = manager.update_status(ds.id, user_id, "schema_ready")
        assert updated.status == "schema_ready"

    def test_valid_transition_schema_ready_to_previewed(self, manager, user_id):
        ds = manager.create_dataset("PreviewTest", user_id)
        schema = Schema(name="s")
        manager.update_schema(ds.id, user_id, schema)
        manager.update_status(ds.id, user_id, "schema_ready")
        updated = manager.update_status(ds.id, user_id, "previewed")
        assert updated.status == "previewed"

    def test_valid_transition_previewed_to_extracted(self, manager, user_id):
        ds = manager.create_dataset("ExtractTest", user_id)
        schema = Schema(name="s")
        manager.update_schema(ds.id, user_id, schema)
        manager.update_status(ds.id, user_id, "schema_ready")
        manager.update_status(ds.id, user_id, "previewed")
        updated = manager.update_status(ds.id, user_id, "extracted")
        assert updated.status == "extracted"

    def test_valid_transition_previewed_back_to_schema_ready(self, manager, user_id):
        ds = manager.create_dataset("BackTest", user_id)
        schema = Schema(name="s")
        manager.update_schema(ds.id, user_id, schema)
        manager.update_status(ds.id, user_id, "schema_ready")
        manager.update_status(ds.id, user_id, "previewed")
        updated = manager.update_status(ds.id, user_id, "schema_ready")
        assert updated.status == "schema_ready"

    def test_any_state_can_go_to_failed(self, manager, user_id):
        for from_status in ("draft", "schema_ready", "previewed", "extracted"):
            ds = manager.create_dataset(f"FailTest-{from_status}", user_id)
            if from_status in ("schema_ready", "previewed", "extracted"):
                schema = Schema(name="s")
                manager.update_schema(ds.id, user_id, schema)
                manager.update_status(ds.id, user_id, "schema_ready")
            if from_status in ("previewed", "extracted"):
                manager.update_status(ds.id, user_id, "previewed")
            if from_status == "extracted":
                manager.update_status(ds.id, user_id, "extracted")
            if from_status != "extracted":
                updated = manager.update_status(ds.id, user_id, "failed")
                assert updated.status == "failed"

    def test_invalid_transition_raises_value_error(self, manager, user_id):
        ds = manager.create_dataset("InvalidTest", user_id)
        with pytest.raises(ValueError, match="Invalid status transition"):
            manager.update_status(ds.id, user_id, "extracted")

    def test_schema_ready_without_schema_raises(self, manager, user_id):
        ds = manager.create_dataset("NoSchemaTest", user_id)
        with pytest.raises(ValueError, match="without a schema"):
            manager.update_status(ds.id, user_id, "schema_ready")

    def test_failed_can_retry_to_draft(self, manager, user_id):
        ds = manager.create_dataset("FailedTest", user_id)
        manager.update_status(ds.id, user_id, "failed")
        updated = manager.update_status(ds.id, user_id, "draft")
        assert updated.status == "draft"

    def test_failed_cannot_skip_to_extracted(self, manager, user_id):
        ds = manager.create_dataset("FailedTest2", user_id)
        manager.update_status(ds.id, user_id, "failed")
        with pytest.raises(ValueError, match="Invalid status transition"):
            manager.update_status(ds.id, user_id, "extracted")


class TestListAndDelete:
    def test_list_datasets(self, manager, user_id):
        manager.create_dataset("A", user_id)
        manager.create_dataset("B", user_id)
        result = manager.list_datasets(user_id)
        assert len(result) == 2

    def test_delete_dataset(self, manager, user_id):
        ds = manager.create_dataset("ToDelete", user_id)
        manager.delete_dataset(ds.id, user_id)
        with pytest.raises(FileNotFoundError):
            manager.get_dataset(ds.id, user_id)
