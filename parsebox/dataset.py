"""Dataset lifecycle management.

Higher-level operations built on top of DatasetStorage: create, add documents,
update schema, transition status, with validation at each step.
"""

import logging
from datetime import datetime

from parsebox.models import Dataset, Document, Schema
from parsebox.storage import DatasetStorage

logger = logging.getLogger(__name__)

VALID_TRANSITIONS = {
    "draft": ["schema_ready", "failed"],
    "schema_ready": ["previewed", "extracted", "failed"],
    "previewed": ["schema_ready", "extracted", "failed"],
    "extracted": ["schema_ready", "failed"],
    "failed": ["draft"],
}


class DatasetManager:
    def __init__(self, storage: DatasetStorage):
        self.storage = storage

    def create_dataset(self, name: str, user_id: str) -> Dataset:
        dataset = Dataset(name=name, status="draft")
        self.storage.save(user_id, dataset)
        logger.info("Created dataset '%s' (id=%s) for user %s", name, dataset.id, user_id)
        return dataset

    def add_documents(self, dataset_id: str, user_id: str, documents: list[Document]) -> Dataset:
        dataset = self.storage.load(user_id, dataset_id)
        dataset.documents.extend(documents)
        dataset.updated_at = datetime.now()
        self.storage.save(user_id, dataset)
        logger.info(
            "Added %d documents to dataset %s (total: %d)",
            len(documents), dataset_id, len(dataset.documents),
        )
        return dataset

    def update_schema(self, dataset_id: str, user_id: str, schema: Schema) -> Dataset:
        dataset = self.storage.load(user_id, dataset_id)
        dataset.schema_ = schema
        dataset.updated_at = datetime.now()
        self.storage.save(user_id, dataset)
        logger.info("Updated schema for dataset %s to '%s'", dataset_id, schema.name)
        return dataset

    def update_status(self, dataset_id: str, user_id: str, status: str) -> Dataset:
        dataset = self.storage.load(user_id, dataset_id)
        current = dataset.status
        allowed = VALID_TRANSITIONS.get(current, [])

        if status not in allowed:
            msg = f"Invalid status transition: '{current}' -> '{status}'. Allowed: {allowed}"
            logger.error(msg)
            raise ValueError(msg)

        if status == "schema_ready" and dataset.schema_ is None:
            msg = "Cannot transition to 'schema_ready' without a schema"
            logger.error(msg)
            raise ValueError(msg)

        dataset.status = status
        dataset.updated_at = datetime.now()
        self.storage.save(user_id, dataset)
        logger.info("Dataset %s status: %s -> %s", dataset_id, current, status)
        return dataset

    def get_dataset(self, dataset_id: str, user_id: str) -> Dataset:
        return self.storage.load(user_id, dataset_id)

    def list_datasets(self, user_id: str) -> list[dict]:
        return self.storage.list(user_id)

    def delete_dataset(self, dataset_id: str, user_id: str) -> None:
        self.storage.delete(user_id, dataset_id)
        logger.info("Deleted dataset %s for user %s", dataset_id, user_id)
