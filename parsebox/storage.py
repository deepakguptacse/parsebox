"""Dataset storage interface and local filesystem implementation.

Provides CRUD operations for datasets, keyed by user_id.
V1 ships with LocalStorage (filesystem). Future backends: S3, GCS, etc.
"""

import abc
import json
import logging
import shutil
from pathlib import Path

from parsebox.models import Dataset

logger = logging.getLogger(__name__)


class DatasetStorage(abc.ABC):
    @abc.abstractmethod
    def save(self, user_id: str, dataset: Dataset) -> None: ...

    @abc.abstractmethod
    def load(self, user_id: str, dataset_id: str) -> Dataset: ...

    @abc.abstractmethod
    def list(self, user_id: str) -> list[dict]: ...

    @abc.abstractmethod
    def delete(self, user_id: str, dataset_id: str) -> None: ...

    @abc.abstractmethod
    def export_dataset(self, user_id: str, dataset_id: str) -> bytes: ...

    @abc.abstractmethod
    def import_dataset(self, user_id: str, bundle_bytes: bytes) -> str: ...


class LocalStorage(DatasetStorage):
    def __init__(self, data_dir: str | Path | None = None):
        if data_dir is None:
            data_dir = Path.home() / ".parsebox"
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        logger.info("LocalStorage initialized at %s", self.data_dir)

    def _dataset_dir(self, user_id: str, dataset_id: str) -> Path:
        return self.data_dir / "users" / user_id / "datasets" / dataset_id

    def save(self, user_id: str, dataset: Dataset) -> None:
        ds_dir = self._dataset_dir(user_id, dataset.id)
        ds_dir.mkdir(parents=True, exist_ok=True)
        dataset_path = ds_dir / "dataset.json"
        data = dataset.model_dump(mode="json", by_alias=True)
        dataset_path.write_text(json.dumps(data, indent=2, default=str))
        logger.info("Saved dataset '%s' (id=%s) for user %s", dataset.name, dataset.id, user_id)

    def load(self, user_id: str, dataset_id: str) -> Dataset:
        ds_dir = self._dataset_dir(user_id, dataset_id)
        dataset_path = ds_dir / "dataset.json"
        if not dataset_path.exists():
            logger.error("Dataset not found: %s for user %s", dataset_id, user_id)
            raise FileNotFoundError(f"Dataset {dataset_id} not found for user {user_id}")
        try:
            raw_text = dataset_path.read_text()
            data = json.loads(raw_text)
        except json.JSONDecodeError as e:
            logger.error("Corrupt dataset JSON at %s: %s", dataset_path, e)
            raise ValueError(
                f"Dataset {dataset_id} has corrupt data and cannot be loaded."
            ) from e
        try:
            dataset = Dataset.model_validate(data)
        except Exception as e:
            logger.error("Dataset validation failed for %s: %s", dataset_id, e)
            raise ValueError(
                f"Dataset {dataset_id} has invalid data: {e}"
            ) from e
        logger.info("Loaded dataset %s for user %s", dataset_id, user_id)
        return dataset

    def list(self, user_id: str) -> list[dict]:
        user_datasets_dir = self.data_dir / "users" / user_id / "datasets"
        if not user_datasets_dir.exists():
            logger.info("No datasets directory for user %s", user_id)
            return []

        results = []
        for ds_dir in user_datasets_dir.iterdir():
            if not ds_dir.is_dir():
                continue
            dataset_path = ds_dir / "dataset.json"
            if not dataset_path.exists():
                continue
            try:
                data = json.loads(dataset_path.read_text())
                results.append({
                    "id": data.get("id", ds_dir.name),
                    "name": data.get("name", ""),
                    "status": data.get("status", "draft"),
                    "created_at": data.get("created_at", ""),
                    "document_count": len(data.get("documents", [])),
                })
            except (json.JSONDecodeError, KeyError) as exc:
                logger.warning("Skipping corrupt dataset at %s: %s", ds_dir, exc)

        logger.info("Listed %d datasets for user %s", len(results), user_id)
        return results

    def delete(self, user_id: str, dataset_id: str) -> None:
        ds_dir = self._dataset_dir(user_id, dataset_id)
        if ds_dir.exists():
            shutil.rmtree(ds_dir)
            logger.info("Deleted dataset %s for user %s", dataset_id, user_id)
        else:
            logger.warning("Dataset %s not found for user %s during delete", dataset_id, user_id)

    def export_dataset(self, user_id: str, dataset_id: str) -> bytes:
        """Export a dataset as a .parsebox zip bundle.

        Bundle contains:
        - dataset.json (metadata, schema, config)
        - data.parquet (if exists)
        - documents/ (original uploaded files, if they exist)
        """
        import zipfile
        import io

        dataset_dir = self._dataset_dir(user_id, dataset_id)
        if not dataset_dir.exists():
            raise FileNotFoundError(f"Dataset not found: {dataset_id}")

        buffer = io.BytesIO()
        with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as zf:
            dataset_json = dataset_dir / "dataset.json"
            if dataset_json.exists():
                zf.write(dataset_json, "dataset.json")
                logger.info("Added dataset.json to export bundle")

            parquet_file = dataset_dir / "data.parquet"
            if parquet_file.exists():
                zf.write(parquet_file, "data.parquet")
                logger.info("Added data.parquet to export bundle")

            docs_dir = dataset_dir / "documents"
            if docs_dir.exists():
                for doc_file in docs_dir.iterdir():
                    zf.write(doc_file, f"documents/{doc_file.name}")
                    logger.info("Added document '%s' to export bundle", doc_file.name)

        logger.info("Exported dataset '%s' as zip bundle", dataset_id)
        buffer.seek(0)
        return buffer.getvalue()

    def import_dataset(self, user_id: str, bundle_bytes: bytes) -> str:
        """Import a dataset from a .parsebox zip bundle.

        Returns the dataset_id of the imported dataset.
        """
        import zipfile
        import io

        buffer = io.BytesIO(bundle_bytes)
        with zipfile.ZipFile(buffer, "r") as zf:
            if "dataset.json" not in zf.namelist():
                logger.error("Import failed: bundle missing dataset.json")
                raise ValueError("Invalid bundle: missing dataset.json")

            dataset_data = json.loads(zf.read("dataset.json"))
            dataset_id = dataset_data.get("id")
            if not dataset_id:
                logger.error("Import failed: dataset.json missing 'id' field")
                raise ValueError("Invalid bundle: dataset.json missing 'id'")

            dataset_dir = self._dataset_dir(user_id, dataset_id)
            dataset_dir.mkdir(parents=True, exist_ok=True)

            for name in zf.namelist():
                target = dataset_dir / name
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_bytes(zf.read(name))
                logger.info("Extracted '%s' to dataset directory", name)

        logger.info("Imported dataset '%s' for user '%s'", dataset_id, user_id)
        return dataset_id
