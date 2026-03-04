"""Dataset context -- binds an agent session to a specific dataset."""

import logging
import re
from dataclasses import dataclass, field
from pathlib import Path

from parsebox.models import Dataset
from parsebox.dataset import DatasetManager
from parsebox.storage import LocalStorage

logger = logging.getLogger(__name__)

DEFAULT_WORK_DIR = "/tmp/parsebox"
DEFAULT_LARGE_THRESHOLD = 500


def _sanitize_table_name(name: str) -> str:
    """Turn an arbitrary dataset name into a valid SQL identifier."""
    name = Path(name).name if "/" in name or "\\" in name else name
    name = re.sub(r"[^a-z0-9]", "_", name.lower())
    name = re.sub(r"_+", "_", name).strip("_")
    if not name or not name[0].isalpha():
        name = "t_" + name
    return name if len(name) > 2 else "dataset"


@dataclass
class DatasetContext:
    """All state an agent session needs, scoped to one dataset.

    Tools close over this context so they can only operate on the
    bound dataset -- the agent never picks the wrong target.
    """

    user_id: str
    dataset: Dataset
    storage: LocalStorage
    manager: DatasetManager
    source_folder: str | None = None
    work_dir: str = DEFAULT_WORK_DIR
    large_threshold: int = DEFAULT_LARGE_THRESHOLD
    demo_mode: bool = False

    def reload_dataset(self) -> None:
        """Reload dataset from storage to pick up any changes."""
        self.dataset = self.manager.get_dataset(self.dataset.id, self.user_id)

    @property
    def data_dir(self) -> Path:
        """Directory where extracted data files live."""
        return self.storage._dataset_dir(self.user_id, self.dataset.id)

    @property
    def csv_path(self) -> Path:
        return self.data_dir / "data.csv"

    @property
    def parquet_path(self) -> Path:
        return self.data_dir / "data.parquet"

    @property
    def text_dump_path(self) -> Path:
        return self.data_dir / "data.txt"

    @property
    def has_csv(self) -> bool:
        return self.csv_path.exists()

    @property
    def has_parquet(self) -> bool:
        return self.parquet_path.exists()

    @property
    def has_text_dump(self) -> bool:
        return self.text_dump_path.exists()

    def available_data_files(self) -> list[str]:
        """Return list of available extracted data file descriptions."""
        files = []
        if self.has_csv:
            files.append(f"CSV: {self.csv_path}")
        if self.has_parquet:
            files.append(f"Parquet: {self.parquet_path}")
        if self.has_text_dump:
            files.append(f"Text dump: {self.text_dump_path}")
        return files

    @property
    def table_name(self) -> str:
        return _sanitize_table_name(self.dataset.name)

    def ensure_query_engine(self):
        """Lazily init a DuckDB engine for demo-mode SQL queries."""
        if not hasattr(self, "_query_engine") or self._query_engine is None:
            from parsebox.query.engine import DuckDBQueryEngine
            engine = DuckDBQueryEngine()
            if self.has_parquet:
                engine.register_table(self.table_name, str(self.parquet_path))
            elif self.has_csv:
                import duckdb
                engine.conn.execute(
                    f"CREATE TABLE {self.table_name} AS SELECT * FROM read_csv_auto('{self.csv_path}')"
                )
                engine._registered_tables[self.table_name] = str(self.csv_path)
            else:
                raise RuntimeError("No extracted data available yet. Run extraction first.")
            self._query_engine = engine
            logger.info("Initialized query engine for table '%s'", self.table_name)
        return self._query_engine

    def cleanup(self) -> None:
        """Release resources."""
        if hasattr(self, "_query_engine") and self._query_engine:
            self._query_engine.close()
            self._query_engine = None
