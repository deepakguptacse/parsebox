"""Writer registry for the Store module.

Maps format names to Writer implementations. Same pattern as the parser registry.
"""

import abc
import logging
from pathlib import Path

from parsebox.models import Record, Schema

logger = logging.getLogger(__name__)


class Writer(abc.ABC):
    @abc.abstractmethod
    def format_name(self) -> str:
        """Return the format name (e.g., 'parquet', 'csv')."""
        ...

    @abc.abstractmethod
    def write(self, records: list[Record], schema: Schema, output_path: Path) -> Path:
        """Write records to the specified format. Returns the output file path."""
        ...


class WriterRegistry:
    def __init__(self):
        self._writers: dict[str, Writer] = {}

    def register(self, writer: Writer) -> None:
        name = writer.format_name()
        self._writers[name] = writer
        logger.info("Registered writer for format: %s", name)

    def get_writer(self, format_name: str) -> Writer:
        if format_name not in self._writers:
            available = ", ".join(self._writers.keys()) or "(none)"
            raise ValueError(
                f"No writer registered for format '{format_name}'. "
                f"Available formats: {available}"
            )
        return self._writers[format_name]

    def available_formats(self) -> list[str]:
        return list(self._writers.keys())

    def write(
        self,
        records: list[Record],
        schema: Schema,
        format_name: str,
        output_path: Path,
    ) -> Path:
        writer = self.get_writer(format_name)
        logger.info(
            "Writing %d records in '%s' format to %s",
            len(records),
            format_name,
            output_path,
        )
        return writer.write(records, schema, output_path)


_registry = WriterRegistry()


def get_registry() -> WriterRegistry:
    return _registry


def auto_discover():
    """Import writer modules so they self-register with the global registry."""
    from parsebox.store.writers import parquet, csv_writer  # noqa: F401

    logger.debug("Writer auto-discovery complete")
