"""Store module -- write extracted records to Parquet, CSV, etc."""

import logging
from pathlib import Path

from parsebox.models import Record, Schema
from parsebox.store.registry import get_registry, auto_discover

logger = logging.getLogger(__name__)


def write_records(
    records: list[Record],
    schema: Schema,
    format_name: str,
    output_path: str | Path,
) -> Path:
    """High-level entry point: write records to the given format.

    Triggers auto-discovery of writers on first call, then delegates
    to the appropriate registered writer. Handles 0-record case gracefully.
    """
    auto_discover()
    output_path = Path(output_path)

    successful = [r for r in records if r.status != "failed"]
    logger.info(
        "write_records called: %d records (%d successful), format=%s, path=%s",
        len(records),
        len(successful),
        format_name,
        output_path,
    )
    if not successful:
        logger.warning("No successful records to write. Writing empty output file.")

    return get_registry().write(records, schema, format_name, output_path)
