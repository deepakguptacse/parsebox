"""Parquet writer -- converts records to a PyArrow table and writes to Parquet."""

import json
import logging
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from parsebox.models import Record, Schema
from parsebox.store.registry import Writer, get_registry

logger = logging.getLogger(__name__)

ARROW_TYPE_MAP = {
    "string": pa.utf8(),
    "number": pa.float64(),
    "boolean": pa.bool_(),
    "date": pa.utf8(),
    "array": pa.utf8(),
    "object": pa.utf8(),
}


class ParquetWriter(Writer):
    def format_name(self) -> str:
        return "parquet"

    def write(self, records: list[Record], schema: Schema, output_path: Path) -> Path:
        """Write records to a Parquet file."""
        output_path = Path(output_path)
        if not output_path.suffix:
            output_path = output_path.with_suffix(".parquet")

        # Build Arrow schema from our Schema
        arrow_fields = []
        for field in schema.fields:
            arrow_type = ARROW_TYPE_MAP.get(field.type, pa.utf8())
            arrow_fields.append(
                pa.field(field.name, arrow_type, nullable=not field.required)
            )
        arrow_schema = pa.schema(arrow_fields)

        # Build columns, skipping failed records
        columns = {field.name: [] for field in schema.fields}
        written_count = 0
        for record in records:
            if record.status == "failed":
                logger.debug(
                    "Skipping failed record for document %s", record.document_id
                )
                continue
            written_count += 1
            for field in schema.fields:
                value = record.data.get(field.name)
                if field.type in ("array", "object") and value is not None:
                    value = json.dumps(value)
                columns[field.name].append(value)

        # Create Arrow arrays and table
        arrays = []
        for field in schema.fields:
            arrow_type = ARROW_TYPE_MAP.get(field.type, pa.utf8())
            arrays.append(pa.array(columns[field.name], type=arrow_type))

        table = pa.table(
            dict(zip([f.name for f in schema.fields], arrays)),
            schema=arrow_schema,
        )

        output_path.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(table, output_path)

        logger.info(
            "Wrote %d records to Parquet: %s (skipped %d failed)",
            written_count,
            output_path,
            len(records) - written_count,
        )
        return output_path


get_registry().register(ParquetWriter())
