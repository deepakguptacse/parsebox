"""CSV writer -- flattens records and writes to CSV."""

import csv
import json
import logging
from pathlib import Path

from parsebox.models import Record, Schema
from parsebox.store.registry import Writer, get_registry

logger = logging.getLogger(__name__)


class CsvWriter(Writer):
    def format_name(self) -> str:
        return "csv"

    def write(self, records: list[Record], schema: Schema, output_path: Path) -> Path:
        """Write records to a CSV file."""
        output_path = Path(output_path)
        if not output_path.suffix:
            output_path = output_path.with_suffix(".csv")

        field_names = [f.name for f in schema.fields]

        output_path.parent.mkdir(parents=True, exist_ok=True)

        written_count = 0
        with open(output_path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=field_names, extrasaction="ignore")
            writer.writeheader()

            for record in records:
                if record.status == "failed":
                    logger.debug(
                        "Skipping failed record for document %s",
                        record.document_id,
                    )
                    continue
                written_count += 1
                row = {}
                for field in schema.fields:
                    value = record.data.get(field.name)
                    if isinstance(value, (list, dict)):
                        value = json.dumps(value)
                    row[field.name] = value
                writer.writerow(row)

        logger.info(
            "Wrote %d records to CSV: %s (skipped %d failed)",
            written_count,
            output_path,
            len(records) - written_count,
        )
        return output_path


get_registry().register(CsvWriter())
