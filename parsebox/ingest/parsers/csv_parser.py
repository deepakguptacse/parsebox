"""CSV parser for .csv files."""

import csv
import io
import logging
import os

from parsebox.models import Document, ContentBlock
from parsebox.ingest.registry import Parser, get_registry

logger = logging.getLogger(__name__)


class CsvParser(Parser):
    """Parse CSV files into Documents with header and row ContentBlocks."""

    def supported_extensions(self) -> list[str]:
        return [".csv"]

    def parse(self, raw_bytes: bytes, filename: str) -> Document:
        logger.info("Parsing CSV file '%s'", filename)
        text = raw_bytes.decode("utf-8")
        reader = csv.reader(io.StringIO(text))
        rows = list(reader)

        content_blocks: list[ContentBlock] = []
        headers: list[str] = []
        data_rows: list[list[str]] = []

        if rows:
            headers = rows[0]
            content_blocks.append(ContentBlock(
                block_type="header",
                content=",".join(headers),
                metadata={"columns": headers},
            ))
            data_rows = rows[1:]
            for idx, row in enumerate(data_rows):
                content_blocks.append(ContentBlock(
                    block_type="row",
                    content=",".join(row),
                    metadata={"row_index": idx},
                ))

        ext = os.path.splitext(filename)[1].lower()
        doc = Document(
            filename=filename,
            file_type=ext,
            content=text,
            content_blocks=content_blocks,
            metadata={
                "row_count": len(data_rows),
                "column_count": len(headers),
                "columns": headers,
            },
            raw_bytes=raw_bytes,
        )
        logger.info(
            "Parsed '%s': %d rows, %d columns",
            filename, len(data_rows), len(headers),
        )
        return doc


# Auto-register
get_registry().register(CsvParser())
