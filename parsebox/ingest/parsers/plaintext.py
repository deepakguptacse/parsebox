"""Plain text parser for .txt files."""

import logging
import os

from parsebox.models import Document, ContentBlock
from parsebox.ingest.registry import Parser, get_registry

logger = logging.getLogger(__name__)


class PlainTextParser(Parser):
    """Parse plain text files into Documents."""

    def supported_extensions(self) -> list[str]:
        return [".txt"]

    def parse(self, raw_bytes: bytes, filename: str) -> Document:
        logger.info("Parsing plain text file '%s'", filename)
        text = raw_bytes.decode("utf-8")

        # Split into paragraphs on blank lines
        paragraphs = [p.strip() for p in text.split("\n\n") if p.strip()]
        content_blocks = [
            ContentBlock(block_type="paragraph", content=p)
            for p in paragraphs
        ]

        ext = os.path.splitext(filename)[1].lower()
        doc = Document(
            filename=filename,
            file_type=ext,
            content=text,
            content_blocks=content_blocks,
            metadata={"char_count": len(text), "paragraph_count": len(paragraphs)},
            raw_bytes=raw_bytes,
        )
        logger.info("Parsed '%s': %d paragraphs, %d chars", filename, len(paragraphs), len(text))
        return doc


# Auto-register
get_registry().register(PlainTextParser())
