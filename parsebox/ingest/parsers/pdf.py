"""PDF parser for .pdf files.

Tries docling first, falls back to pypdf if docling is unavailable.
"""

import io
import logging
import os

from parsebox.models import Document, ContentBlock
from parsebox.ingest.registry import Parser, get_registry

logger = logging.getLogger(__name__)

_HAS_DOCLING = False
try:
    from docling.document_converter import DocumentConverter  # noqa: F401
    _HAS_DOCLING = True
    logger.info("docling is available for PDF parsing")
except ImportError:
    logger.info("docling not available, checking for pypdf fallback")

_HAS_PYPDF = False
try:
    from pypdf import PdfReader  # noqa: F401
    _HAS_PYPDF = True
    logger.info("pypdf is available for PDF parsing")
except ImportError:
    logger.warning("pypdf not available -- PDF parsing will be limited")


class PdfParser(Parser):
    """Parse PDF files into Documents.

    Uses docling if available, otherwise falls back to pypdf.
    """

    def supported_extensions(self) -> list[str]:
        return [".pdf"]

    def parse(self, raw_bytes: bytes, filename: str) -> Document:
        logger.info("Parsing PDF file '%s' (%d bytes)", filename, len(raw_bytes))

        if _HAS_DOCLING:
            return self._parse_with_docling(raw_bytes, filename)
        if _HAS_PYPDF:
            return self._parse_with_pypdf(raw_bytes, filename)

        logger.error("No PDF parsing library available")
        raise RuntimeError(
            "No PDF parsing library available. Install pypdf or docling."
        )

    def _parse_with_docling(self, raw_bytes: bytes, filename: str) -> Document:
        """Parse PDF using docling."""
        from docling.document_converter import DocumentConverter
        import tempfile

        logger.info("Using docling to parse '%s'", filename)
        # docling requires a file path, so write to a temp file
        with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
            tmp.write(raw_bytes)
            tmp_path = tmp.name

        try:
            converter = DocumentConverter()
            result = converter.convert(tmp_path)
            text = result.document.export_to_text()
        finally:
            os.unlink(tmp_path)

        content_blocks = [
            ContentBlock(block_type="paragraph", content=p.strip())
            for p in text.split("\n\n") if p.strip()
        ]

        ext = os.path.splitext(filename)[1].lower()
        doc = Document(
            filename=filename,
            file_type=ext,
            content=text,
            content_blocks=content_blocks,
            metadata={"parser": "docling", "char_count": len(text)},
            raw_bytes=raw_bytes,
        )
        logger.info("Parsed '%s' with docling: %d chars", filename, len(text))
        return doc

    def _parse_with_pypdf(self, raw_bytes: bytes, filename: str) -> Document:
        """Parse PDF using pypdf as fallback."""
        from pypdf import PdfReader

        logger.info("Using pypdf to parse '%s'", filename)
        reader = PdfReader(io.BytesIO(raw_bytes))
        page_count = len(reader.pages)

        content_blocks: list[ContentBlock] = []
        all_text_parts: list[str] = []

        for page_num, page in enumerate(reader.pages):
            page_text = page.extract_text() or ""
            all_text_parts.append(page_text)
            if page_text.strip():
                content_blocks.append(ContentBlock(
                    block_type="paragraph",
                    content=page_text.strip(),
                    metadata={"page": page_num + 1},
                ))

        text = "\n\n".join(all_text_parts)
        ext = os.path.splitext(filename)[1].lower()
        doc = Document(
            filename=filename,
            file_type=ext,
            content=text,
            content_blocks=content_blocks,
            metadata={
                "parser": "pypdf",
                "page_count": page_count,
                "char_count": len(text),
            },
            raw_bytes=raw_bytes,
        )
        logger.info("Parsed '%s' with pypdf: %d pages, %d chars", filename, page_count, len(text))
        return doc


# Auto-register
get_registry().register(PdfParser())
