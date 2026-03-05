"""Tests for the parsebox ingest module (Issues 6-10).

Covers: ParserRegistry, PlainTextParser, MarkdownParser, CsvParser, PdfParser,
auto_discover, and the parse_file entry point.
"""

import pytest
from parsebox.models import Document, ContentBlock
from parsebox.ingest.registry import Parser, ParserRegistry, get_registry, auto_discover
from parsebox.ingest.parsers.plaintext import PlainTextParser
from parsebox.ingest.parsers.markdown import MarkdownParser
from parsebox.ingest.parsers.csv_parser import CsvParser
from parsebox.ingest.parsers.pdf import PdfParser, _HAS_PYPDF, _HAS_DOCLING


# ---------------------------------------------------------------------------
# Issue 6: ParserRegistry
# ---------------------------------------------------------------------------

class TestParserRegistry:
    def test_register_and_get_parser(self):
        registry = ParserRegistry()
        parser = PlainTextParser()
        registry.register(parser)
        assert registry.get_parser("file.txt") is parser

    def test_get_parser_case_insensitive(self):
        registry = ParserRegistry()
        registry.register(PlainTextParser())
        # Extension lookup is case-insensitive
        result = registry.get_parser("FILE.TXT")
        assert isinstance(result, PlainTextParser)

    def test_get_parser_unsupported_extension_raises(self):
        registry = ParserRegistry()
        with pytest.raises(ValueError, match="Unsupported file extension"):
            registry.get_parser("file.xyz")

    def test_supported_extensions(self):
        registry = ParserRegistry()
        registry.register(PlainTextParser())
        registry.register(CsvParser())
        exts = registry.supported_extensions()
        assert ".txt" in exts
        assert ".csv" in exts

    def test_parse_file_delegates_to_parser(self):
        registry = ParserRegistry()
        registry.register(PlainTextParser())
        doc = registry.parse_file(b"hello world", "test.txt")
        assert isinstance(doc, Document)
        assert doc.content == "hello world"
        assert doc.filename == "test.txt"

    def test_global_registry_singleton(self):
        r1 = get_registry()
        r2 = get_registry()
        assert r1 is r2


class TestAutoDiscover:
    def test_auto_discover_registers_all_parsers(self):
        auto_discover()
        registry = get_registry()
        exts = registry.supported_extensions()
        assert ".txt" in exts
        assert ".md" in exts
        assert ".csv" in exts
        assert ".pdf" in exts

    def test_auto_discover_idempotent(self):
        """Calling auto_discover multiple times does not break anything."""
        auto_discover()
        auto_discover()
        registry = get_registry()
        assert ".txt" in registry.supported_extensions()


# ---------------------------------------------------------------------------
# Issue 7: PlainTextParser
# ---------------------------------------------------------------------------

class TestPlainTextParser:
    def test_supported_extensions(self):
        parser = PlainTextParser()
        assert parser.supported_extensions() == [".txt"]

    def test_parse_basic(self):
        content = "Hello, world!"
        doc = PlainTextParser().parse(content.encode("utf-8"), "greeting.txt")
        assert isinstance(doc, Document)
        assert doc.filename == "greeting.txt"
        assert doc.file_type == ".txt"
        assert doc.content == content
        assert doc.id  # has a UUID

    def test_parse_paragraphs(self):
        content = "First paragraph.\n\nSecond paragraph.\n\nThird paragraph."
        doc = PlainTextParser().parse(content.encode("utf-8"), "multi.txt")
        assert len(doc.content_blocks) == 3
        assert all(b.block_type == "paragraph" for b in doc.content_blocks)
        assert doc.content_blocks[0].content == "First paragraph."
        assert doc.content_blocks[2].content == "Third paragraph."

    def test_parse_metadata(self):
        content = "Some text"
        doc = PlainTextParser().parse(content.encode("utf-8"), "meta.txt")
        assert doc.metadata["char_count"] == len(content)
        assert doc.metadata["paragraph_count"] == 1

    def test_parse_empty_file(self):
        doc = PlainTextParser().parse(b"", "empty.txt")
        assert doc.content == ""
        assert doc.content_blocks == []

    def test_raw_bytes_preserved(self):
        raw = b"hello"
        doc = PlainTextParser().parse(raw, "raw.txt")
        assert doc.raw_bytes == raw


# ---------------------------------------------------------------------------
# Issue 8: MarkdownParser
# ---------------------------------------------------------------------------

class TestMarkdownParser:
    def test_supported_extensions(self):
        parser = MarkdownParser()
        assert parser.supported_extensions() == [".md"]

    def test_parse_headers(self):
        content = "# Title\n\nSome text\n\n## Subtitle\n\nMore text"
        doc = MarkdownParser().parse(content.encode("utf-8"), "doc.md")
        assert doc.file_type == ".md"
        headers = [b for b in doc.content_blocks if b.block_type == "header"]
        assert len(headers) == 2
        assert headers[0].content == "Title"
        assert headers[0].metadata["level"] == 1
        assert headers[1].content == "Subtitle"
        assert headers[1].metadata["level"] == 2

    def test_parse_paragraphs(self):
        content = "# Title\n\nFirst paragraph.\n\nSecond paragraph."
        doc = MarkdownParser().parse(content.encode("utf-8"), "doc.md")
        paragraphs = [b for b in doc.content_blocks if b.block_type == "paragraph"]
        assert len(paragraphs) == 2
        assert paragraphs[0].content == "First paragraph."

    def test_parse_code_blocks(self):
        content = "# Example\n\n```python\nprint('hello')\n```\n\nDone."
        doc = MarkdownParser().parse(content.encode("utf-8"), "code.md")
        code_blocks = [b for b in doc.content_blocks if b.block_type == "code"]
        assert len(code_blocks) == 1
        assert "print('hello')" in code_blocks[0].content
        assert code_blocks[0].metadata.get("language") == "python"

    def test_parse_code_block_no_language(self):
        content = "```\nsome code\n```"
        doc = MarkdownParser().parse(content.encode("utf-8"), "code.md")
        code_blocks = [b for b in doc.content_blocks if b.block_type == "code"]
        assert len(code_blocks) == 1
        assert code_blocks[0].metadata == {}

    def test_parse_empty(self):
        doc = MarkdownParser().parse(b"", "empty.md")
        assert doc.content == ""
        assert doc.content_blocks == []

    def test_parse_metadata(self):
        content = "# Title\n\nText"
        doc = MarkdownParser().parse(content.encode("utf-8"), "meta.md")
        assert doc.metadata["char_count"] == len(content)
        assert doc.metadata["block_count"] == len(doc.content_blocks)


# ---------------------------------------------------------------------------
# Issue 9: CsvParser
# ---------------------------------------------------------------------------

class TestCsvParser:
    def test_supported_extensions(self):
        parser = CsvParser()
        assert parser.supported_extensions() == [".csv"]

    def test_parse_basic(self):
        content = "name,age,city\nAlice,30,NYC\nBob,25,LA\n"
        doc = CsvParser().parse(content.encode("utf-8"), "data.csv")
        assert isinstance(doc, Document)
        assert doc.file_type == ".csv"
        assert doc.content == content

    def test_parse_metadata(self):
        content = "name,age,city\nAlice,30,NYC\nBob,25,LA\n"
        doc = CsvParser().parse(content.encode("utf-8"), "data.csv")
        assert doc.metadata["row_count"] == 2
        assert doc.metadata["column_count"] == 3
        assert doc.metadata["columns"] == ["name", "age", "city"]

    def test_parse_content_blocks(self):
        content = "x,y\n1,2\n3,4\n"
        doc = CsvParser().parse(content.encode("utf-8"), "xy.csv")
        assert doc.content_blocks[0].block_type == "header"
        assert doc.content_blocks[0].content == "x,y"
        rows = [b for b in doc.content_blocks if b.block_type == "row"]
        assert len(rows) == 2
        assert rows[0].content == "1,2"
        assert rows[1].content == "3,4"

    def test_parse_empty_csv(self):
        doc = CsvParser().parse(b"", "empty.csv")
        assert doc.metadata["row_count"] == 0
        assert doc.metadata["column_count"] == 0
        assert doc.content_blocks == []

    def test_parse_header_only(self):
        content = "a,b,c\n"
        doc = CsvParser().parse(content.encode("utf-8"), "header_only.csv")
        assert doc.metadata["row_count"] == 0
        assert doc.metadata["column_count"] == 3


# ---------------------------------------------------------------------------
# Issue 10: PdfParser
# ---------------------------------------------------------------------------

class TestPdfParser:
    def test_supported_extensions(self):
        parser = PdfParser()
        assert parser.supported_extensions() == [".pdf"]

    def test_parser_is_registered(self):
        auto_discover()
        registry = get_registry()
        parser = registry.get_parser("document.pdf")
        assert isinstance(parser, PdfParser)

    @pytest.mark.skipif(
        not _HAS_PYPDF and not _HAS_DOCLING,
        reason="No PDF parsing library available",
    )
    def test_parse_minimal_pdf(self):
        """Parse a minimal valid PDF created with pypdf."""
        from pypdf import PdfWriter
        import io

        writer = PdfWriter()
        page = writer.add_blank_page(width=200, height=200)
        # pypdf page annotations: add text via a simple page content stream
        # Instead, we just test with a blank page -- content will be empty
        buf = io.BytesIO()
        writer.write(buf)
        pdf_bytes = buf.getvalue()

        parser = PdfParser()
        doc = parser.parse(pdf_bytes, "minimal.pdf")
        assert isinstance(doc, Document)
        assert doc.filename == "minimal.pdf"
        assert doc.file_type == ".pdf"
        assert doc.metadata.get("parser") in ("pypdf", "docling")

    @pytest.mark.skipif(
        not _HAS_PYPDF and not _HAS_DOCLING,
        reason="No PDF parsing library available",
    )
    def test_parse_empty_pdf_bytes(self):
        """Corrupt/empty bytes should raise an exception."""
        parser = PdfParser()
        with pytest.raises(Exception):
            parser.parse(b"not a pdf", "corrupt.pdf")

    def test_no_library_raises_runtime_error(self, monkeypatch):
        """If both libraries are unavailable, raise RuntimeError."""
        import parsebox.ingest.parsers.pdf as pdf_mod
        monkeypatch.setattr(pdf_mod, "_HAS_DOCLING", False)
        monkeypatch.setattr(pdf_mod, "_HAS_PYPDF", False)
        parser = PdfParser()
        with pytest.raises(RuntimeError, match="No PDF parsing library available"):
            parser.parse(b"dummy", "test.pdf")


# ---------------------------------------------------------------------------
# parse_file entry point
# ---------------------------------------------------------------------------

class TestParseFileEntryPoint:
    def test_parse_txt_via_entry_point(self):
        from parsebox.ingest import parse_file
        doc = parse_file(b"hello from entry point", "test.txt")
        assert doc.content == "hello from entry point"
        assert doc.filename == "test.txt"

    def test_parse_md_via_entry_point(self):
        from parsebox.ingest import parse_file
        doc = parse_file(b"# Heading\n\nParagraph.", "test.md")
        assert doc.file_type == ".md"
        headers = [b for b in doc.content_blocks if b.block_type == "header"]
        assert len(headers) == 1

    def test_parse_csv_via_entry_point(self):
        from parsebox.ingest import parse_file
        doc = parse_file(b"a,b\n1,2\n", "test.csv")
        assert doc.metadata["column_count"] == 2

    def test_unsupported_extension_via_entry_point(self):
        from parsebox.ingest import parse_file
        with pytest.raises(ValueError, match="Unsupported file extension"):
            parse_file(b"data", "file.xyz")
