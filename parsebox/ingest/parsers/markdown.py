"""Markdown parser for .md files.

Extracts structure: headers, paragraphs, and code blocks as ContentBlocks.
"""

import logging
import os
import re

from parsebox.models import Document, ContentBlock
from parsebox.ingest.registry import Parser, get_registry

logger = logging.getLogger(__name__)


class MarkdownParser(Parser):
    """Parse Markdown files into Documents with structural ContentBlocks."""

    def supported_extensions(self) -> list[str]:
        return [".md"]

    def parse(self, raw_bytes: bytes, filename: str) -> Document:
        logger.info("Parsing markdown file '%s'", filename)
        text = raw_bytes.decode("utf-8")
        content_blocks = self._extract_blocks(text)

        ext = os.path.splitext(filename)[1].lower()
        doc = Document(
            filename=filename,
            file_type=ext,
            content=text,
            content_blocks=content_blocks,
            metadata={
                "char_count": len(text),
                "block_count": len(content_blocks),
            },
            raw_bytes=raw_bytes,
        )
        logger.info(
            "Parsed '%s': %d content blocks, %d chars",
            filename, len(content_blocks), len(text),
        )
        return doc

    def _extract_blocks(self, text: str) -> list[ContentBlock]:
        """Split markdown text into typed content blocks."""
        blocks: list[ContentBlock] = []
        lines = text.split("\n")
        i = 0

        while i < len(lines):
            line = lines[i]

            # Fenced code block
            if line.strip().startswith("```"):
                lang = line.strip().removeprefix("```").strip()
                code_lines = []
                i += 1
                while i < len(lines) and not lines[i].strip().startswith("```"):
                    code_lines.append(lines[i])
                    i += 1
                # skip closing ```
                i += 1
                blocks.append(ContentBlock(
                    block_type="code",
                    content="\n".join(code_lines),
                    metadata={"language": lang} if lang else {},
                ))
                continue

            # Header
            header_match = re.match(r"^(#{1,6})\s+(.*)", line)
            if header_match:
                level = len(header_match.group(1))
                blocks.append(ContentBlock(
                    block_type="header",
                    content=header_match.group(2).strip(),
                    metadata={"level": level},
                ))
                i += 1
                continue

            # Blank line -- skip
            if not line.strip():
                i += 1
                continue

            # Paragraph: collect contiguous non-blank, non-special lines
            para_lines = []
            while i < len(lines) and lines[i].strip() and not re.match(r"^#{1,6}\s+", lines[i]) and not lines[i].strip().startswith("```"):
                para_lines.append(lines[i])
                i += 1
            if para_lines:
                blocks.append(ContentBlock(
                    block_type="paragraph",
                    content="\n".join(para_lines),
                ))

        return blocks


# Auto-register
get_registry().register(MarkdownParser())
