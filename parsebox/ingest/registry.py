"""Parser protocol and registry for the ingest module.

The registry maps file extensions to parser implementations.
Adding a new file type means implementing the Parser ABC and registering it.
"""

import abc
import logging
import os

from parsebox.models import Document

logger = logging.getLogger(__name__)


class Parser(abc.ABC):
    """Abstract base class that all file parsers must implement."""

    @abc.abstractmethod
    def supported_extensions(self) -> list[str]:
        """Return list of file extensions this parser handles (e.g., ['.txt'])."""
        ...

    @abc.abstractmethod
    def parse(self, raw_bytes: bytes, filename: str) -> Document:
        """Parse raw file bytes into a Document."""
        ...


class ParserRegistry:
    """Maps file extensions to Parser instances and dispatches parsing."""

    def __init__(self):
        self._parsers: dict[str, Parser] = {}

    def register(self, parser: Parser) -> None:
        """Register a parser for its supported extensions."""
        for ext in parser.supported_extensions():
            ext = ext.lower()
            logger.info("Registering parser %s for extension '%s'", type(parser).__name__, ext)
            self._parsers[ext] = parser

    def get_parser(self, filename: str) -> Parser:
        """Look up parser by file extension. Raise ValueError if unsupported."""
        ext = os.path.splitext(filename)[1].lower()
        if ext not in self._parsers:
            raise ValueError(
                f"Unsupported file extension '{ext}'. "
                f"Supported: {', '.join(sorted(self._parsers.keys()))}"
            )
        return self._parsers[ext]

    def supported_extensions(self) -> list[str]:
        """Return all registered extensions."""
        return sorted(self._parsers.keys())

    def parse_file(self, raw_bytes: bytes, filename: str) -> Document:
        """Look up the right parser and parse the file."""
        logger.info("Parsing file '%s' (%d bytes)", filename, len(raw_bytes))
        if not raw_bytes:
            logger.warning("Empty file received: '%s'", filename)
        parser = self.get_parser(filename)
        try:
            document = parser.parse(raw_bytes, filename)
        except Exception as e:
            logger.error("Parser error for '%s': %s", filename, e)
            raise ValueError(
                f"Failed to parse '{filename}': {e}. "
                "The file may be corrupt or not a valid file of its declared type."
            ) from e
        logger.info("Successfully parsed '%s' into Document id=%s", filename, document.id)
        return document


# Global registry instance
_registry = ParserRegistry()


def get_registry() -> ParserRegistry:
    """Return the global parser registry."""
    return _registry


def auto_discover():
    """Import all parser modules to trigger registration."""
    from parsebox.ingest.parsers import plaintext, markdown, csv_parser, pdf  # noqa: F401
