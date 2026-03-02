"""Ingest module -- parse raw files into Document objects.

Usage:
    from parsebox.ingest import parse_file
    doc = parse_file(raw_bytes, "example.txt")
"""

from parsebox.ingest.registry import get_registry, auto_discover


def parse_file(raw_bytes: bytes, filename: str):
    """Parse a file into a Document. Auto-discovers all registered parsers."""
    auto_discover()
    return get_registry().parse_file(raw_bytes, filename)
