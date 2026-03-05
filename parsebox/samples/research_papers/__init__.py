import logging
from pathlib import Path
from parsebox.models import Schema, FieldDefinition

logger = logging.getLogger(__name__)

SAMPLE_DIR = Path(__file__).parent


def get_sample_files() -> list[Path]:
    """Return paths to all sample research paper files."""
    files = sorted(SAMPLE_DIR.glob("*.txt"))
    logger.info("Found %d sample research paper files", len(files))
    return files


def get_expected_schema() -> Schema:
    """Return the expected schema for research paper extraction."""
    return Schema(
        name="research_paper_data",
        description="Structured data extracted from research paper abstracts",
        fields=[
            FieldDefinition(
                name="title",
                type="string",
                description="Title of the research paper",
                required=True,
            ),
            FieldDefinition(
                name="authors",
                type="array",
                description="List of author names",
                items=FieldDefinition(
                    name="author",
                    type="string",
                    description="Author name",
                ),
            ),
            FieldDefinition(
                name="abstract",
                type="string",
                description="Paper abstract text",
                required=True,
            ),
            FieldDefinition(
                name="year",
                type="number",
                description="Publication year",
                required=True,
            ),
            FieldDefinition(
                name="journal",
                type="string",
                description="Journal or venue name",
            ),
            FieldDefinition(
                name="keywords",
                type="array",
                description="List of keywords",
                items=FieldDefinition(
                    name="keyword",
                    type="string",
                    description="Keyword",
                ),
            ),
            FieldDefinition(
                name="doi",
                type="string",
                description="Digital Object Identifier",
            ),
        ],
    )
