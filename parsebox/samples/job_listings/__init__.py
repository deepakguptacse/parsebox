import logging
from pathlib import Path
from parsebox.models import Schema, FieldDefinition

logger = logging.getLogger(__name__)

SAMPLE_DIR = Path(__file__).parent


def get_sample_files() -> list[Path]:
    """Return paths to all sample job listing files."""
    files = sorted(SAMPLE_DIR.glob("*.txt"))
    logger.info("Found %d sample job listing files", len(files))
    return files


def get_expected_schema() -> Schema:
    """Return the expected schema for job listing extraction."""
    return Schema(
        name="job_listing_data",
        description="Structured data extracted from job listings",
        fields=[
            FieldDefinition(
                name="company",
                type="string",
                description="Company name",
                required=True,
            ),
            FieldDefinition(
                name="title",
                type="string",
                description="Job title",
                required=True,
            ),
            FieldDefinition(
                name="location",
                type="string",
                description="Job location",
            ),
            FieldDefinition(
                name="salary_min",
                type="number",
                description="Minimum salary",
            ),
            FieldDefinition(
                name="salary_max",
                type="number",
                description="Maximum salary",
            ),
            FieldDefinition(
                name="job_type",
                type="string",
                description="Employment type (Full-time, Part-time, Contract)",
            ),
            FieldDefinition(
                name="requirements",
                type="array",
                description="List of job requirements",
                items=FieldDefinition(
                    name="requirement",
                    type="string",
                    description="Individual requirement",
                ),
            ),
            FieldDefinition(
                name="description",
                type="string",
                description="Job description text",
            ),
        ],
    )
