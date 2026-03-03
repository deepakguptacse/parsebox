"""Build extraction prompts from document content, schema, and user instructions."""

import logging

from parsebox.models import FieldDefinition, Schema

logger = logging.getLogger(__name__)


def build_extraction_prompt(
    document_content: str, schema: Schema, instructions: str = ""
) -> dict:
    """Build the extraction prompt messages for the LLM.

    Args:
        document_content: The text content of the document
        schema: The schema defining what fields to extract
        instructions: Optional user instructions/feedback

    Returns:
        dict with "system" and "user" message strings
    """
    schema_desc = _format_schema(schema)

    system_msg = f"""You are a precise data extraction assistant. Extract structured data from the provided document according to the schema below.

Schema: {schema.name}
{schema.description}

Fields to extract:
{schema_desc}

Rules:
- Extract exactly the fields defined in the schema
- Use null for fields that cannot be found in the document
- For required fields, make your best effort to extract a value
- Match the specified types exactly (string, number, boolean, date, array, object)
- For dates, use ISO format (YYYY-MM-DD) unless the description says otherwise"""

    if instructions:
        system_msg += f"\n\nAdditional instructions from the user:\n{instructions}"

    user_msg = f"""Extract data from this document:

{document_content}"""

    logger.info(
        "Built extraction prompt for schema '%s' with %d fields",
        schema.name,
        len(schema.fields),
    )
    return {"system": system_msg, "user": user_msg}


def _format_schema(schema: Schema) -> str:
    """Format schema fields into a readable description for the prompt."""
    lines = []
    for field in schema.fields:
        lines.append(_format_field(field, indent=0))
    return "\n".join(lines)


def _format_field(field: FieldDefinition, indent: int) -> str:
    """Format a single field definition recursively."""
    prefix = "  " * indent
    required = " (REQUIRED)" if field.required else ""
    line = f"{prefix}- {field.name}: {field.type}{required}"
    if field.description:
        line += f" -- {field.description}"

    if field.items:
        line += f"\n{_format_field(field.items, indent + 1)}"

    if field.properties:
        for prop in field.properties:
            line += f"\n{_format_field(prop, indent + 1)}"

    return line
