"""Default extraction strategy: one LLM call per document via instructor."""

import logging

import instructor
import litellm
from pydantic import create_model

from parsebox.extract.prompt import build_extraction_prompt
from parsebox.models import Document, ExtractionConfig, FieldDefinition, Record, Schema

logger = logging.getLogger(__name__)


def _schema_to_pydantic_fields(fields: list[FieldDefinition]) -> dict:
    """Convert FieldDefinition list to Pydantic field definitions for create_model."""
    pydantic_fields = {}
    type_map = {
        "string": str,
        "number": float,
        "boolean": bool,
        "date": str,
    }

    for field in fields:
        field_type = type_map.get(field.type, str)

        if field.type == "array":
            field_type = list
        elif field.type == "object":
            field_type = dict

        if field.required:
            pydantic_fields[field.name] = (field_type, ...)
        else:
            pydantic_fields[field.name] = (field_type | None, None)

    return pydantic_fields


def extract_single(
    document: Document, schema: Schema, config: ExtractionConfig
) -> Record:
    """Extract data from a single document.

    Returns a Record with status "success" or "failed".
    """
    logger.info(
        "Extracting from document '%s' using schema '%s'",
        document.filename,
        schema.name,
    )

    try:
        prompt = build_extraction_prompt(document.content, schema, config.instructions)

        fields = _schema_to_pydantic_fields(schema.fields)
        DynamicModel = create_model("ExtractedData", **fields)

        client = instructor.from_litellm(litellm.completion)
        result = client.chat.completions.create(
            model=config.llm_provider,
            response_model=DynamicModel,
            messages=[
                {"role": "system", "content": prompt["system"]},
                {"role": "user", "content": prompt["user"]},
            ],
            temperature=config.temperature,
            max_retries=config.max_retries,
            api_key=config.api_key,
        )

        record = Record(
            document_id=document.id,
            document_filename=document.filename,
            data=result.model_dump(),
            status="success",
        )
        logger.info("Successfully extracted from '%s'", document.filename)
        return record

    except Exception as e:
        # Truncate error messages -- instructor can produce very verbose XML dumps
        error_msg = str(e)
        if len(error_msg) > 200:
            error_msg = error_msg[:200] + "..."
        logger.error("Failed to extract from '%s': %s", document.filename, error_msg)
        return Record(
            document_id=document.id,
            document_filename=document.filename,
            data={},
            status="failed",
            errors=[error_msg],
        )
