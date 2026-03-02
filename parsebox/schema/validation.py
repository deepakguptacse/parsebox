"""Schema validation.

Validates a user-edited schema and returns clear error messages.
"""

import logging
import re

from parsebox.models import FieldDefinition, Schema

logger = logging.getLogger(__name__)

VALID_TYPES = {"string", "number", "boolean", "date", "array", "object"}
IDENTIFIER_PATTERN = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")


def validate_schema(schema: Schema) -> list[str]:
    """Validate a schema and return a list of error messages. Empty list means valid.

    Checks:
    - Schema has a name
    - Schema has at least one field
    - Field names are valid Python identifiers
    - Field types are in the allowed set
    - No duplicate field names
    - Array fields have items defined
    - Object fields have properties defined
    - Nested structures are well-formed (recursive)
    """
    errors = []

    if not schema.name or not schema.name.strip():
        errors.append("Schema name is required")

    if not schema.fields:
        errors.append("Schema must have at least one field")
        return errors

    seen_names = set()
    for field in schema.fields:
        errors.extend(_validate_field(field, seen_names, prefix=""))

    logger.info("Schema validation: %d errors found", len(errors))
    return errors


def _validate_field(
    field: FieldDefinition, seen_names: set, prefix: str
) -> list[str]:
    """Validate a single field definition recursively."""
    errors = []
    full_name = f"{prefix}{field.name}" if prefix else field.name

    if not IDENTIFIER_PATTERN.match(field.name):
        errors.append(
            f"Field '{full_name}': name must be a valid identifier "
            "(letters, digits, underscores, not starting with digit)"
        )

    if field.name in seen_names:
        errors.append(f"Field '{full_name}': duplicate field name")
    seen_names.add(field.name)

    if field.type not in VALID_TYPES:
        errors.append(
            f"Field '{full_name}': invalid type '{field.type}'. "
            f"Must be one of: {', '.join(sorted(VALID_TYPES))}"
        )

    if field.type == "array" and field.items is None:
        errors.append(
            f"Field '{full_name}': array type requires 'items' definition"
        )

    if field.type == "object" and not field.properties:
        errors.append(
            f"Field '{full_name}': object type requires 'properties' definition"
        )

    if field.items:
        nested_seen = set()
        errors.extend(
            _validate_field(field.items, nested_seen, prefix=f"{full_name}.items.")
        )

    if field.properties:
        nested_seen = set()
        for prop in field.properties:
            errors.extend(
                _validate_field(prop, nested_seen, prefix=f"{full_name}.")
            )

    return errors


def is_valid(schema: Schema) -> bool:
    """Check if a schema is valid."""
    return len(validate_schema(schema)) == 0
