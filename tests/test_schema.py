"""Tests for schema validation."""

import pytest

from parsebox.models import FieldDefinition, Schema
from parsebox.schema.validation import is_valid, validate_schema


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_field(name="field_a", type_="string", **kwargs):
    return FieldDefinition(name=name, type=type_, **kwargs)


def _make_schema(name="test_schema", fields=None):
    return Schema(name=name, fields=fields or [_make_field()])



# ===========================================================================
# Validation tests
# ===========================================================================


class TestValidateSchema:
    """Tests for validate_schema."""

    def test_valid_schema_passes(self):
        schema = _make_schema(
            fields=[
                _make_field("vendor_name", "string", required=True),
                _make_field("total", "number"),
            ]
        )
        errors = validate_schema(schema)
        assert errors == []

    def test_empty_name_fails(self):
        schema = Schema(name="", fields=[_make_field()])
        errors = validate_schema(schema)
        assert any("name is required" in e for e in errors)

    def test_whitespace_only_name_fails(self):
        schema = Schema(name="   ", fields=[_make_field()])
        errors = validate_schema(schema)
        assert any("name is required" in e for e in errors)

    def test_no_fields_fails(self):
        schema = Schema(name="empty", fields=[])
        errors = validate_schema(schema)
        assert any("at least one field" in e for e in errors)

    def test_invalid_field_name_digit_start(self):
        schema = _make_schema(fields=[_make_field("1bad")])
        errors = validate_schema(schema)
        assert any("valid identifier" in e for e in errors)

    def test_invalid_field_name_special_chars(self):
        schema = _make_schema(fields=[_make_field("field-name")])
        errors = validate_schema(schema)
        assert any("valid identifier" in e for e in errors)

    def test_invalid_field_name_space(self):
        schema = _make_schema(fields=[_make_field("field name")])
        errors = validate_schema(schema)
        assert any("valid identifier" in e for e in errors)

    def test_valid_field_name_with_underscore(self):
        schema = _make_schema(fields=[_make_field("_private_field")])
        errors = validate_schema(schema)
        assert errors == []

    def test_duplicate_field_names_fail(self):
        schema = _make_schema(
            fields=[_make_field("amount"), _make_field("amount")]
        )
        errors = validate_schema(schema)
        assert any("duplicate" in e for e in errors)

    def test_array_without_items_fails(self):
        schema = _make_schema(fields=[_make_field("tags", "array")])
        errors = validate_schema(schema)
        assert any("items" in e for e in errors)

    def test_array_with_items_passes(self):
        schema = _make_schema(
            fields=[
                _make_field(
                    "tags", "array", items=_make_field("tag_item", "string")
                )
            ]
        )
        errors = validate_schema(schema)
        assert errors == []

    def test_object_without_properties_fails(self):
        schema = _make_schema(fields=[_make_field("address", "object")])
        errors = validate_schema(schema)
        assert any("properties" in e for e in errors)

    def test_object_with_properties_passes(self):
        schema = _make_schema(
            fields=[
                _make_field(
                    "address",
                    "object",
                    properties=[
                        _make_field("street", "string"),
                        _make_field("city", "string"),
                    ],
                )
            ]
        )
        errors = validate_schema(schema)
        assert errors == []

    def test_nested_object_validation(self):
        """Nested object properties are validated recursively."""
        schema = _make_schema(
            fields=[
                _make_field(
                    "address",
                    "object",
                    properties=[
                        _make_field("1street", "string"),  # invalid name
                    ],
                )
            ]
        )
        errors = validate_schema(schema)
        assert any("valid identifier" in e for e in errors)
        assert any("address." in e for e in errors)

    def test_nested_array_items_validation(self):
        """Array items are validated recursively."""
        schema = _make_schema(
            fields=[
                _make_field(
                    "items_list",
                    "array",
                    items=_make_field("nested_arr", "array"),  # array without items
                )
            ]
        )
        errors = validate_schema(schema)
        assert any("items" in e for e in errors)

    def test_nested_duplicate_in_properties(self):
        """Duplicate names within an object's properties are caught."""
        schema = _make_schema(
            fields=[
                _make_field(
                    "address",
                    "object",
                    properties=[
                        _make_field("city", "string"),
                        _make_field("city", "string"),
                    ],
                )
            ]
        )
        errors = validate_schema(schema)
        assert any("duplicate" in e for e in errors)

    def test_multiple_errors_returned(self):
        """Multiple validation errors are all reported."""
        schema = Schema(
            name="",
            fields=[
                _make_field("1bad"),
                _make_field("tags", "array"),  # missing items
            ],
        )
        errors = validate_schema(schema)
        assert len(errors) >= 3  # name, identifier, items

    def test_deeply_nested_validation(self):
        """Deep nesting: object -> array -> object is validated."""
        schema = _make_schema(
            fields=[
                _make_field(
                    "orders",
                    "array",
                    items=_make_field(
                        "order",
                        "object",
                        properties=[
                            _make_field("product", "string"),
                            _make_field("quantity", "number"),
                        ],
                    ),
                )
            ]
        )
        errors = validate_schema(schema)
        assert errors == []


class TestIsValid:
    """Tests for the is_valid convenience function."""

    def test_valid_schema_returns_true(self):
        schema = _make_schema()
        assert is_valid(schema) is True

    def test_invalid_schema_returns_false(self):
        schema = Schema(name="", fields=[])
        assert is_valid(schema) is False

    def test_returns_bool(self):
        result = is_valid(_make_schema())
        assert isinstance(result, bool)


