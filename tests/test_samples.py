"""Tests for parsebox sample datasets."""

import pytest

from parsebox.models import Schema, FieldDefinition, VALID_FIELD_TYPES
from parsebox.samples import (
    get_sample_dataset_info,
    load_sample_files,
    get_sample_schema,
    SAMPLE_DATASETS,
)
from parsebox.samples import invoices, research_papers, job_listings


class TestInvoiceSamples:
    def test_sample_files_exist(self):
        files = invoices.get_sample_files()
        for f in files:
            assert f.exists(), f"Sample file does not exist: {f}"

    def test_sample_files_count(self):
        files = invoices.get_sample_files()
        assert len(files) == 5

    def test_sample_files_are_txt(self):
        files = invoices.get_sample_files()
        for f in files:
            assert f.suffix == ".txt"

    def test_sample_files_not_empty(self):
        files = invoices.get_sample_files()
        for f in files:
            assert f.stat().st_size > 0, f"Sample file is empty: {f}"

    def test_expected_schema_is_valid(self):
        schema = invoices.get_expected_schema()
        assert isinstance(schema, Schema)
        assert schema.name == "invoice_data"
        assert len(schema.fields) > 0

    def test_schema_field_types_valid(self):
        schema = invoices.get_expected_schema()
        for field in schema.fields:
            assert field.type in VALID_FIELD_TYPES


class TestResearchPaperSamples:
    def test_sample_files_exist(self):
        files = research_papers.get_sample_files()
        for f in files:
            assert f.exists(), f"Sample file does not exist: {f}"

    def test_sample_files_count(self):
        files = research_papers.get_sample_files()
        assert len(files) == 5

    def test_sample_files_are_txt(self):
        files = research_papers.get_sample_files()
        for f in files:
            assert f.suffix == ".txt"

    def test_sample_files_not_empty(self):
        files = research_papers.get_sample_files()
        for f in files:
            assert f.stat().st_size > 0, f"Sample file is empty: {f}"

    def test_expected_schema_is_valid(self):
        schema = research_papers.get_expected_schema()
        assert isinstance(schema, Schema)
        assert schema.name == "research_paper_data"
        assert len(schema.fields) > 0

    def test_schema_field_types_valid(self):
        schema = research_papers.get_expected_schema()
        for field in schema.fields:
            assert field.type in VALID_FIELD_TYPES


class TestJobListingSamples:
    def test_sample_files_exist(self):
        files = job_listings.get_sample_files()
        for f in files:
            assert f.exists(), f"Sample file does not exist: {f}"

    def test_sample_files_count(self):
        files = job_listings.get_sample_files()
        assert len(files) == 5

    def test_sample_files_are_txt(self):
        files = job_listings.get_sample_files()
        for f in files:
            assert f.suffix == ".txt"

    def test_sample_files_not_empty(self):
        files = job_listings.get_sample_files()
        for f in files:
            assert f.stat().st_size > 0, f"Sample file is empty: {f}"

    def test_expected_schema_is_valid(self):
        schema = job_listings.get_expected_schema()
        assert isinstance(schema, Schema)
        assert schema.name == "job_listing_data"
        assert len(schema.fields) > 0

    def test_schema_field_types_valid(self):
        schema = job_listings.get_expected_schema()
        for field in schema.fields:
            assert field.type in VALID_FIELD_TYPES


class TestSamplesTopLevel:
    def test_get_sample_dataset_info_returns_three(self):
        info = get_sample_dataset_info()
        assert len(info) == 3

    def test_get_sample_dataset_info_has_expected_keys(self):
        info = get_sample_dataset_info()
        for entry in info:
            assert "key" in entry
            assert "name" in entry
            assert "description" in entry

    def test_get_sample_dataset_info_keys(self):
        info = get_sample_dataset_info()
        keys = {entry["key"] for entry in info}
        assert keys == {"invoices", "research_papers", "job_listings"}

    def test_load_sample_files_invoices(self):
        files = load_sample_files("invoices")
        assert len(files) == 5

    def test_load_sample_files_research_papers(self):
        files = load_sample_files("research_papers")
        assert len(files) == 5

    def test_load_sample_files_job_listings(self):
        files = load_sample_files("job_listings")
        assert len(files) == 5

    def test_load_sample_files_unknown_key_raises(self):
        with pytest.raises(ValueError, match="Unknown sample dataset"):
            load_sample_files("nonexistent")

    def test_get_sample_schema_all_keys(self):
        for key in SAMPLE_DATASETS:
            schema = get_sample_schema(key)
            assert isinstance(schema, Schema)
            assert len(schema.fields) > 0

    def test_get_sample_schema_unknown_key_raises(self):
        with pytest.raises(ValueError, match="Unknown sample dataset"):
            get_sample_schema("nonexistent")
