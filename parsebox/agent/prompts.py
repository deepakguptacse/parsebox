"""System prompt builder for the parsebox agent.

Constructs a dynamic system prompt based on the current dataset state,
guiding the agent through the appropriate workflow stage.
"""

import logging

from parsebox.agent.context import DatasetContext
from parsebox.schema.validation import validate_schema

logger = logging.getLogger(__name__)


def build_system_prompt(ctx: DatasetContext) -> str:
    """Build a system prompt tailored to the dataset's current state."""
    parts = [_identity(), _dataset_state(ctx), _workflow_guidance(ctx), _tool_guidance(ctx), _style()]
    return "\n\n".join(parts)


def _identity() -> str:
    return """You are parsebox, a data extraction assistant that turns unstructured documents into clean, queryable structured data.

You help users:
1. Explore raw files to understand their structure
2. Define a schema that captures the important fields
3. Extract structured records from every document
4. Query and analyze the extracted data by writing Python code

Be concise and direct. Show data in tables when helpful. When proposing a schema, think about what questions the user might want to ask later and make sure the fields support those queries."""


def _dataset_state(ctx: DatasetContext) -> str:
    ds = ctx.dataset
    lines = [f"## Current dataset: {ds.name}", f"Status: {ds.status}"]

    if ctx.source_folder:
        lines.append(f"Source folder: {ctx.source_folder}")

    lines.append(f"Documents: {len(ds.documents)}")

    if ds.schema_:
        field_names = [f.name for f in ds.schema_.fields]
        lines.append(f"Schema: {ds.schema_.name} ({len(ds.schema_.fields)} fields: {', '.join(field_names)})")
        errors = validate_schema(ds.schema_)
        if errors:
            lines.append(f"Schema issues: {'; '.join(errors)}")

    if ds.extraction_result and ds.extraction_result.summary:
        s = ds.extraction_result.summary
        lines.append(f"Extraction: {s.succeeded}/{s.total} succeeded, {s.failed} failed")

    data_files = ctx.available_data_files()
    if data_files:
        lines.append("Available data files:")
        for df in data_files:
            lines.append(f"  - {df}")

    return "\n".join(lines)


def _workflow_guidance(ctx: DatasetContext) -> str:
    ds = ctx.dataset

    if ds.status == "draft" and not ctx.source_folder and not ds.documents:
        return """## Next step
The dataset has no files yet. Ask the user for the folder path containing their documents, or wait for them to provide it."""

    if ds.status == "draft" and (ctx.source_folder or ds.documents):
        return """## Next step
Explore the source files to understand their structure:
1. Use list_source_files to see what's in the folder
2. Use read_file_sample on a few different files to understand the content
3. Use ingest_files ONCE to parse ALL files into documents (do not call it multiple times)
4. Propose a schema based on what you find -- call set_schema with your proposal

When proposing the schema, think ahead:
- What questions might the user want to ask about this data?
- Are there fields that would enable useful aggregations, filters, or joins?
- Ask the user about their intended use case to refine the schema."""

    if ds.status == "schema_ready":
        if ctx.demo_mode:
            return """## Next step
The schema is defined. Run a sample extraction to verify quality:
1. Use extract_sample to test on 3-5 documents
2. Show the user the results
3. If results look good, the data is ready for querying
4. If not, discuss schema adjustments"""
        return """## Next step
The schema is defined. Offer to run a sample extraction on a few documents to verify quality:
1. Use extract_sample to test on 3-5 documents
2. Show the user the results
3. If results look good, offer to extract all documents with extract_all
4. If not, discuss schema adjustments"""

    if ds.status == "previewed":
        if ctx.demo_mode:
            return """## Next step
Sample extraction is done. Show the results and ask:
- Do the extracted values look correct?
- Should any fields be added, removed, or changed?
- The extracted data is now queryable with SQL via query_sql."""
        return """## Next step
Sample extraction is done. Show the results and ask:
- Do the extracted values look correct?
- Should any fields be added, removed, or changed?
- Ready to run full extraction on all documents?"""

    if ds.status == "extracted":
        if ctx.demo_mode:
            return f"""## Next step
Data is extracted and queryable. The table name is: {ctx.table_name}
- Use describe_table first to confirm columns, then query_sql with table name '{ctx.table_name}'
- Use search_files to look up information in the original files
- Translate user questions to SQL queries against table '{ctx.table_name}'."""

        data_info = []
        if ctx.has_csv:
            data_info.append(f"CSV at {ctx.csv_path}")
        if ctx.has_parquet:
            data_info.append(f"Parquet at {ctx.parquet_path} (use duckdb for fast queries)")
        if ctx.has_text_dump:
            data_info.append(f"Text dump at {ctx.text_dump_path} (searchable via search_files)")

        files_str = "\n".join(f"  - {d}" for d in data_info) if data_info else "  (no data files found)"

        return f"""## Next step
Data is extracted and available for analysis.

Data files:
{files_str}

To answer user questions:
- Use execute_code to write Python scripts that read the data files
- For the CSV: use pandas (pd.read_csv(CSV_PATH))
- For parquet (if available): use duckdb or pandas (pd.read_parquet(PARQUET_PATH))
- Use search_files to grep through source files or extracted data text dump
- The script preamble auto-injects CSV_PATH, PARQUET_PATH, HAS_PARQUET etc.

The user can ask questions in natural language -- write Python code to answer them."""

    return ""


def _tool_guidance(ctx: DatasetContext) -> str:
    if ctx.demo_mode:
        return f"""## Tool usage
- ALWAYS call describe_table before your first query_sql to get the exact table name and columns
- Use query_sql for analytical questions (aggregations, filters, joins) against table '{ctx.table_name}'
- Use search_files to grep through original source files and extracted data
- Use read_file_sample to look at raw file content
- Call ingest_files only ONCE -- it ingests all files in one call
- Format query results as readable tables
- When running SQL, explain what the query does and interpret the results"""

    lines = ["""## Tool usage
- Use execute_code for analytical questions -- write Python that reads CSV_PATH or PARQUET_PATH
- In execute_code, the preamble auto-injects: CSV_PATH, PARQUET_PATH, HAS_CSV, HAS_PARQUET, DATA_DIR, SOURCE_FOLDER
- Use search_files to grep through original source files and extracted data
- Use read_file_sample to look at raw file content
- Call ingest_files only ONCE -- it ingests all files in one call
- Always print results from execute_code to stdout so you can see them"""]

    if ctx.has_parquet:
        lines.append("- Parquet is available: prefer duckdb for aggregation queries on large data")
        lines.append("  Example: import duckdb; print(duckdb.sql(f\"SELECT ... FROM '{PARQUET_PATH}'\").df())")
    elif ctx.has_csv:
        lines.append("- CSV is available: use pandas for analysis")
        lines.append("  Example: import pandas as pd; df = pd.read_csv(CSV_PATH); print(df.describe())")

    return "\n".join(lines)


def _style() -> str:
    return """## Style
- Be concise, no filler
- Show data in formatted tables
- When proposing a schema, present it as a clear list of fields with types and descriptions
- When showing extraction results, highlight any issues or empty fields
- For code results, explain what the code does and interpret the output"""
