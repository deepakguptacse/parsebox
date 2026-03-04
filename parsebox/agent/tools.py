"""Tool definitions for the parsebox agent.

Each tool is a thin wrapper around existing parsebox modules.
Tools are created via a factory that closes over a DatasetContext,
so the agent can only operate on the bound dataset.
"""

import json
import logging
import os
import re
import subprocess
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Any

from claude_agent_sdk import tool

from parsebox.agent.context import DatasetContext
from parsebox.ingest import parse_file
from parsebox.models import (
    Document,
    ExtractionConfig,
    ExtractionResult,
    ExtractionSummary,
    FieldDefinition,
    Record,
    Schema,
)
from parsebox.schema.validation import validate_schema
from parsebox.store import write_records

logger = logging.getLogger(__name__)

# -- Helpers ------------------------------------------------------------------


def _text_result(text: str) -> dict[str, Any]:
    return {"content": [{"type": "text", "text": text}]}


def _error_result(text: str) -> dict[str, Any]:
    return {"content": [{"type": "text", "text": f"Error: {text}"}], "is_error": True}


def _format_rows_as_table(columns: list[str], rows: list[list]) -> str:
    """Format rows into a simple markdown table."""
    if not rows:
        return "(no rows)"
    col_widths = [len(c) for c in columns]
    for row in rows:
        for i, val in enumerate(row):
            if i < len(col_widths):
                col_widths[i] = max(col_widths[i], len(str(val)))
    header = " | ".join(c.ljust(w) for c, w in zip(columns, col_widths))
    sep = "-|-".join("-" * w for w in col_widths)
    body_lines = []
    for row in rows:
        cells = [str(v).ljust(w) for v, w in zip(row, col_widths)]
        body_lines.append(" | ".join(cells))
    return f"{header}\n{sep}\n" + "\n".join(body_lines)


def _build_text_dump(records: list[Record], schema: Schema) -> str:
    """Build a grep-friendly text dump of all extracted records.

    Format:
        === Record 1: filename.txt ===
        field_name: value
        ...

        === Record 2: filename2.pdf ===
        ...
    """
    lines = []
    for i, record in enumerate(records, 1):
        if record.status == "failed":
            continue
        lines.append(f"=== Record {i}: {record.document_filename} ===")
        for field_def in schema.fields:
            value = record.data.get(field_def.name, "")
            lines.append(f"{field_def.name}: {value}")
        lines.append("")
    return "\n".join(lines)


# -- Factory ------------------------------------------------------------------


def create_dataset_tools(ctx: DatasetContext) -> list:
    """Create all tools bound to a specific DatasetContext.

    Tools are partitioned by what phase the dataset is in,
    but we always register all of them so the agent can reference
    any capability in conversation.
    """

    # -- File exploration tools -----------------------------------------------

    @tool(
        "list_source_files",
        "List files in the dataset's source folder with sizes and extensions",
        {"pattern": str},
    )
    async def list_source_files(args: dict[str, Any]) -> dict[str, Any]:
        folder = ctx.source_folder
        if not folder:
            return _error_result("No source folder configured for this dataset.")

        folder_path = Path(folder)
        if not folder_path.exists():
            return _error_result(f"Source folder not found: {folder}")

        pattern = args.get("pattern", "*")
        files = sorted(folder_path.glob(pattern))
        if not files:
            files = sorted(folder_path.rglob(pattern))

        entries = []
        for f in files:
            if f.is_file():
                size = f.stat().st_size
                if size < 1024:
                    size_str = f"{size} B"
                elif size < 1024 * 1024:
                    size_str = f"{size / 1024:.1f} KB"
                else:
                    size_str = f"{size / (1024 * 1024):.1f} MB"
                entries.append(f"  {f.name:<40} {f.suffix or '(none)':<10} {size_str}")

        result = f"Source folder: {folder}\nFiles ({len(entries)}):\n" + "\n".join(entries)
        logger.info("Listed %d files from %s", len(entries), folder)
        return _text_result(result)

    @tool(
        "read_file_sample",
        "Read the first N lines of a source file to understand its content",
        {"filename": str, "max_lines": int},
    )
    async def read_file_sample(args: dict[str, Any]) -> dict[str, Any]:
        folder = ctx.source_folder
        if not folder:
            return _error_result("No source folder configured.")

        filename = args["filename"]
        max_lines = args.get("max_lines", 50)

        file_path = Path(folder) / filename
        if not file_path.exists():
            # Try recursive search
            matches = list(Path(folder).rglob(filename))
            if matches:
                file_path = matches[0]
            else:
                return _error_result(f"File not found: {filename}")

        try:
            content = file_path.read_text(errors="replace")
            lines = content.splitlines()
            sample = "\n".join(lines[:max_lines])
            truncated = f"\n... ({len(lines) - max_lines} more lines)" if len(lines) > max_lines else ""
            result = f"File: {file_path.name} ({len(lines)} lines)\n---\n{sample}{truncated}"
            return _text_result(result)
        except Exception:
            # Binary file -- show hex preview
            raw = file_path.read_bytes()[:512]
            return _text_result(f"File: {file_path.name} (binary, {file_path.stat().st_size} bytes)\nHex preview: {raw[:64].hex()}")

    @tool(
        "get_file_info",
        "Get metadata about a specific source file",
        {"filename": str},
    )
    async def get_file_info(args: dict[str, Any]) -> dict[str, Any]:
        folder = ctx.source_folder
        if not folder:
            return _error_result("No source folder configured.")

        file_path = Path(folder) / args["filename"]
        if not file_path.exists():
            matches = list(Path(folder).rglob(args["filename"]))
            if matches:
                file_path = matches[0]
            else:
                return _error_result(f"File not found: {args['filename']}")

        stat = file_path.stat()
        try:
            line_count = len(file_path.read_text(errors="replace").splitlines())
        except Exception:
            line_count = None

        info = {
            "name": file_path.name,
            "extension": file_path.suffix,
            "size_bytes": stat.st_size,
            "modified": datetime.fromtimestamp(stat.st_mtime).isoformat(),
            "line_count": line_count,
        }
        return _text_result(json.dumps(info, indent=2))

    # -- Ingest tools ---------------------------------------------------------

    @tool(
        "ingest_files",
        "Parse ALL files from the source folder into documents. Call this once -- it ingests everything. Optionally filter by extension.",
        {"extensions": str},
    )
    async def ingest_files(args: dict[str, Any]) -> dict[str, Any]:
        folder = ctx.source_folder
        if not folder:
            return _error_result("No source folder configured.")

        # Prevent duplicate ingestion
        ctx.reload_dataset()
        if ctx.dataset.documents:
            existing = {d.filename for d in ctx.dataset.documents}
            return _text_result(
                f"Already ingested {len(existing)} documents: {', '.join(sorted(existing))}. "
                "No need to ingest again."
            )

        folder_path = Path(folder)
        ext_filter = args.get("extensions", "")
        allowed_exts = set()
        if ext_filter:
            for ext in ext_filter.split(","):
                ext = ext.strip().lower()
                if not ext.startswith("."):
                    ext = f".{ext}"
                allowed_exts.add(ext)

        files = sorted(folder_path.rglob("*"))
        files = [f for f in files if f.is_file()]
        if allowed_exts:
            files = [f for f in files if f.suffix.lower() in allowed_exts]

        docs = []
        errors = []
        for f in files:
            try:
                raw = f.read_bytes()
                doc = parse_file(raw, f.name)
                docs.append(doc)
            except Exception as e:
                errors.append(f"{f.name}: {e}")
                logger.warning("Failed to parse %s: %s", f.name, e)

        if docs:
            ctx.dataset = ctx.manager.add_documents(ctx.dataset.id, ctx.user_id, docs)

        lines = [f"Ingested {len(docs)} documents from {folder}"]
        if errors:
            lines.append(f"Failed ({len(errors)}):")
            for err in errors[:10]:
                lines.append(f"  - {err}")
        if docs:
            lines.append(f"\nDocuments now in dataset: {len(ctx.dataset.documents)}")
        return _text_result("\n".join(lines))

    @tool(
        "list_documents",
        "List all ingested documents in the current dataset",
        {},
    )
    async def list_documents(args: dict[str, Any]) -> dict[str, Any]:
        ctx.reload_dataset()
        docs = ctx.dataset.documents
        if not docs:
            return _text_result("No documents ingested yet.")

        lines = [f"Documents ({len(docs)}):"]
        for i, doc in enumerate(docs):
            char_count = len(doc.content)
            lines.append(f"  {i+1}. {doc.filename:<40} {doc.file_type:<8} {char_count:>6} chars")
        return _text_result("\n".join(lines))

    @tool(
        "read_document",
        "Read the parsed content of a specific document by index (1-based) or filename",
        {"document": str, "max_chars": int},
    )
    async def read_document(args: dict[str, Any]) -> dict[str, Any]:
        ctx.reload_dataset()
        docs = ctx.dataset.documents
        doc_ref = args["document"]
        max_chars = args.get("max_chars", 3000)

        doc = None
        # Try as 1-based index
        try:
            idx = int(doc_ref) - 1
            if 0 <= idx < len(docs):
                doc = docs[idx]
        except ValueError:
            pass

        # Try as filename
        if doc is None:
            for d in docs:
                if d.filename == doc_ref:
                    doc = d
                    break

        if doc is None:
            return _error_result(f"Document not found: {doc_ref}")

        content = doc.content[:max_chars]
        truncated = f"\n... ({len(doc.content) - max_chars} more chars)" if len(doc.content) > max_chars else ""
        return _text_result(f"Document: {doc.filename} ({doc.file_type})\n---\n{content}{truncated}")

    # -- Schema tools ---------------------------------------------------------

    @tool(
        "get_schema",
        "Get the current schema for this dataset",
        {},
    )
    async def get_schema(args: dict[str, Any]) -> dict[str, Any]:
        ctx.reload_dataset()
        schema = ctx.dataset.schema_
        if not schema:
            return _text_result("No schema defined yet.")

        lines = [f"Schema: {schema.name}", f"Description: {schema.description}", f"Fields ({len(schema.fields)}):"]
        for f in schema.fields:
            req = " (required)" if f.required else ""
            lines.append(f"  - {f.name}: {f.type}{req}")
            if f.description:
                lines.append(f"    {f.description}")
            if f.items:
                lines.append(f"    items: {f.items.name} ({f.items.type})")
            if f.properties:
                for p in f.properties:
                    lines.append(f"    .{p.name}: {p.type} -- {p.description}")
        return _text_result("\n".join(lines))

    @tool(
        "set_schema",
        "Set or update the dataset schema. Pass the full schema as JSON.",
        {"schema_json": str},
    )
    async def set_schema(args: dict[str, Any]) -> dict[str, Any]:
        try:
            schema_data = json.loads(args["schema_json"])
            schema = Schema.model_validate(schema_data)
        except Exception as e:
            return _error_result(f"Invalid schema JSON: {e}")

        errors = validate_schema(schema)
        if errors:
            return _error_result("Schema validation failed:\n" + "\n".join(f"  - {e}" for e in errors))

        ctx.dataset = ctx.manager.update_schema(ctx.dataset.id, ctx.user_id, schema)

        # Transition status if we have documents
        if ctx.dataset.status == "draft" and ctx.dataset.documents:
            ctx.dataset = ctx.manager.update_status(ctx.dataset.id, ctx.user_id, "schema_ready")

        logger.info("Schema set: %s (%d fields)", schema.name, len(schema.fields))
        return _text_result(f"Schema '{schema.name}' saved with {len(schema.fields)} fields. Status: {ctx.dataset.status}")

    # -- Extraction tools -----------------------------------------------------

    @tool(
        "extract_sample",
        "Extract data from a sample of documents (3-5) to preview results before full extraction",
        {"sample_size": int},
    )
    async def extract_sample(args: dict[str, Any]) -> dict[str, Any]:
        ctx.reload_dataset()
        ds = ctx.dataset
        if not ds.schema_ or not ds.schema_.fields:
            return _error_result("No schema defined. Set a schema first.")
        if not ds.documents:
            return _error_result("No documents ingested. Ingest files first.")

        api_key = os.environ.get("ANTHROPIC_API_KEY", "")
        if not api_key:
            return _error_result("ANTHROPIC_API_KEY not set in environment.")

        config = ExtractionConfig(
            llm_provider="anthropic/claude-sonnet-4-20250514",
            api_key=api_key,
            temperature=0.0,
            max_retries=2,
        )

        # Filter to single-record documents for sampling
        _SKIP_TYPES = {"csv", "md", "markdown"}
        extractable = [d for d in ds.documents if d.file_type.lower() not in _SKIP_TYPES]

        sample_size = args.get("sample_size", 3)
        sample = extractable[:sample_size]

        from parsebox.extract.strategies.default import extract_single

        records = []
        for doc in sample:
            record = extract_single(doc, ds.schema_, config)
            records.append(record)

        succeeded = sum(1 for r in records if r.status == "success")
        failed = sum(1 for r in records if r.status == "failed")

        result = ExtractionResult(
            records=records,
            schema=ds.schema_,
            config=config,
            summary=ExtractionSummary(total=len(records), succeeded=succeeded, failed=failed),
        )

        # Save result and update status
        ds.extraction_result = result
        ds.updated_at = datetime.now()
        ctx.storage.save(ctx.user_id, ds)
        if ds.status == "schema_ready":
            ctx.dataset = ctx.manager.update_status(ds.id, ctx.user_id, "previewed")

        # Format results
        lines = [f"Sample extraction: {succeeded}/{len(records)} succeeded"]
        if failed:
            lines.append(f"  {failed} failed")

        for r in records:
            lines.append(f"\n--- {r.document_filename} [{r.status}] ---")
            if r.status == "success":
                for key, val in r.data.items():
                    val_str = str(val)
                    if len(val_str) > 100:
                        val_str = val_str[:100] + "..."
                    lines.append(f"  {key}: {val_str}")
            else:
                for err in r.errors:
                    lines.append(f"  ERROR: {err}")

        return _text_result("\n".join(lines))

    @tool(
        "extract_all",
        "Run extraction on all documents. Writes CSV (always), parquet (for large datasets), and a text dump for search. Skips non-record files (CSV summaries, markdown docs).",
        {},
    )
    async def extract_all(args: dict[str, Any]) -> dict[str, Any]:
        ctx.reload_dataset()
        ds = ctx.dataset
        if not ds.schema_ or not ds.schema_.fields:
            return _error_result("No schema defined.")
        if not ds.documents:
            return _error_result("No documents ingested.")

        api_key = os.environ.get("ANTHROPIC_API_KEY", "")
        if not api_key:
            return _error_result("ANTHROPIC_API_KEY not set.")

        config = ExtractionConfig(
            llm_provider="anthropic/claude-sonnet-4-20250514",
            api_key=api_key,
            temperature=0.0,
            max_retries=2,
        )

        # Filter to single-record documents only
        _SKIP_TYPES = {"csv", "md", "markdown"}
        extractable = [d for d in ds.documents if d.file_type.lower() not in _SKIP_TYPES]
        skipped = len(ds.documents) - len(extractable)

        from parsebox.extract import extract_batch

        result = extract_batch(extractable, ds.schema_, config, max_workers=3)

        ds_dir = ctx.data_dir
        successful_records = [r for r in result.records if r.status == "success"]
        record_count = len(successful_records)

        # Always write CSV
        csv_path = ds_dir / "data.csv"
        write_records(result.records, ds.schema_, "csv", csv_path)
        logger.info("Wrote CSV with %d records to %s", record_count, csv_path)

        # Write parquet if record count meets threshold
        wrote_parquet = False
        if record_count >= ctx.large_threshold:
            parquet_path = ds_dir / "data.parquet"
            write_records(result.records, ds.schema_, "parquet", parquet_path)
            wrote_parquet = True
            logger.info("Wrote parquet with %d records to %s", record_count, parquet_path)

        # Always write text dump for grep-friendly search
        text_dump = _build_text_dump(result.records, ds.schema_)
        text_path = ds_dir / "data.txt"
        text_path.write_text(text_dump, encoding="utf-8")
        logger.info("Wrote text dump to %s", text_path)

        # Update dataset
        ds.extraction_result = result
        ds.extraction_config = config
        ds.parquet_path = str(ds_dir / "data.parquet") if wrote_parquet else None
        ds.updated_at = datetime.now()
        ctx.storage.save(ctx.user_id, ds)

        try:
            ctx.dataset = ctx.manager.update_status(ds.id, ctx.user_id, "extracted")
        except ValueError:
            ctx.reload_dataset()

        s = result.summary
        lines = [f"Extraction complete: {s.succeeded}/{s.total} succeeded, {s.failed} failed"]
        if skipped:
            lines.append(f"Skipped {skipped} non-record files (CSV/MD summaries)")

        lines.append(f"\nData files written:")
        lines.append(f"  CSV: {csv_path}")
        if wrote_parquet:
            lines.append(f"  Parquet: {ds_dir / 'data.parquet'} (large dataset mode, {record_count} records >= {ctx.large_threshold} threshold)")
        lines.append(f"  Text dump: {text_path}")
        lines.append(f"\nUse execute_code to analyze the data with Python (pandas, duckdb, etc).")

        return _text_result("\n".join(lines))

    # -- Code execution tool --------------------------------------------------

    @tool(
        "execute_code",
        "Write and run a Python script to analyze extracted data. The script has access to data files (CSV, parquet if available). Use pandas, duckdb, or any installed library. Print results to stdout.",
        {"code": str},
    )
    async def execute_code(args: dict[str, Any]) -> dict[str, Any]:
        code = args["code"]

        # Build preamble with data file paths
        preamble_lines = [
            "# -- parsebox data paths (auto-injected) --",
            "import os",
            f"DATA_DIR = {str(ctx.data_dir)!r}",
            f"CSV_PATH = {str(ctx.csv_path)!r}",
            f"PARQUET_PATH = {str(ctx.parquet_path)!r}",
            f"TEXT_DUMP_PATH = {str(ctx.text_dump_path)!r}",
            f"HAS_CSV = {ctx.has_csv!r}",
            f"HAS_PARQUET = {ctx.has_parquet!r}",
            f"HAS_TEXT_DUMP = {ctx.has_text_dump!r}",
        ]
        if ctx.source_folder:
            preamble_lines.append(f"SOURCE_FOLDER = {ctx.source_folder!r}")
        preamble_lines.append("# -- end preamble --\n")
        preamble = "\n".join(preamble_lines)

        full_script = preamble + code

        # Write script to work_dir
        work_dir = Path(ctx.work_dir)
        work_dir.mkdir(parents=True, exist_ok=True)

        script_path = work_dir / "script.py"
        script_path.write_text(full_script, encoding="utf-8")

        logger.info("Executing script at %s", script_path)

        try:
            result = subprocess.run(
                ["python", str(script_path)],
                capture_output=True,
                text=True,
                timeout=60,
                cwd=str(work_dir),
            )

            output_parts = []
            if result.stdout.strip():
                output_parts.append(result.stdout.strip())
            if result.stderr.strip():
                # Only show stderr if there's an error (non-zero exit)
                if result.returncode != 0:
                    stderr_msg = result.stderr.strip()
                    if len(stderr_msg) > 500:
                        stderr_msg = stderr_msg[-500:]
                    output_parts.append(f"STDERR:\n{stderr_msg}")

            if not output_parts:
                output_parts.append("(script produced no output)")

            output = "\n".join(output_parts)

            if result.returncode != 0:
                return _error_result(f"Script exited with code {result.returncode}\n\n{output}")

            return _text_result(output)

        except subprocess.TimeoutExpired:
            return _error_result("Script timed out after 60 seconds.")
        except Exception as e:
            return _error_result(f"Failed to execute script: {e}")

    # -- Search tools ---------------------------------------------------------

    @tool(
        "search_files",
        "Search across source files AND extracted data text dump for a pattern (case-insensitive regex). Searches both original documents and the extracted records.",
        {"pattern": str, "max_results": int},
    )
    async def search_files(args: dict[str, Any]) -> dict[str, Any]:
        pattern = args["pattern"]
        max_results = args.get("max_results", 20)

        try:
            regex = re.compile(pattern, re.IGNORECASE)
        except re.error as e:
            return _error_result(f"Invalid regex pattern: {e}")

        matches = []

        # Search source files
        if ctx.source_folder:
            folder_path = Path(ctx.source_folder)
            for f in sorted(folder_path.rglob("*")):
                if not f.is_file():
                    continue
                try:
                    content = f.read_text(errors="replace")
                    for line_num, line in enumerate(content.splitlines(), 1):
                        if regex.search(line):
                            matches.append(f"[source] {f.name}:{line_num}: {line.strip()[:120]}")
                            if len(matches) >= max_results:
                                break
                except Exception:
                    continue
                if len(matches) >= max_results:
                    break

        # Search extracted data text dump
        if ctx.has_text_dump and len(matches) < max_results:
            try:
                content = ctx.text_dump_path.read_text(encoding="utf-8")
                for line_num, line in enumerate(content.splitlines(), 1):
                    if regex.search(line):
                        matches.append(f"[extracted] data.txt:{line_num}: {line.strip()[:120]}")
                        if len(matches) >= max_results:
                            break
            except Exception:
                pass

        if not matches:
            return _text_result(f"No matches found for pattern: {pattern}")

        result = f"Search results for '{pattern}' ({len(matches)} matches):\n" + "\n".join(matches)
        return _text_result(result)

    # -- Demo-mode query tools (safe SQL, no code execution) ----------------

    @tool(
        "query_sql",
        "Execute a SQL query against the extracted data. Use describe_table first to get column names. The table name is shown by describe_table.",
        {"sql": str},
    )
    async def query_sql(args: dict[str, Any]) -> dict[str, Any]:
        try:
            engine = ctx.ensure_query_engine()
        except (RuntimeError, FileNotFoundError) as e:
            return _error_result(str(e))

        try:
            result = engine.execute_sql(args["sql"])
        except ValueError as e:
            hint = f" The table name is: {ctx.table_name}" if "does not exist" in str(e) else ""
            return _error_result(f"{e}{hint}")

        col_names = [c.name for c in result.columns]
        table = _format_rows_as_table(col_names, result.rows[:50])
        lines = [f"Query: {args['sql']}", f"Rows: {result.total_rows}", "", table]
        if result.total_rows > 50:
            lines.append(f"\n... showing first 50 of {result.total_rows} rows")
        return _text_result("\n".join(lines))

    @tool(
        "describe_table",
        "Show the table name, column names, and types for the extracted data. Call this before query_sql.",
        {},
    )
    async def describe_table(args: dict[str, Any]) -> dict[str, Any]:
        try:
            engine = ctx.ensure_query_engine()
        except (RuntimeError, FileNotFoundError) as e:
            return _error_result(str(e))

        columns = engine.get_table_schema(ctx.table_name)
        lines = [
            f"Table name (use this in SQL): {ctx.table_name}",
            f"Columns ({len(columns)}):",
        ]
        for col in columns:
            lines.append(f"  - {col.name}: {col.type}")
        lines.append(f"\nExample: SELECT * FROM {ctx.table_name} LIMIT 5")
        return _text_result("\n".join(lines))

    # -- Assemble tools based on mode -----------------------------------------

    # Common tools available in all modes
    common_tools = [
        list_source_files,
        read_file_sample,
        get_file_info,
        ingest_files,
        list_documents,
        read_document,
        get_schema,
        set_schema,
        extract_sample,
        search_files,
    ]

    if ctx.demo_mode:
        # Demo: safe SQL tools, no code execution, no full batch extraction
        return common_tools + [query_sql, describe_table]
    else:
        # Full: code execution and batch extraction
        return common_tools + [extract_all, execute_code]
