# parsebox -- System Design

## Overview

parsebox is a CLI application built around a Claude Agent SDK session. The agent
drives the workflow -- exploring files, proposing schemas, extracting data, and
answering questions -- through a set of tools bound to a specific dataset.

Internally, parsebox is composed of distinct modules with clean boundaries:

- The CLI layer (Rich + prompt_toolkit) handles display and user input.
- The agent layer (Claude SDK) manages conversation and tool orchestration.
- The pipeline modules (ingest, schema, extract, store) do the actual work.
- Tools are thin wrappers that connect the agent to pipeline modules.

Any module can be swapped, extended, or run independently. The CLI is just one
consumer of the underlying modules.


## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                        CLI Layer (Rich)                             │
│   Dataset picker, chat loop, display rendering, slash commands.     │
│   No business logic -- only orchestration and display.              │
└──────────┬──────────────────────────────────────────────────────────┘
           │
┌──────────▼──────────────────────────────────────────────────────────┐
│                    Agent Layer (Claude SDK)                         │
│   One session per dataset. System prompt adapts to dataset state.   │
│   Tools are bound to a DatasetContext via closures.                 │
└──────────┬──────────────────────────────────────────────────────────┘
           │ tools (12)
           │
┌──────────▼──────────────────────────────────────────────────────────┐
│                        Tool Layer                                   │
│                                                                     │
│   File exploration:  list_source_files, read_file_sample,           │
│                      get_file_info                                  │
│   Ingest:            ingest_files, list_documents, read_document    │
│   Schema:            get_schema, set_schema                         │
│   Extraction:        extract_sample, extract_all                    │
│   Skills:            execute_code, search_files                     │
│                                                                     │
└──────┬──────────┬──────────┬──────────┬─────────────────────────────┘
       │          │          │          │
 ┌─────▼──┐ ┌────▼────┐ ┌───▼────┐ ┌───▼───┐
 │ Ingest │ │ Schema  │ │Extract │ │ Store │
 │        │ │         │ │        │ │       │
 │ parse  │ │ infer   │ │ sample │ │parquet│
 │ files  │ │ propose │ │ batch  │ │ csv   │
 │ into   │ │ edit    │ │        │ │ text  │
 │ content│ │ validate│ │        │ │       │
 └────────┘ └─────────┘ └────────┘ └───────┘
      ▲
      │ extensible
 ┌────┴─────┐
 │ Parsers  │
 │ Registry │
 │          │
 │ .pdf     │
 │ .md      │
 │ .txt     │
 │ .csv     │
 └──────────┘
```


## Agent Architecture

### DatasetContext

Every agent session is scoped to a single dataset. The `DatasetContext` dataclass
holds all state the agent needs:

```python
@dataclass
class DatasetContext:
    user_id: str
    dataset: Dataset
    storage: LocalStorage
    manager: DatasetManager
    source_folder: str | None       # path to raw files
    work_dir: str                   # for execute_code scripts (default: /tmp/parsebox)
    large_threshold: int            # record count to trigger parquet (default: 500)
```

Properties provide access to data file paths:
- `data_dir` -- dataset storage directory
- `csv_path`, `parquet_path`, `text_dump_path` -- extracted data files
- `has_csv`, `has_parquet`, `has_text_dump` -- availability checks

### Tool Factory Pattern

Tools are created via `create_dataset_tools(ctx)`, which returns a list of
`@tool`-decorated async functions. Each function closes over the `DatasetContext`,
so the agent can only operate on the bound dataset -- it's impossible to target
the wrong one.

```python
def create_dataset_tools(ctx: DatasetContext) -> list:
    @tool("list_source_files", "List files in the source folder", {"pattern": str})
    async def list_source_files(args):
        folder = ctx.source_folder  # closed over
        ...
    return [list_source_files, ...]
```

### System Prompt

The system prompt is dynamically built from dataset state via `build_system_prompt(ctx)`.
It adapts guidance based on the current status:

- **draft** -- guide the agent to explore files, ingest, and propose a schema
- **schema_ready** -- guide toward sample extraction
- **previewed** -- show results, ask about adjustments
- **extracted** -- guide toward querying with execute_code

The prompt also includes available data files and usage examples for execute_code.

### AgentSession

`AgentSession` wraps `ClaudeSDKClient`. It:
1. Creates tools from the DatasetContext
2. Builds an MCP server from the tools
3. Starts a Claude SDK client with the dynamic system prompt
4. Provides `send(message)` which yields `(event_type, content)` tuples

```python
session = AgentSession(ctx)
await session.start()
async for event_type, content in session.send("What's in these files?"):
    ...
await session.stop()
```


## Tools (12)

### File Exploration (3)
- **list_source_files** -- list files in the source folder with sizes and extensions
- **read_file_sample** -- read first N lines of a source file
- **get_file_info** -- get metadata about a specific file

### Ingest (3)
- **ingest_files** -- parse ALL source files into documents (idempotent, call once)
- **list_documents** -- list ingested documents
- **read_document** -- read parsed content of a document by index or filename

### Schema (2)
- **get_schema** -- get current schema
- **set_schema** -- set or update schema (JSON), auto-transitions to schema_ready

### Extraction (2)
- **extract_sample** -- extract from 3-5 documents for preview
- **extract_all** -- full batch extraction, writes CSV + conditional parquet + text dump

### Skills (2)
- **execute_code** -- write and run a Python script with auto-injected data paths
- **search_files** -- grep across source files AND extracted data text dump


## Skills: execute_code

The core query mechanism. Instead of hardcoded SQL tools, the agent writes Python
scripts to answer questions. This handles everything from simple counts to complex
visualizations.

**How it works:**
1. Agent writes Python code
2. A preamble is auto-injected with data file paths:
   ```python
   CSV_PATH = "/path/to/data.csv"
   PARQUET_PATH = "/path/to/data.parquet"
   HAS_CSV = True
   HAS_PARQUET = False
   SOURCE_FOLDER = "/path/to/source"
   ```
3. Script is written to `work_dir/script.py` and executed
4. stdout is returned to the agent

The agent can use pandas, duckdb, or any installed library. For small datasets
(CSV only), pandas is typical. For large datasets (parquet available), duckdb is
preferred for performance.

**Why this over rigid SQL tools:**
- Handles any analysis, not just SQL-expressible queries
- Agent can write multi-step analysis (load, transform, aggregate, format)
- No table name guessing -- paths are injected
- Can produce formatted output, summaries, even matplotlib charts


## Pipeline: Storage Format Decision

The storage format is decided at extraction time based on record count:

```
Records < large_threshold (default 500):
  -> CSV only  (data.csv)
  -> Text dump (data.txt)

Records >= large_threshold:
  -> CSV       (data.csv)
  -> Parquet   (data.parquet)
  -> Text dump (data.txt)
```

**CSV** is always written -- universal, readable, works with pandas.

**Parquet** is written for large datasets -- columnar, compressed, works with duckdb
for fast aggregation queries without loading everything into memory.

**Text dump** is always written -- a grep-friendly format:
```
=== Record 1: invoice_001.txt ===
vendor: Acme Corp
amount: 1500.00
date: 2025-01-15

=== Record 2: invoice_002.txt ===
...
```

The text dump is critical for PDFs and binary sources where the original file
isn't text-searchable. `search_files` indexes both source files and the text dump.


## CLI Layer

### app.py -- Entry Point

Handles:
- CLI argument parsing (`--verbose`, `--work-dir`, `--large-threshold`)
- Dataset listing, creation, deletion
- Sample dataset picker (multiple presets: s1, s2, ...)
- Launches chat_loop for the selected dataset

### chat.py -- Chat Loop

The interactive conversation:
- Auto-kicks exploration for fresh datasets with source folders
- Slash commands: /status, /schema, /back, /help, /quit
- Routes user messages to the AgentSession
- Renders responses with text buffering and tool use indicators

### display.py -- Rich Rendering

All terminal output goes through Rich:
- Tool use indicators ("searching files...", "running code...", etc.)
- Hidden SDK-internal tools (ToolSearch, TaskStop, etc.)
- Agent text rendered as Markdown
- Dataset status panels, tables, banners


## Data Contracts

Modules communicate through Pydantic models defined in `parsebox/models.py`:

```
Document
  id, filename, file_type, content, content_blocks, metadata, raw_bytes

Schema
  name, description, fields: list[FieldDefinition], version

FieldDefinition
  name, type, description, required, items, properties

ExtractionConfig
  llm_provider, api_key, instructions, temperature, max_retries

ExtractionResult
  records: list[Record], schema, config, summary

Record
  document_id, document_filename, data: dict, status, errors

Dataset
  id, name, created_at, updated_at, schema, documents, extraction_config,
  extraction_result, parquet_path, source_folder, status, metadata
```

**Dataset status lifecycle:**

```
  create          set_schema      preview          extract
    |               |                |                |
    v               v                v                v
 [draft] --> [schema_ready] --> [previewed] --> [extracted]
                    ^                |               |
                    |                |               |
                    +----------------+               |
                    (refine schema)                   |
                    ^                                 |
                    +---------------------------------+
                    (re-extract with new schema)
```

Any state can transition to [failed]. Failed can transition to [draft].
schema_ready can skip directly to [extracted] (bypass preview).


## Persistence

Datasets are stored as JSON files on the local filesystem:

```
~/.parsebox/users/<user-uuid>/datasets/
  <dataset-uuid>/
    dataset.json        # metadata, schema, config, results
    data.csv            # extracted data (always)
    data.parquet        # extracted data (large datasets only)
    data.txt            # text dump for search (always)
```

User identity is a UUID stored in `~/.parsebox/.user_id`, generated on first run.


## Sample Datasets

Two bundled datasets in `sample_data/`:

**startup_employees** (10 files) -- Employee records from a tech startup.
Text files with structured employee data: name, department, compensation, skills,
performance reviews. Plus a CSV roster and markdown org chart.

**vc_deal_memos** (13 files) -- Venture capital investment memos from a fictional
VC firm (Horizon Ventures Fund III). Analyst notes from pitch meetings covering
11 startups across AI, fintech, cleantech, cybersecurity, and more. Includes
invest/pass decisions, financials, competitive analysis. Plus a CSV pipeline
summary and markdown sector notes.


## Directory Structure

```
parsebox/
  SPEC.md                         # what and why
  design.md                       # this file
  pyproject.toml
  requirements.txt
  sample_data/
    startup_employees/            # 10 files
    vc_deal_memos/                # 13 files
  parsebox/
    __init__.py
    models.py                     # data contracts
    identity.py                   # filesystem-based user ID
    dataset.py                    # dataset lifecycle management
    storage.py                    # LocalStorage implementation
    ingest/
      __init__.py
      registry.py                 # parser registry
      parsers/
        plaintext.py
        markdown.py
        csv_parser.py
        pdf.py
    schema/
      __init__.py
      inference.py                # LLM-based schema inference
      validation.py               # schema validation
    extract/
      __init__.py
      prompt.py                   # extraction prompt builder
      strategies/
        default.py                # one LLM call per document via instructor
    store/
      __init__.py
      registry.py                 # writer registry
      writers/
        parquet.py
        csv_writer.py
    query/
      __init__.py
      engine.py                   # DuckDB query engine
      nl_to_sql.py                # NL to SQL (available for execute_code)
    samples/                      # sample schema/data definitions
      __init__.py
      invoices/
      research_papers/
      job_listings/
    agent/
      __init__.py
      context.py                  # DatasetContext dataclass
      prompts.py                  # dynamic system prompt builder
      tools.py                    # 12 tools created via factory
      session.py                  # AgentSession (ClaudeSDKClient wrapper)
    cli/
      __init__.py
      app.py                      # main entry point, dataset picker
      chat.py                     # interactive chat loop
      display.py                  # Rich-based rendering
  tests/
    test_models.py
    test_identity.py
    test_storage.py
    test_dataset.py
    test_ingest.py
    test_schema.py
    test_extract.py
    test_store.py
    test_query.py
    test_samples.py
    test_export_import.py
```


## Extensibility

| Extension Point      | Mechanism        | Example                                      |
|----------------------|------------------|----------------------------------------------|
| File types           | Parser registry  | Add .eml parser, .pptx parser, .docx parser  |
| Output formats       | Writer registry  | Add BigQuery writer, Postgres writer          |
| Extraction strategy  | Strategy pattern | Vision-based, chunked, multi-pass             |
| Dataset storage      | Storage backend  | S3, database, cloud                           |
| Tools                | Tool factory     | Add new tools to create_dataset_tools()       |
| Sample datasets      | _SAMPLE_DATASETS | Add entry in app.py, folder in sample_data/   |
| System prompt        | prompts.py       | Customize agent behavior per workflow stage    |
| Interface            | Module APIs      | Replace CLI with web UI, API server, notebook  |
