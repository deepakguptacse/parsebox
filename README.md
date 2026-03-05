# parsebox

Turn unstructured documents into clean, queryable structured data -- powered by Claude.

Drop anything into a box -- PDFs, markdown files, CSVs, plain text -- and just ask your questions. parsebox figures out the structure, extracts it, and lets you query with SQL. No schemas to write upfront, no ETL pipelines to maintain.

*(We call datasets "boxes" around here. You put things in a box, parsebox opens them.)*

### How it works

```
You: Here are 13 VC deal memos. What's the average asking valuation?
parsebox: *reads files → proposes schema → extracts records → runs SQL*
         The average pre-money valuation across 13 deals is $18.2M.
```

The agent handles the entire pipeline through conversation:

1. **Ingest** -- reads your files, understands their format
2. **Schema** -- proposes a structured schema based on what it finds
3. **Extract** -- pulls structured records from every document using LLM extraction
4. **Query** -- writes to Parquet, exposes via DuckDB, answers with SQL

You can intervene at any step -- tweak the schema, re-extract, ask follow-up queries -- or just let it run.


## Architecture

```
┌─────────────────────────────────────────────────────┐
│  CLI (Rich)  or  Web (FastAPI + WebSocket)          │
└──────────────────────┬──────────────────────────────┘
                       │
              ┌────────▼────────┐
              │  Agent Session  │  Claude Agent SDK
              │  (MCP tools)    │  streams text + tool calls
              └────────┬────────┘
                       │
        ┌──────────────┼──────────────┐
        ▼              ▼              ▼
   ┌─────────┐   ┌──────────┐   ┌─────────┐
   │ Ingest  │   │ Extract  │   │  Query  │
   │ parsers │   │ engine   │   │ DuckDB  │
   └─────────┘   └──────────┘   └─────────┘
    txt,md,csv    instructor +    SQL on
    pdf           litellm         Parquet
```

- **parsebox/agent/** -- MCP tool definitions (closures over dataset context), system prompt, session lifecycle
- **parsebox/ingest/** -- parser registry: plaintext, markdown, CSV, PDF
- **parsebox/extract/** -- LLM-based structured extraction with `instructor` + `litellm`
- **parsebox/schema/** -- validation for nested field definitions
- **parsebox/store/** -- write extracted records to Parquet or CSV
- **parsebox/query/** -- DuckDB engine with SQL execution, pagination, table introspection
- **parsebox/cli/** -- interactive terminal UI with Rich
- **parsebox/web/** -- FastAPI server, WebSocket streaming, chat frontend


## Running locally

**Prerequisites**: Python 3.10+, Node.js 18+ (needed for Claude CLI)

```bash
# Install Claude CLI (required by the agent SDK)
npm install -g @anthropic-ai/claude-code

# Clone and install
git clone https://github.com/deepakguptacse/parsebox.git
cd parsebox
pip install -e ".[dev]"

# Set your API key
export ANTHROPIC_API_KEY=sk-...

# Launch the CLI
parsebox
```

The CLI will show you a dataset picker. Create a new box, point it at a folder of files, and start chatting.

You can also run the web UI locally:

```bash
uvicorn parsebox.web.server:create_app --factory --reload
# Open http://localhost:8000
```


## Running tests

```bash
pip install -e ".[dev]"
pytest
```

Tests cover models, storage, dataset lifecycle, ingest parsers, schema validation, extraction, store writers, and the DuckDB query engine. No API keys needed -- extraction tests use mocks.


## Live demo

A hosted demo is running at **[parsebox-production.up.railway.app](https://parsebox-production.up.railway.app)** with two pre-loaded boxes (Startup Employees, VC Deal Memos) and restricted session limits. Run it locally for the full experience.
