# parsebox

## Why this exists

Every organization sits on a pile of unstructured documents -- PDFs, reports, contracts,
invoices, research papers, support tickets, you name it. The data trapped inside these
documents is valuable, but it's locked away in formats that machines can't easily work with.

Today, if you want to turn 500 invoices into a clean spreadsheet, you have two options:

1. **Do it by hand.** Open each document, copy-paste into a spreadsheet, repeat 499 more times.
2. **Stitch together 4-5 libraries yourself.** Write a script that uses one tool to parse PDFs,
   another to call an LLM for extraction, another to validate the output, then manually convert
   everything to a usable format. Hope it works. Debug when it doesn't.

Neither option is acceptable. The first doesn't scale. The second requires too much
technical plumbing for what should be a simple task.

What people actually want is dead simple:

> "Here are my documents. Figure out what's in them. Let me refine. Extract everything.
> Now answer my questions."

That's parsebox.


## What parsebox does

parsebox turns a pile of unstructured documents into clean, queryable structured data
through an interactive agent conversation.

You point it at a folder of documents. An AI agent explores the files, proposes a schema,
extracts structured records, and then answers analytical questions by writing Python code
against the extracted data -- all through a conversational CLI.

Extraction is not a one-shot operation. It's a conversation. You need to see what the
system produces, correct mistakes, provide guidance, and then let it run. parsebox is
built around this loop.


## How it works

**1. Start with documents**

Point parsebox at a folder -- PDFs, text files, Markdown, CSVs. Or pick one of the
built-in sample datasets to see how it works before touching your own data.

**2. The agent explores and proposes**

The agent reads your files, understands their structure, and proposes a schema:
"It looks like these documents contain company names, dates, amounts, and categories.
Does that look right?"

You refine the schema through conversation -- add fields, remove fields, change types,
describe what you want extracted. The agent updates the schema based on your feedback.

**3. Preview before you commit**

The agent extracts data from a small sample (3-5 documents) and shows you the results.
This is where you catch problems early. Describe what's wrong in plain language:

- "The date format should be YYYY-MM-DD, not MM/DD/YYYY"
- "Ignore the disclaimer section at the bottom"
- "Split the address into street, city, state, zip"

The agent adjusts the schema and re-runs until you're satisfied.

**4. Run the full batch**

Once the sample looks right, the agent processes all your documents. The results are
written to CSV (always), Parquet (for large datasets), and a searchable text dump.

**5. Query your data**

Ask questions in plain English: "What's the average invoice amount by vendor?"
The agent writes and executes Python code to answer -- using pandas, duckdb, or
whatever fits the question. You get both the answer and the code that produced it.


## Who this is for

- **Data analysts** who receive messy document dumps and need to make sense of them
- **Researchers** processing large sets of papers, reports, or filings
- **Small teams** that can't justify building a document processing pipeline
- **Anyone** who has ever copy-pasted from a PDF into a spreadsheet and thought
  "there has to be a better way"


## Design principles

**1. Agent-first, not form-first.**
The interaction is a conversation, not a sequence of form fields. The agent drives
the workflow: explore, propose, extract, query. The user steers through natural language.

**2. Preview is not optional.**
Never process a full batch without showing the user what the output looks like first.
Extraction is imperfect -- the user needs to see, correct, and guide before committing.

**3. Feedback is a first-class feature.**
Users should be able to look at sample results, describe problems in plain language,
and see the system improve. This is not a fire-and-forget tool.

**4. Datasets are the unit of work.**
Everything revolves around named datasets. Create a dataset, point it at a folder,
extract, query, come back later. This gives parsebox the feel of an application,
not a one-shot script.

**5. Zero infrastructure.**
No database server. No account system. No backend to maintain. Everything runs locally.
Bring your own Anthropic API key, and you're done.

**6. The agent writes code, not just SQL.**
Instead of a rigid query interface, the agent writes Python scripts to answer questions.
This handles everything from simple aggregations to complex analysis, without constraining
the user to SQL syntax.

**7. Sample data ships with the tool.**
A new user should be able to run parsebox and start exploring in under 30 seconds
without uploading anything. Built-in sample datasets demonstrate the full workflow.


## What it includes

- AI agent powered by Claude Agent SDK (one session per dataset)
- Automatic schema proposal from document content
- Schema refinement through natural language conversation
- Sample extraction preview before committing to full batch
- Batch processing with progress tracking and error handling
- Named datasets that persist locally -- come back and query them later
- Python code execution for analysis (pandas, duckdb, any installed library)
- Grep-based search across source files and extracted data
- Multi-format output: CSV (always), Parquet (large datasets), text dump (always)
- Configurable large-dataset threshold (--large-threshold CLI arg)
- Two built-in sample datasets: startup employee records, VC deal memos
- CLI interface with Rich rendering (full mode)
- Web interface with chat UI and marketing page (demo mode)
- Demo mode (`--demo`) for safe public hosting with restricted tools
- Bring your own Anthropic API key (CLI), or server-side key (demo)


## Demo mode (`--demo`)

parsebox can be run with `--demo` to produce a hosted, shareable version that's safe
to expose publicly. Demo mode is a **configuration** on top of the full system -- it
restricts what tools are available and what the agent is allowed to do, but the
underlying code is identical.

Run locally (full power):
```
parsebox
```

Run as a hosted demo (restricted):
```
parsebox --demo
```

### What demo mode changes

**Tools restricted:**

| Tool | Normal | Demo | Why |
|------|--------|------|-----|
| execute_code | Yes | **No** | Arbitrary code can read server env vars, leak API keys |
| extract_all | Yes | **No** | Full batch extraction costs $1-5+ per run in API calls |
| query_sql | No | **Yes** | Safe replacement -- DuckDB SQL, server-side, no code execution |
| describe_table | No | **Yes** | Shows table schema so agent can write correct SQL |
| Everything else | Yes | Yes | File exploration, ingest, schema, extract_sample, search |

In normal mode, the agent writes Python code to answer analytical questions (via
execute_code). In demo mode, the agent writes SQL instead (via query_sql + DuckDB).
Both are impressive for a demo; SQL is safe to run on a shared server.

**Datasets restricted:**
- Sample datasets only (no custom folder paths)
- Users choose from the preset datasets (startup employees, VC deal memos)
- No file upload, no arbitrary folder access

**Session limits:**
- Message cap per session (e.g., 30 messages)
- Concurrent session cap (e.g., 5)
- Sessions are ephemeral -- not persisted to disk

**API key handling:**
- The Anthropic API key is a server-side environment variable
- It is never sent to the client or exposed in any tool output
- execute_code is disabled, so there is no way to run `os.environ` or read `/proc`
- The agent's tool set has no mechanism to access, print, or leak the key

### Why this approach

The alternative would be to maintain a separate "demo version" with different tools
and different code paths. That's fragile -- changes to the core would drift from the
demo, and bugs would appear in one but not the other.

Instead, demo mode is a flag that the tool factory reads. The same `create_dataset_tools(ctx)`
function checks `ctx.demo_mode` and includes or excludes tools accordingly. The same
agent session, same prompts (adjusted for available tools), same extraction pipeline.

One codebase. Two configurations. The `--demo` flag is the only difference between
"run this on my laptop" and "host this for the world."


## What it does not include

- Multi-user collaboration or accounts
- Confidence scores per extracted field
- Local model support (Ollama)
- Custom document parsing pipeline selection
- Vector search or RAG
- Scheduling or automatic reprocessing
- Cloud storage backends
