"""Rich-based display utilities for the parsebox CLI."""

import logging
from pathlib import Path

from rich.console import Console
from rich.markdown import Markdown
from rich.panel import Panel
from rich.table import Table
from rich.text import Text
from rich.theme import Theme

from parsebox.models import Dataset

logger = logging.getLogger(__name__)

PARSEBOX_THEME = Theme({
    "info": "cyan",
    "warning": "yellow",
    "error": "bold red",
    "success": "bold green",
    "muted": "dim",
    "accent": "bold magenta",
})

console = Console(theme=PARSEBOX_THEME)


def show_banner() -> None:
    banner = Text()
    banner.append("parsebox", style="bold cyan")
    banner.append(" v0.2.0", style="muted")
    banner.append(" -- turn documents into queryable data", style="muted")
    console.print()
    console.print(banner)
    console.print()


def show_dataset_list(datasets: list[dict]) -> None:
    if not datasets:
        console.print("  No datasets yet. Create one to get started.", style="muted")
        return

    table = Table(show_header=True, header_style="bold", box=None, padding=(0, 2))
    table.add_column("#", style="bold", width=4)
    table.add_column("Name", min_width=20)
    table.add_column("Status", width=14)
    table.add_column("Docs", width=6, justify="right")
    table.add_column("Created", width=12)

    for i, ds in enumerate(datasets, 1):
        status = ds.get("status", "draft")
        status_style = {
            "draft": "yellow",
            "schema_ready": "cyan",
            "previewed": "blue",
            "extracted": "green",
            "failed": "red",
        }.get(status, "white")

        created = ds.get("created_at", "")
        if isinstance(created, str) and len(created) > 10:
            created = created[:10]

        table.add_row(
            str(i),
            ds.get("name", ""),
            Text(status, style=status_style),
            str(ds.get("document_count", 0)),
            str(created),
        )

    console.print(table)


def show_dataset_status(dataset: Dataset, source_folder: str | None = None) -> None:
    lines = []
    lines.append(f"[bold]{dataset.name}[/bold]  [{dataset.status}]")
    if source_folder:
        lines.append(f"Source: {source_folder}")
    lines.append(f"Documents: {len(dataset.documents)}")
    if dataset.schema_:
        field_names = ", ".join(f.name for f in dataset.schema_.fields)
        lines.append(f"Schema: {dataset.schema_.name} ({len(dataset.schema_.fields)} fields)")
    if dataset.parquet_path:
        lines.append("Data: extracted and queryable")
    if dataset.extraction_result and dataset.extraction_result.summary:
        s = dataset.extraction_result.summary
        lines.append(f"Extraction: {s.succeeded}/{s.total} records")

    panel = Panel("\n".join(lines), title="Dataset", border_style="cyan", expand=False)
    console.print(panel)


def show_commands() -> None:
    console.print()
    console.print("Commands:", style="bold")
    console.print("  [bold cyan]n[/]  New dataset         [bold cyan]d[/]  Delete dataset")
    console.print("  [bold cyan]#[/]  Open dataset (by number)  [bold cyan]q[/]  Quit")
    console.print()


def show_chat_help() -> None:
    console.print()
    console.print("Chat commands:", style="bold")
    console.print("  [bold cyan]/status[/]   Show dataset status")
    console.print("  [bold cyan]/schema[/]   Show current schema")
    console.print("  [bold cyan]/back[/]     Return to dataset list")
    console.print("  [bold cyan]/help[/]     Show this help")
    console.print("  [bold cyan]/quit[/]     Exit parsebox")
    console.print()


def show_agent_text(text: str) -> None:
    """Render agent response text as markdown."""
    console.print()
    try:
        md = Markdown(text)
        console.print(md)
    except Exception:
        console.print(text)
    console.print()


# SDK-internal tools that should not be shown to users
_HIDDEN_TOOLS = {"ToolSearch", "ToolResult", "TaskStop", "TaskCreate", "TaskGet"}

# Our parsebox tool names (last segment after __)
_TOOL_LABELS = {
    "list_source_files": "listing files",
    "read_file_sample": "reading file",
    "get_file_info": "checking file",
    "ingest_files": "ingesting files",
    "list_documents": "listing documents",
    "read_document": "reading document",
    "get_schema": "loading schema",
    "set_schema": "saving schema",
    "extract_sample": "extracting sample",
    "extract_all": "extracting all documents",
    "execute_code": "running code",
    "search_files": "searching files",
    "query_sql": "running query",
    "describe_table": "describing table",
}


def show_tool_use(tool_name: str) -> None:
    """Show a brief indicator that a tool is being used."""
    # Strip the MCP prefix if present
    short_name = tool_name.split("__")[-1] if "__" in tool_name else tool_name

    # Hide SDK-internal tools
    if short_name in _HIDDEN_TOOLS:
        return

    label = _TOOL_LABELS.get(short_name, short_name)
    console.print(f"  [muted]> {label}...[/muted]")


def show_error(message: str) -> None:
    console.print(f"[error]{message}[/error]")


def show_success(message: str) -> None:
    console.print(f"[success]{message}[/success]")


def prompt_input(prompt_text: str = "parsebox") -> str:
    """Get user input with a styled prompt."""
    try:
        return console.input(f"[bold cyan]{prompt_text}>[/bold cyan] ")
    except (EOFError, KeyboardInterrupt):
        return "/quit"


def prompt_main_menu() -> str:
    return prompt_input("")


def confirm(message: str) -> bool:
    response = console.input(f"{message} [bold](y/n)[/bold] ")
    return response.strip().lower() in ("y", "yes")
