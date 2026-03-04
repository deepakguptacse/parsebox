"""Main CLI entry point for parsebox.

Provides the dataset picker, creation flow, and launches chat sessions.
Run with: parsebox (or python -m parsebox.cli.app)
"""

import argparse
import asyncio
import logging
import os
import sys
from pathlib import Path

from parsebox.agent.context import DEFAULT_LARGE_THRESHOLD, DEFAULT_WORK_DIR, DatasetContext
from parsebox.cli import display
from parsebox.cli.chat import chat_loop
from parsebox.dataset import DatasetManager
from parsebox.identity import get_or_create_user_id
from parsebox.storage import LocalStorage

logger = logging.getLogger(__name__)

# Bundled sample datasets
_SAMPLE_DATA_DIR = Path(os.environ.get("PARSEBOX_SAMPLE_DATA", Path(__file__).resolve().parent.parent.parent / "sample_data"))

_SAMPLE_DATASETS = [
    {
        "key": "s1",
        "name": "Startup Employees",
        "description": "Employee records from a tech startup (10 files)",
        "folder": "startup_employees",
    },
    {
        "key": "s2",
        "name": "VC Deal Memos",
        "description": "Venture capital investment memos (13 files)",
        "folder": "vc_deal_memos",
    },
]

# Module-level config set by CLI args
_work_dir: str = DEFAULT_WORK_DIR
_large_threshold: int = DEFAULT_LARGE_THRESHOLD
_demo_mode: bool = False


def _setup_logging(verbose: bool = False) -> None:
    if verbose:
        logging.basicConfig(
            level=logging.DEBUG,
            format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
            datefmt="%H:%M:%S",
        )
    else:
        # In normal mode, silence all library logs so users never see raw ERROR lines
        logging.basicConfig(level=logging.CRITICAL)

    logging.getLogger("httpx").setLevel(logging.CRITICAL)
    logging.getLogger("anthropic").setLevel(logging.CRITICAL)
    logging.getLogger("litellm").setLevel(logging.CRITICAL)


def _check_api_key() -> bool:
    if os.environ.get("ANTHROPIC_API_KEY"):
        return True
    display.show_error("ANTHROPIC_API_KEY environment variable is not set.")
    display.console.print("  Set it with: [bold]export ANTHROPIC_API_KEY=sk-...[/bold]")
    display.console.print()
    return False


async def _create_dataset_from_folder(
    manager: DatasetManager, user_id: str, name: str, folder: str
) -> DatasetContext:
    """Create a dataset wired to a source folder."""
    folder_path = Path(folder).expanduser().resolve()
    dataset = manager.create_dataset(name, user_id)
    dataset.source_folder = str(folder_path)
    manager.storage.save(user_id, dataset)

    display.show_success(f"Created dataset '{name}'")
    file_count = len([f for f in folder_path.rglob("*") if f.is_file()])
    display.console.print(f"  Found {file_count} files in {folder_path}")

    return DatasetContext(
        user_id=user_id,
        dataset=dataset,
        storage=manager.storage,
        manager=manager,
        source_folder=str(folder_path),
        work_dir=_work_dir,
        large_threshold=_large_threshold,
        demo_mode=_demo_mode,
    )


async def _create_dataset(manager: DatasetManager, user_id: str) -> DatasetContext | None:
    """Interactive dataset creation flow."""
    display.console.print()
    name = display.console.input("[bold]Dataset name:[/bold] ").strip()
    if not name:
        display.show_error("Name cannot be empty.")
        return None

    folder = display.console.input("[bold]Folder path[/bold] (or Enter to skip): ").strip()
    source_folder = None
    if folder:
        folder_path = Path(folder).expanduser().resolve()
        if not folder_path.exists():
            display.show_error(f"Folder not found: {folder_path}")
            return None
        if not folder_path.is_dir():
            display.show_error(f"Not a directory: {folder_path}")
            return None
        source_folder = str(folder_path)

    dataset = manager.create_dataset(name, user_id)
    if source_folder:
        dataset.source_folder = source_folder
        manager.storage.save(user_id, dataset)

    display.show_success(f"Created dataset '{name}'")
    if source_folder:
        file_count = len([f for f in Path(source_folder).rglob("*") if f.is_file()])
        display.console.print(f"  Found {file_count} files in {source_folder}")

    return DatasetContext(
        user_id=user_id,
        dataset=dataset,
        storage=manager.storage,
        manager=manager,
        source_folder=source_folder,
        work_dir=_work_dir,
        large_threshold=_large_threshold,
        demo_mode=_demo_mode,
    )


async def _open_dataset(manager: DatasetManager, user_id: str, dataset_info: dict) -> DatasetContext:
    """Load a dataset and create its context."""
    dataset = manager.get_dataset(dataset_info["id"], user_id)

    return DatasetContext(
        user_id=user_id,
        dataset=dataset,
        storage=manager.storage,
        manager=manager,
        source_folder=dataset.source_folder,
        work_dir=_work_dir,
        large_threshold=_large_threshold,
        demo_mode=_demo_mode,
    )


async def _delete_dataset(manager: DatasetManager, user_id: str, datasets: list[dict]) -> None:
    """Interactive dataset deletion."""
    if not datasets:
        display.console.print("[muted]No datasets to delete.[/muted]")
        return

    display.show_dataset_list(datasets)
    choice = display.console.input("[bold]Delete dataset #:[/bold] ").strip()
    try:
        idx = int(choice) - 1
        if 0 <= idx < len(datasets):
            ds = datasets[idx]
            if display.confirm(f"Delete '{ds['name']}'?"):
                manager.delete_dataset(ds["id"], user_id)
                display.show_success(f"Deleted '{ds['name']}'")
            else:
                display.console.print("[muted]Cancelled.[/muted]")
        else:
            display.show_error("Invalid number.")
    except ValueError:
        display.show_error("Enter a number.")


def _get_available_samples() -> list[dict]:
    """Return sample datasets whose folders exist on disk."""
    return [
        s for s in _SAMPLE_DATASETS
        if (_SAMPLE_DATA_DIR / s["folder"]).exists()
    ]


def _find_sample(key: str, available: list[dict]) -> dict | None:
    """Find a sample dataset by key (e.g. 's1', 's2', or just 's' for single)."""
    for s in available:
        if s["key"] == key:
            return s
    # If user typed just "s" and there's only one, use it
    if key == "s" and len(available) == 1:
        return available[0]
    return None


async def main_loop() -> None:
    """Main application loop: show datasets, handle commands, launch chat."""
    storage = LocalStorage()
    manager = DatasetManager(storage)
    user_id = get_or_create_user_id()

    display.show_banner()

    if not _check_api_key():
        return

    while True:
        datasets = manager.list_datasets(user_id)
        display.console.print("[bold]Your datasets:[/bold]")
        display.show_dataset_list(datasets)

        # Show sample datasets when no datasets exist
        available_samples = _get_available_samples()
        if not datasets and available_samples:
            display.console.print()
            display.console.print("[bold]Sample datasets:[/bold]")
            for sample in available_samples:
                display.console.print(f"  [bold cyan]{sample['key']}[/]  {sample['description']}")

        display.show_commands()

        choice = display.prompt_input("").strip().lower()

        if choice in ("q", "quit", "exit"):
            display.console.print("[muted]Goodbye.[/muted]")
            break

        # Sample dataset shortcut
        if not datasets and choice.startswith("s"):
            sample = _find_sample(choice, available_samples)
            if sample:
                folder = str(_SAMPLE_DATA_DIR / sample["folder"])
                ctx = await _create_dataset_from_folder(
                    manager, user_id, sample["name"], folder
                )
                result = await chat_loop(ctx)
                if result == "/quit":
                    break
                continue

        if choice == "n":
            ctx = await _create_dataset(manager, user_id)
            if ctx:
                result = await chat_loop(ctx)
                if result == "/quit":
                    break
            continue

        if choice == "d":
            datasets = manager.list_datasets(user_id)
            await _delete_dataset(manager, user_id, datasets)
            continue

        # Try as dataset number
        try:
            idx = int(choice) - 1
            if 0 <= idx < len(datasets):
                ctx = await _open_dataset(manager, user_id, datasets[idx])
                result = await chat_loop(ctx)
                if result == "/quit":
                    break
                continue
            else:
                display.show_error(f"No dataset #{choice}. Enter a number from 1-{len(datasets)}.")
                continue
        except ValueError:
            pass

        if choice == "":
            continue

        display.show_error(f"Unknown command: '{choice}'. Enter n, d, #, or q.")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="parsebox",
        description="Turn documents into queryable structured data",
    )
    parser.add_argument("-v", "--verbose", action="store_true", help="Enable debug logging")
    parser.add_argument(
        "--work-dir",
        default=DEFAULT_WORK_DIR,
        help=f"Working directory for script execution (default: {DEFAULT_WORK_DIR})",
    )
    parser.add_argument(
        "--large-threshold",
        type=int,
        default=DEFAULT_LARGE_THRESHOLD,
        help=f"Record count threshold for writing parquet files (default: {DEFAULT_LARGE_THRESHOLD})",
    )
    parser.add_argument(
        "--demo",
        action="store_true",
        help="Run in demo mode (restricted tools, safe for public hosting)",
    )
    return parser.parse_args()


def main() -> None:
    """Entry point for the parsebox CLI."""
    global _work_dir, _large_threshold, _demo_mode

    args = _parse_args()
    _setup_logging(args.verbose)
    _work_dir = args.work_dir
    _large_threshold = args.large_threshold
    _demo_mode = args.demo

    try:
        asyncio.run(main_loop())
    except KeyboardInterrupt:
        display.console.print("\n[muted]Goodbye.[/muted]")


if __name__ == "__main__":
    main()
