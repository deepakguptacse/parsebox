import logging
from pathlib import Path
from parsebox.samples import invoices, research_papers, job_listings

logger = logging.getLogger(__name__)

SAMPLE_DATASETS = {
    "invoices": {
        "name": "Sample Invoices",
        "description": "5 sample invoices from different vendors",
        "module": invoices,
    },
    "research_papers": {
        "name": "Sample Research Papers",
        "description": "5 research paper abstracts from different fields",
        "module": research_papers,
    },
    "job_listings": {
        "name": "Sample Job Listings",
        "description": "5 job listings from different companies",
        "module": job_listings,
    },
}


def get_sample_dataset_info() -> list[dict]:
    """Return info about available sample datasets."""
    return [
        {"key": key, "name": info["name"], "description": info["description"]}
        for key, info in SAMPLE_DATASETS.items()
    ]


def load_sample_files(dataset_key: str) -> list[Path]:
    """Load sample files for a given dataset key."""
    if dataset_key not in SAMPLE_DATASETS:
        raise ValueError(f"Unknown sample dataset: {dataset_key}")
    return SAMPLE_DATASETS[dataset_key]["module"].get_sample_files()


def get_sample_schema(dataset_key: str):
    """Get the expected schema for a sample dataset."""
    if dataset_key not in SAMPLE_DATASETS:
        raise ValueError(f"Unknown sample dataset: {dataset_key}")
    return SAMPLE_DATASETS[dataset_key]["module"].get_expected_schema()
