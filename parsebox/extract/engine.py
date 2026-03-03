"""Extraction engine: sample and batch orchestration."""

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

from parsebox.extract.strategies.default import extract_single
from parsebox.models import (
    Document,
    ExtractionConfig,
    ExtractionResult,
    ExtractionSummary,
    Schema,
)

logger = logging.getLogger(__name__)


def extract_sample(
    documents: list[Document],
    schema: Schema,
    config: ExtractionConfig,
    sample_size: int = 5,
) -> ExtractionResult:
    """Extract from a sample of documents (preview mode).

    Args:
        documents: All documents in the dataset
        schema: Schema to extract against
        config: Extraction configuration
        sample_size: Number of documents to sample (default 5)

    Returns:
        ExtractionResult with sample records
    """
    if not documents:
        logger.warning("No documents provided for sample extraction")
        return ExtractionResult(
            records=[],
            schema=schema,
            config=config,
            summary=ExtractionSummary(total=0, succeeded=0, failed=0),
        )

    if not schema.fields:
        logger.error("Schema has no fields defined for extraction")
        raise ValueError("Schema must have at least one field before extraction.")

    if not config.api_key:
        logger.error("Extraction called without an API key")
        raise ValueError("API key is required for extraction.")

    sample = documents[:sample_size]
    logger.info("Running sample extraction on %d documents", len(sample))

    records = []
    for doc in sample:
        record = extract_single(doc, schema, config)
        records.append(record)

    succeeded = sum(1 for r in records if r.status == "success")
    failed = sum(1 for r in records if r.status == "failed")

    result = ExtractionResult(
        records=records,
        schema=schema,
        config=config,
        summary=ExtractionSummary(total=len(records), succeeded=succeeded, failed=failed),
    )
    logger.info("Sample extraction complete: %d succeeded, %d failed", succeeded, failed)
    return result


def extract_batch(
    documents: list[Document],
    schema: Schema,
    config: ExtractionConfig,
    max_workers: int = 3,
    progress_callback=None,
) -> ExtractionResult:
    """Extract from all documents (batch mode).

    Args:
        documents: All documents to extract from
        schema: Schema to extract against
        config: Extraction configuration
        max_workers: Number of concurrent workers
        progress_callback: Optional callback(completed, total) for progress reporting

    Returns:
        ExtractionResult with all records
    """
    if not documents:
        logger.warning("No documents provided for batch extraction")
        return ExtractionResult(
            records=[],
            schema=schema,
            config=config,
            summary=ExtractionSummary(total=0, succeeded=0, failed=0),
        )

    if not schema.fields:
        logger.error("Schema has no fields defined for batch extraction")
        raise ValueError("Schema must have at least one field before extraction.")

    if not config.api_key:
        logger.error("Batch extraction called without an API key")
        raise ValueError("API key is required for extraction.")

    logger.info(
        "Running batch extraction on %d documents with %d workers",
        len(documents),
        max_workers,
    )

    records = []
    completed = 0

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(extract_single, doc, schema, config): doc
            for doc in documents
        }

        for future in as_completed(futures):
            record = future.result()
            records.append(record)
            completed += 1

            if progress_callback:
                progress_callback(completed, len(documents))

    # Sort records by document order
    doc_order = {doc.id: i for i, doc in enumerate(documents)}
    records.sort(key=lambda r: doc_order.get(r.document_id, 0))

    succeeded = sum(1 for r in records if r.status == "success")
    failed = sum(1 for r in records if r.status == "failed")

    result = ExtractionResult(
        records=records,
        schema=schema,
        config=config,
        summary=ExtractionSummary(total=len(records), succeeded=succeeded, failed=failed),
    )
    logger.info("Batch extraction complete: %d succeeded, %d failed", succeeded, failed)
    return result
