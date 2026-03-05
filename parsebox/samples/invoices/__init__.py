import logging
from pathlib import Path
from parsebox.models import Schema, FieldDefinition

logger = logging.getLogger(__name__)

SAMPLE_DIR = Path(__file__).parent


def get_sample_files() -> list[Path]:
    """Return paths to all sample invoice files."""
    files = sorted(SAMPLE_DIR.glob("*.txt"))
    logger.info("Found %d sample invoice files", len(files))
    return files


def get_expected_schema() -> Schema:
    """Return the expected schema for invoice extraction."""
    return Schema(
        name="invoice_data",
        description="Structured data extracted from invoice documents",
        fields=[
            FieldDefinition(
                name="vendor_name",
                type="string",
                description="Company that issued the invoice",
                required=True,
            ),
            FieldDefinition(
                name="invoice_number",
                type="string",
                description="Invoice identifier",
                required=True,
            ),
            FieldDefinition(
                name="invoice_date",
                type="date",
                description="Date invoice was issued (YYYY-MM-DD)",
                required=True,
            ),
            FieldDefinition(
                name="due_date",
                type="date",
                description="Payment due date (YYYY-MM-DD)",
            ),
            FieldDefinition(
                name="currency",
                type="string",
                description="Currency code (USD, EUR, GBP)",
            ),
            FieldDefinition(
                name="subtotal",
                type="number",
                description="Amount before tax",
            ),
            FieldDefinition(
                name="tax_amount",
                type="number",
                description="Tax amount",
            ),
            FieldDefinition(
                name="total_amount",
                type="number",
                description="Total amount due",
                required=True,
            ),
            FieldDefinition(
                name="line_items",
                type="array",
                description="Individual line items",
                items=FieldDefinition(
                    name="item",
                    type="object",
                    properties=[
                        FieldDefinition(
                            name="description",
                            type="string",
                            description="Item description",
                        ),
                        FieldDefinition(
                            name="quantity",
                            type="number",
                            description="Quantity",
                        ),
                        FieldDefinition(
                            name="unit_price",
                            type="number",
                            description="Price per unit",
                        ),
                        FieldDefinition(
                            name="amount",
                            type="number",
                            description="Line total",
                        ),
                    ],
                ),
            ),
        ],
    )
