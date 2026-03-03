"""DuckDB query engine for executing SQL over Parquet files."""

import logging
from pathlib import Path

import duckdb

from parsebox.models import ColumnInfo, QueryResult

logger = logging.getLogger(__name__)


class DuckDBQueryEngine:
    def __init__(self):
        self.conn = duckdb.connect(":memory:")
        self._registered_tables: dict[str, str] = {}  # table_name -> parquet_path
        logger.info("Initialized DuckDB in-memory query engine")

    def register_table(self, table_name: str, parquet_path: str | Path) -> None:
        """Register a Parquet file as a named table."""
        parquet_path_obj = Path(parquet_path).resolve()
        if not parquet_path_obj.exists():
            logger.error("Parquet file not found: %s", parquet_path_obj)
            raise FileNotFoundError(
                f"Data file not found at {parquet_path_obj}. "
                "The extracted data may have been deleted."
            )
        parquet_path_str = str(parquet_path_obj)
        try:
            self.conn.execute(
                f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM read_parquet('{parquet_path_str}')"
            )
        except Exception as e:
            logger.error("Failed to load parquet '%s': %s", parquet_path_str, e)
            raise ValueError(
                f"Failed to load data file: {e}. The file may be corrupt."
            ) from e
        self._registered_tables[table_name] = parquet_path_str
        logger.info("Registered table '%s' from %s", table_name, parquet_path_str)

    def execute_sql(self, sql: str, limit: int = 1000, offset: int = 0) -> QueryResult:
        """Execute a SQL query and return results.

        Args:
            sql: The SQL query to execute
            limit: Max rows to return (for pagination)
            offset: Row offset (for pagination)

        Returns:
            QueryResult with columns, rows, and metadata
        """
        logger.info("Executing SQL: %s", sql[:200])

        try:
            # Wrap query with pagination
            paginated_sql = f"SELECT * FROM ({sql}) AS _q LIMIT {limit} OFFSET {offset}"
            result = self.conn.execute(paginated_sql)

            # Get column info
            columns = [
                ColumnInfo(name=desc[0], type=str(desc[1]))
                for desc in result.description
            ]

            rows = result.fetchall()
            # Convert to lists (DuckDB returns tuples)
            rows = [list(row) for row in rows]

            # Get total count
            count_result = self.conn.execute(f"SELECT COUNT(*) FROM ({sql}) AS _q")
            total_rows = count_result.fetchone()[0]

            logger.info("Query returned %d rows (total: %d)", len(rows), total_rows)

            return QueryResult(
                columns=columns,
                rows=rows,
                total_rows=total_rows,
                sql=sql,
            )
        except Exception as e:
            logger.error("SQL execution failed: %s", str(e))
            raise ValueError(f"SQL error: {str(e)}")

    def get_table_schema(self, table_name: str) -> list[ColumnInfo]:
        """Get column names and types for a table."""
        result = self.conn.execute(f"DESCRIBE {table_name}")
        columns = []
        for row in result.fetchall():
            columns.append(ColumnInfo(name=row[0], type=row[1]))
        logger.info("Table '%s' has %d columns", table_name, len(columns))
        return columns

    def list_tables(self) -> list[str]:
        """List all registered tables."""
        return list(self._registered_tables.keys())

    def close(self):
        """Close the DuckDB connection."""
        self.conn.close()
        logger.info("DuckDB connection closed")
