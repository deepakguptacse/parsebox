"""Tests for the Query module: DuckDB engine."""

import logging

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from parsebox.models import ColumnInfo, QueryResult
from parsebox.query.engine import DuckDBQueryEngine

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def sample_parquet(tmp_path):
    """Create a sample Parquet file with test data."""
    table = pa.table(
        {
            "name": ["Alice", "Bob", "Charlie", "Diana", "Eve"],
            "age": [30, 25, 35, 28, 32],
            "city": ["New York", "London", "Paris", "Tokyo", "Berlin"],
            "salary": [70000.0, 60000.0, 80000.0, 65000.0, 75000.0],
        }
    )
    path = tmp_path / "people.parquet"
    pq.write_table(table, path)
    logger.info("Created sample Parquet file at %s with %d rows", path, len(table))
    return path


@pytest.fixture
def engine():
    """Create a DuckDBQueryEngine and close it after the test."""
    eng = DuckDBQueryEngine()
    yield eng
    eng.close()


@pytest.fixture
def loaded_engine(engine, sample_parquet):
    """Engine with the sample table already registered."""
    engine.register_table("people", sample_parquet)
    return engine


# ---------------------------------------------------------------------------
# DuckDBQueryEngine tests
# ---------------------------------------------------------------------------


class TestDuckDBQueryEngine:
    def test_register_table(self, engine, sample_parquet):
        """register_table loads a Parquet file without error."""
        engine.register_table("people", sample_parquet)
        assert "people" in engine.list_tables()
        logger.info("register_table test passed")

    def test_list_tables_empty(self, engine):
        """list_tables returns empty list when nothing is registered."""
        assert engine.list_tables() == []

    def test_list_tables_multiple(self, engine, sample_parquet, tmp_path):
        """list_tables returns all registered table names."""
        engine.register_table("t1", sample_parquet)
        # Create a second parquet
        table2 = pa.table({"x": [1, 2, 3]})
        path2 = tmp_path / "nums.parquet"
        pq.write_table(table2, path2)
        engine.register_table("t2", path2)
        assert sorted(engine.list_tables()) == ["t1", "t2"]

    def test_execute_sql_basic(self, loaded_engine):
        """execute_sql returns correct QueryResult for a simple SELECT."""
        result = loaded_engine.execute_sql("SELECT * FROM people")

        assert isinstance(result, QueryResult)
        assert len(result.columns) == 4
        col_names = [c.name for c in result.columns]
        assert "name" in col_names
        assert "age" in col_names
        assert "city" in col_names
        assert "salary" in col_names
        assert len(result.rows) == 5
        assert result.total_rows == 5
        assert result.sql == "SELECT * FROM people"
        logger.info("execute_sql basic test passed with %d rows", len(result.rows))

    def test_execute_sql_with_where(self, loaded_engine):
        """execute_sql handles WHERE clauses correctly."""
        result = loaded_engine.execute_sql("SELECT name, age FROM people WHERE age > 30")
        assert result.total_rows == 2
        assert len(result.rows) == 2
        col_names = [c.name for c in result.columns]
        assert col_names == ["name", "age"]

    def test_execute_sql_aggregation(self, loaded_engine):
        """execute_sql handles aggregation queries."""
        result = loaded_engine.execute_sql("SELECT AVG(salary) AS avg_salary FROM people")
        assert result.total_rows == 1
        assert len(result.rows) == 1
        avg_salary = result.rows[0][0]
        assert abs(avg_salary - 70000.0) < 0.01

    def test_execute_sql_pagination_limit(self, loaded_engine):
        """execute_sql respects the limit parameter."""
        result = loaded_engine.execute_sql("SELECT * FROM people", limit=2)
        assert len(result.rows) == 2
        assert result.total_rows == 5
        logger.info("Pagination limit test: got %d rows, total %d", len(result.rows), result.total_rows)

    def test_execute_sql_pagination_offset(self, loaded_engine):
        """execute_sql respects the offset parameter."""
        result = loaded_engine.execute_sql(
            "SELECT * FROM people ORDER BY name", limit=2, offset=2
        )
        assert len(result.rows) == 2
        assert result.total_rows == 5
        # With ORDER BY name: Alice, Bob, Charlie, Diana, Eve
        # offset=2, limit=2 should give Charlie and Diana
        names = [row[0] for row in result.rows]
        assert names == ["Charlie", "Diana"]

    def test_execute_sql_pagination_offset_past_end(self, loaded_engine):
        """execute_sql with offset past all rows returns empty."""
        result = loaded_engine.execute_sql("SELECT * FROM people", limit=10, offset=100)
        assert len(result.rows) == 0
        assert result.total_rows == 5

    def test_execute_sql_invalid_raises(self, loaded_engine):
        """execute_sql raises ValueError for invalid SQL."""
        with pytest.raises(ValueError, match="SQL error"):
            loaded_engine.execute_sql("SELECT * FROM nonexistent_table")

    def test_execute_sql_syntax_error_raises(self, loaded_engine):
        """execute_sql raises ValueError for SQL syntax errors."""
        with pytest.raises(ValueError, match="SQL error"):
            loaded_engine.execute_sql("SELECTTTT invalid stuff")

    def test_get_table_schema(self, loaded_engine):
        """get_table_schema returns correct ColumnInfo list."""
        columns = loaded_engine.get_table_schema("people")
        assert len(columns) == 4
        assert all(isinstance(c, ColumnInfo) for c in columns)
        col_dict = {c.name: c.type for c in columns}
        assert "name" in col_dict
        assert "age" in col_dict
        assert "city" in col_dict
        assert "salary" in col_dict
        logger.info("get_table_schema returned: %s", col_dict)

    def test_close(self, sample_parquet):
        """close shuts down the connection cleanly."""
        eng = DuckDBQueryEngine()
        eng.register_table("t", sample_parquet)
        eng.close()
        # After close, executing should fail
        with pytest.raises(Exception):
            eng.conn.execute("SELECT 1")


