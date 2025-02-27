from pathlib import Path

import duckdb
import pytest

from src.clients.duckdb import DuckDBClient
from src.migration_test.utils.comparison import compare_duckdb_tables
from src.migration_test.utils.failed_models import print_failed_model_details


@pytest.fixture
def test_db_path(tmp_path: Path) -> Path:
    """
    Fixture that creates a temporary test database.

    Args:
        tmp_path: Pytest fixture providing a temporary directory

    Returns:
        Path to the temporary test database
    """
    return tmp_path / "test_migration.duckdb"


@pytest.fixture
def duckdb_client(test_db_path: Path) -> DuckDBClient:
    """
    Fixture that provides a DuckDB client connected to the test database.

    Args:
        test_db_path: Path to the test database

    Returns:
        Configured DuckDB client
    """
    return DuckDBClient(database_path=test_db_path)


@pytest.fixture
def setup_test_data(test_db_path: Path) -> None:
    """
    Fixture that sets up test data in the test database.

    This creates two schemas (prod and raw) with identical tables for the passing case
    and tables with differences for the failing case.

    Args:
        test_db_path: Path to the test database
    """
    # Connect to the test database
    conn = duckdb.connect(str(test_db_path))

    # Create schemas
    conn.execute("CREATE SCHEMA IF NOT EXISTS prod")
    conn.execute("CREATE SCHEMA IF NOT EXISTS raw")

    # Create identical tables for the passing case
    conn.execute("""
    CREATE OR REPLACE TABLE prod.identical_table (
        id INTEGER PRIMARY KEY,
        name VARCHAR,
        value DOUBLE
    )
    """)

    conn.execute("""
    CREATE OR REPLACE TABLE raw.identical_table (
        id INTEGER PRIMARY KEY,
        name VARCHAR,
        value DOUBLE
    )
    """)

    # Insert identical data
    for i in range(10):
        conn.execute(f"""
        INSERT INTO prod.identical_table VALUES 
        ({i}, 'name_{i}', {i * 1.5})
        """)

        conn.execute(f"""
        INSERT INTO raw.identical_table VALUES 
        ({i}, 'name_{i}', {i * 1.5})
        """)

    # Create tables with different values for the failing case
    conn.execute("""
    CREATE OR REPLACE TABLE prod.different_values (
        id INTEGER PRIMARY KEY,
        name VARCHAR,
        value DOUBLE
    )
    """)

    conn.execute("""
    CREATE OR REPLACE TABLE raw.different_values (
        id INTEGER PRIMARY KEY,
        name VARCHAR,
        value DOUBLE
    )
    """)

    # Insert data with different values
    for i in range(10):
        conn.execute(f"""
        INSERT INTO prod.different_values VALUES 
        ({i}, 'name_{i}', {i * 1.5})
        """)

        # Different value in raw table
        conn.execute(f"""
        INSERT INTO raw.different_values VALUES 
        ({i}, 'name_{i}', {i * 1.6})
        """)

    # Create tables with different rows for the failing case
    conn.execute("""
    CREATE OR REPLACE TABLE prod.different_rows (
        id INTEGER PRIMARY KEY,
        name VARCHAR,
        value DOUBLE
    )
    """)

    conn.execute("""
    CREATE OR REPLACE TABLE raw.different_rows (
        id INTEGER PRIMARY KEY,
        name VARCHAR,
        value DOUBLE
    )
    """)

    # Insert different number of rows
    for i in range(12):  # 12 rows in prod
        conn.execute(f"""
        INSERT INTO prod.different_rows VALUES 
        ({i}, 'name_{i}', {i * 1.5})
        """)

    for i in range(10):  # 10 rows in raw
        conn.execute(f"""
        INSERT INTO raw.different_rows VALUES 
        ({i}, 'name_{i}', {i * 1.5})
        """)

    # Close the connection
    conn.close()


def test_given_identical_tables_when_comparing_then_passes(
    duckdb_client: DuckDBClient, setup_test_data: None
) -> None:
    """
    Test that the comparison passes when tables are identical.

    Args:
        duckdb_client: DuckDB client to use for comparison
        setup_test_data: Fixture that sets up test data
    """
    # Compare the tables
    comparison_result = compare_duckdb_tables(
        duckdb_client=duckdb_client,
        table1_name="prod.identical_table",
        table2_name="raw.identical_table",
        primary_key=["id"],
        value_tolerance=0.01,
        row_tolerance=0.01,
        exclude_columns=[],
        trim_strings=True,
    )

    # Assert that the comparison passed
    assert comparison_result.passed, "Comparison should pass for identical tables"
    assert comparison_result.failed_row_perc == 0.0, "No rows should fail"
    assert (
        comparison_result.failed_columns is None
        or len(comparison_result.failed_columns) == 0
    ), "No columns should fail"


def test_given_different_values_when_comparing_then_fails(
    duckdb_client: DuckDBClient, setup_test_data: None
) -> None:
    """
    Test that the comparison fails when tables have different values.

    Args:
        duckdb_client: DuckDB client to use for comparison
        setup_test_data: Fixture that sets up test data
    """
    # Compare the tables
    comparison_result = compare_duckdb_tables(
        duckdb_client=duckdb_client,
        table1_name="prod.different_values",
        table2_name="raw.different_values",
        primary_key=["id"],
        value_tolerance=0.01,  # 1% tolerance
        row_tolerance=0.01,
        exclude_columns=[],
        trim_strings=True,
    )

    # Print detailed information about the failed comparison
    if not comparison_result.passed:
        print_failed_model_details(comparison_result, ["id"])

    # Assert that the comparison failed
    assert not comparison_result.passed, (
        "Comparison should fail for tables with different values"
    )
    assert comparison_result.failed_row_perc > 0.0, "Some rows should fail"
    assert comparison_result.failed_columns is not None, (
        "Failed columns should be tracked"
    )
    assert "value" in comparison_result.failed_columns, "The 'value' column should fail"


def test_given_different_rows_when_comparing_then_fails(
    duckdb_client: DuckDBClient, setup_test_data: None
) -> None:
    """
    Test that the comparison fails when tables have different numbers of rows.

    Args:
        duckdb_client: DuckDB client to use for comparison
        setup_test_data: Fixture that sets up test data
    """
    # Compare the tables
    comparison_result = compare_duckdb_tables(
        duckdb_client=duckdb_client,
        table1_name="prod.different_rows",
        table2_name="raw.different_rows",
        primary_key=["id"],
        value_tolerance=0.01,
        row_tolerance=0.01,  # 1% tolerance
        exclude_columns=[],
        trim_strings=True,
    )

    # Print detailed information about the failed comparison
    if not comparison_result.passed:
        print_failed_model_details(comparison_result, ["id"])

    # Assert that the comparison failed
    assert not comparison_result.passed, (
        "Comparison should fail for tables with different row counts"
    )


def test_given_high_tolerance_when_comparing_different_values_then_passes(
    duckdb_client: DuckDBClient, setup_test_data: None
) -> None:
    """
    Test that the comparison passes when tolerance is high enough.

    Args:
        duckdb_client: DuckDB client to use for comparison
        setup_test_data: Fixture that sets up test data
    """
    # Compare the tables with high tolerance
    comparison_result = compare_duckdb_tables(
        duckdb_client=duckdb_client,
        table1_name="prod.different_values",
        table2_name="raw.different_values",
        primary_key=["id"],
        value_tolerance=0.10,  # 10% tolerance, should be enough for our test data
        row_tolerance=0.01,
        exclude_columns=[],
        trim_strings=True,
    )

    # Assert that the comparison passed due to high tolerance
    assert comparison_result.passed, "Comparison should pass with high enough tolerance"


def test_given_high_row_tolerance_when_comparing_different_rows_then_passes(
    duckdb_client: DuckDBClient, setup_test_data: None
) -> None:
    """
    Test that the comparison passes when row tolerance is high enough.

    Args:
        duckdb_client: DuckDB client to use for comparison
        setup_test_data: Fixture that sets up test data
    """
    # Compare the tables with high row tolerance
    comparison_result = compare_duckdb_tables(
        duckdb_client=duckdb_client,
        table1_name="prod.different_rows",
        table2_name="raw.different_rows",
        primary_key=["id"],
        value_tolerance=0.01,
        row_tolerance=0.20,  # 20% tolerance, should be enough for our test data
        exclude_columns=[],
        trim_strings=True,
    )

    # Assert that the comparison passed due to high row tolerance
    assert comparison_result.passed, (
        "Comparison should pass with high enough row tolerance"
    )


if __name__ == "__main__":
    pytest.main([__file__])
