"""Legacy module for derived table A using straightforward SQL approach."""

import os

import duckdb


def load_sql_file(file_path: str) -> str:
    """Load SQL from a file.

    Args:
        file_path: Path to the SQL file

    Returns:
        The SQL query as a string
    """
    with open(file_path) as f:
        return f.read()


def create_derived_table_a() -> None:
    """Create derived table A using straightforward SQL approach.

    This function creates a derived table of top players by market value.
    It loads the SQL query from a file instead of using a hardcoded query.
    """
    # Connect to the database
    conn = duckdb.connect("database/legacy/transferroom.duckdb")

    # Create the derived table
    print("Creating derived table A (top_players_by_value)...")

    # Get the directory of the current file
    current_dir = os.path.dirname(os.path.abspath(__file__))

    # Load SQL from file
    sql_file_path = os.path.join(current_dir, "derived_a.sql")
    query = load_sql_file(sql_file_path)

    # Execute the query and create a table from the results
    conn.execute(f"""
    CREATE OR REPLACE TABLE prod.derived_a AS
    {query}
    """)

    # Print the number of rows
    result = conn.execute("SELECT COUNT(*) FROM prod.derived_a").fetchone()
    if result is not None:
        print(f"Created derived table A with {result[0]} rows")
    else:
        print("Created derived table A but couldn't get row count")

    # Close the connection
    conn.close()


if __name__ == "__main__":
    create_derived_table_a()
