"""Legacy module for derived table A using straightforward SQL approach."""

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
    """
    # Connect to the database
    conn = duckdb.connect("database/transferroom.duckdb")

    # Create the derived table
    print("Creating derived table A (top_players_by_value)...")

    # Use a hardcoded query instead of loading from file
    query = """
    SELECT
        p.player_id,
        p.name,
        p.position,
        p.age,
        p.nationality,
        p.current_club,
        p.market_value_millions,
        p.contract_end_date,
        'Unknown' AS league,
        'Unknown' AS country
    FROM
        football.app_a p
    WHERE
        p.market_value_millions > 5
    ORDER BY
        p.market_value_millions DESC
    """

    # Execute the query and create a table from the results
    conn.execute(f"""
    CREATE OR REPLACE TABLE football.derived_a AS
    {query}
    """)

    # Print the number of rows
    result = conn.execute("SELECT COUNT(*) FROM football.derived_a").fetchone()
    if result is not None:
        print(f"Created derived table A with {result[0]} rows")
    else:
        print("Created derived table A but couldn't get row count")

    # Close the connection
    conn.close()


if __name__ == "__main__":
    create_derived_table_a()
