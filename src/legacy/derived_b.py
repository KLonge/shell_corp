"""Legacy module for derived table B using highly dynamic SQL approach."""

from datetime import datetime

import duckdb


def generate_dynamic_sql(
    min_transfer_fee: float = 10.0,
    transfer_types: list[str] | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    limit: int = 100,
) -> str:
    """Generate a dynamic SQL query based on parameters.

    This function demonstrates the highly dynamic SQL approach where
    the query is constructed at runtime based on various parameters.

    Args:
        min_transfer_fee: Minimum transfer fee in millions
        transfer_types: List of transfer types to include
        start_date: Start date for transfers (YYYY-MM-DD)
        end_date: End date for transfers (YYYY-MM-DD)
        limit: Maximum number of records to return

    Returns:
        Dynamically generated SQL query
    """
    # Default values
    if transfer_types is None:
        transfer_types = ["Permanent", "Loan with Option to Buy"]

    if start_date is None:
        # Default to 3 years ago
        current_year = datetime.now().year
        start_date = f"{current_year - 3}-01-01"

    if end_date is None:
        # Default to current date
        end_date = datetime.now().strftime("%Y-%m-%d")

    # Build the base query
    query = f"""
    WITH player_clubs AS (
        SELECT 
            p.player_id,
            p.name AS player_name,
            p.position,
            p.nationality,
            p.market_value_millions,
            c.club_id,
            c.name AS club_name,
            c.league,
            c.country
        FROM 
            football.app_a p
        LEFT JOIN 
            football.app_b c ON p.current_club = c.name
    )
    
    SELECT 
        t.transfer_id,
        t.player_id,
        p.player_name,
        p.position,
        p.nationality,
        p.market_value_millions,
        t.selling_club_id,
        sell.name AS selling_club,
        sell.league AS selling_league,
        t.buying_club_id,
        buy.name AS buying_club,
        buy.league AS buying_league,
        t.transfer_fee_millions,
        t.transfer_type,
        t.transfer_window,
        t.transfer_date,
        t.contract_length_years,
        t.salary_thousands_weekly
    FROM 
        football.app_c t
    LEFT JOIN 
        player_clubs p ON t.player_id = p.player_id
    LEFT JOIN 
        football.app_b sell ON t.selling_club_id = sell.club_id
    LEFT JOIN 
        football.app_b buy ON t.buying_club_id = buy.club_id
    WHERE 
        t.transfer_fee_millions >= {min_transfer_fee}
    """

    # Add transfer type filter if specified
    if transfer_types:
        transfer_types_str = ", ".join([f"'{t}'" for t in transfer_types])
        query += f"\n    AND t.transfer_type IN ({transfer_types_str})"

    # Add date range filters
    query += f"\n    AND t.transfer_date >= '{start_date}'"
    query += f"\n    AND t.transfer_date <= '{end_date}'"

    # Add status filter - only completed transfers
    query += "\n    AND t.status = 'Completed'"

    # Add ordering and limit
    query += "\n    ORDER BY t.transfer_fee_millions DESC"
    query += f"\n    LIMIT {limit}"

    return query


def create_derived_table_b(
    min_fee: float = 5.0, types: list[str] | None = None, years_back: int = 2
) -> None:
    """Create derived table B using highly dynamic SQL approach.

    This function generates a dynamic SQL query based on parameters
    and executes it to create a derived table of high-value transfers.

    Args:
        min_fee: Minimum transfer fee to include
        types: List of transfer types to include
        years_back: Number of years to look back
    """
    # Calculate start date based on years_back
    current_year = datetime.now().year
    start_date = f"{current_year - years_back}-01-01"

    # Generate the dynamic SQL
    query = generate_dynamic_sql(
        min_transfer_fee=min_fee, transfer_types=types, start_date=start_date, limit=200
    )

    # Connect to the database
    conn = duckdb.connect("database/transferroom.duckdb")

    # Create the derived table
    print(f"Creating derived table B (high_value_transfers, min fee: {min_fee}m)...")

    # Execute the query and create a table from the results
    conn.execute(f"""
    CREATE OR REPLACE TABLE football.derived_b AS
    {query}
    """)

    # Print the number of rows
    result = conn.execute("SELECT COUNT(*) FROM football.derived_b").fetchone()
    if result is not None:
        print(f"Created derived table B with {result[0]} rows")
    else:
        print("Created derived table B but couldn't get row count")

    # Close the connection
    conn.close()


if __name__ == "__main__":
    # Example usage with different parameters
    create_derived_table_b(
        min_fee=10.0, types=["Permanent", "Loan with Option to Buy"], years_back=3
    )
