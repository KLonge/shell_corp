import os
import time
from collections.abc import Callable, Hashable, Iterator
from typing import Any

import dlt
import duckdb

# Import the functions directly
from src.legacy.derived_a import create_derived_table_a
from src.legacy.derived_b import create_derived_table_b
from src.legacy.derived_c import create_derived_table_c


def run_function(name: str, func: Callable[[], None]) -> None:
    """Run a function and time it.

    Args:
        name: Name of the function for display
        func: Function to run
    """
    print(f"\n{'=' * 80}")
    print(f"Running {name}...")
    print(f"{'=' * 80}\n")

    try:
        start_time = time.time()
        func()
        end_time = time.time()

        print(f"\nCompleted {name} in {end_time - start_time:.2f} seconds")

    except Exception as e:
        print(f"Error running {name}: {e}")
        raise


@dlt.resource(
    write_disposition="replace",
)
def load_derived_table(table_name: str) -> Iterator[list[dict[Hashable, Any]]]:
    """Load a derived table from the legacy database.

    This function reads a derived table from the legacy DuckDB database
    and returns it as a list of dictionaries for loading into the new database.

    Args:
        table_name: Name of the derived table to load

    Returns:
        List of dictionaries representing the table data
    """
    print(f"Loading {table_name} from legacy database...")

    # Connect to the legacy database
    conn = duckdb.connect("database/legacy/transferroom.duckdb")

    # Query the table and convert to a list of dictionaries
    result = conn.execute(f"SELECT * FROM prod.{table_name}").fetchdf()
    records: list[dict[Hashable, Any]] = result.to_dict(orient="records")

    print(f"Loaded {len(records)} records from {table_name}")

    # Close the connection
    conn.close()

    yield records


def load_derived_tables_to_new_db() -> None:
    """Load all derived tables from legacy to new database using dlt.

    This function reads all derived tables from the legacy DuckDB database
    and loads them into the new DuckDB database under the raw schema.
    """
    print(f"\n{'=' * 80}")
    print("Loading derived tables to new database...")
    print(f"{'=' * 80}\n")

    try:
        start_time = time.time()

        # Ensure the new database directory exists
        os.makedirs("database/new", exist_ok=True)

        # Load each derived table with a fresh pipeline instance
        tables = ["derived_a", "derived_b", "derived_c"]

        for table in tables:
            print(f"Processing {table}...")

            # Create a resource with the table name and explicit write disposition
            resource = load_derived_table(table_name=table).with_name(table)

            # Create a new pipeline for each table to avoid state issues
            table_pipeline = dlt.pipeline(
                pipeline_name=f"derived_table_{table}",
                destination=dlt.destinations.duckdb("database/new/transferroom.duckdb"),
                dataset_name="raw",
            )

            # Run the pipeline for this table
            info = table_pipeline.run(resource)
            print(f"Loaded {table} to new database. Load info: {info}")

        end_time = time.time()
        print(
            f"\nCompleted loading derived tables in {end_time - start_time:.2f} seconds"
        )

    except Exception as e:
        print(f"Error loading derived tables: {e}")
        raise


def main() -> None:
    """Run all derived tables in the correct order and load them to the new database."""
    # Run the derived tables in order
    run_function("derived_table_a", create_derived_table_a)
    run_function("derived_table_b", create_derived_table_b)
    run_function("derived_table_c", create_derived_table_c)

    print("\nAll derived tables have been created successfully!")

    # Load the derived tables to the new database
    load_derived_tables_to_new_db()

    print("\nAll derived tables have been loaded to the new database successfully!")


if __name__ == "__main__":
    main()
