import datetime as dt
from pathlib import Path

from src.clients.duckdb import DuckDBClient
from src.migration_test.models import ComparisonResult
from src.migration_test.utils.comparison import compare_duckdb_tables
from src.migration_test.utils.results import comparison_results_to_csv


def main(folder_path: str) -> None:
    """
    Main function to compare tables between prod and legacy_prod schemas.

    Args:
        folder_path: Path to save results
    """
    count: int = 0
    comparison_results: list[ComparisonResult] = []

    # Define the tables to test with their primary keys
    tables_to_test = {
        "derived_a": ["player_id", "contract_end_date"],
        # "derived_b": ["user_id", "event_date"],
        # "derived_c": ["session_id"],
    }

    total_model_count = len(tables_to_test)

    # Create DuckDB client
    duckdb_client = DuckDBClient(database_path=DATABASE_PATH)

    for table_name, primary_key in tables_to_test.items():
        comparison_result = compare_duckdb_tables(
            duckdb_client=duckdb_client,
            table1_name=f"prod.{table_name}",
            table2_name=f"raw.{table_name}",
            primary_key=primary_key,
            value_tolerance=0.20,
            row_tolerance=0.01,
            exclude_columns=[],
            timestamp_range_exclude=(
                dt.datetime(2024, 12, 1),
                dt.datetime(2024, 12, 31),
            ),
            trim_strings=True,
        )
        comparison_result.model_name = table_name
        comparison_results.append(comparison_result)
        count += 1
        print(f"\n{count}/{total_model_count} TABLES COMPARED")

    comparison_results_to_csv(
        folder_path=folder_path, comparison_results=comparison_results
    )

    failed_models: list[ComparisonResult] = [
        result for result in comparison_results if not result.passed
    ]

    if failed_models:
        print(f"\n⚠️ FAILED MODELS: {len(failed_models)}/{total_model_count}")

        for failed_model in failed_models:
            print(f"⚠️ {failed_model.model_name}")
            print("\n")
            print(f"📊 {failed_model.failed_row_perc * 100:.2f}% OF ROWS FAILED")
            print("\n")
            print("-" * 100)
            print("\n")
    else:
        print("\n✅ ALL MODELS PASSED")


if __name__ == "__main__":
    # Define the folder path for saving results
    FOLDER_PATH: str = "migration_test_results"
    DATABASE_PATH: Path = (
        Path(__file__).parent.parent.parent / "database/new/transferroom.duckdb"
    )

    # Run the main function
    main(folder_path=FOLDER_PATH)
