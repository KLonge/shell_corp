from pathlib import Path

from src.clients.duckdb import DuckDBClient
from src.migration_test.models import ComparisonResult
from src.migration_test.utils.comparison import compare_duckdb_tables
from src.migration_test.utils.results import comparison_results_to_csv


def print_failed_model_details(
    failed_model: ComparisonResult, primary_key: list[str]
) -> None:
    """
    Print detailed information about a failed model comparison.

    Args:
        failed_model: The ComparisonResult object containing failure details
        primary_key: List of column names that make up the primary key
    """
    print(f"\n{'=' * 80}")
    print(f"⚠️ FAILED MODEL: {failed_model.model_name}")
    print(f"{'=' * 80}")
    print(f"📊 Total rows: {failed_model.total_rows}")
    print(
        f"📊 Failed rows: {failed_model.failed_row_perc * 100:.2f}% ({int(failed_model.failed_row_perc * failed_model.total_rows)} rows)"
    )

    if failed_model.failed_columns:
        print("\nFailed columns:")
        for col, count in failed_model.failed_columns.items():
            print(
                f"  {col}: {count} rows ({count / failed_model.total_rows * 100:.2f}%)"
            )

    # Print sample failed rows if available
    if failed_model.sample_failed_rows:
        print(f"\n{'=' * 80}")
        print("SAMPLE FAILED ROWS")
        print(f"{'=' * 80}")

        for i, row in enumerate(failed_model.sample_failed_rows):
            print(f"\nFailed Row {i + 1}:")

            # Print primary key
            pk_values = []
            for pk in primary_key:
                pk_value = row.get(pk)
                if pk_value is not None:
                    pk_values.append(f"{pk} = {pk_value}")

            if pk_values:
                print(f"  Primary Key: {', '.join(pk_values)}")

            # Print failed columns
            failed_cols = row.get("failed_columns", [])
            if failed_cols:
                print(f"  Failed Columns: {', '.join(failed_cols)}")

            # Print value differences
            if "value_differences" in row:
                print("  Value Differences:")
                for col, values in row.get("value_differences", {}).items():
                    source = values.get("source")
                    target = values.get("target")

                    # Format the difference based on data type
                    if isinstance(source, (int, float)) and isinstance(
                        target, (int, float)
                    ):
                        diff = values.get("diff", target - source)
                        diff_pct = values.get(
                            "diff_pct",
                            (diff / source) * 100 if source != 0 else float("inf"),
                        )
                        print(
                            f"    {col}: {source} → {target} (diff: {diff:+.4g}, {diff_pct:+.2f}%)"
                        )
                    else:
                        print(f"    {col}: {source} → {target}")

    print("\n" + "-" * 80)


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
        # "derived_a": ["player_id", "contract_end_date"],
        # "derived_b": ["transfer_id"],
        "derived_c": ["insight_id"],
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
            value_tolerance=0.01,
            row_tolerance=0.01,
            exclude_columns=["metrics_json"],
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
            model_name = failed_model.model_name or ""
            model_primary_key = tables_to_test.get(model_name, [])
            print_failed_model_details(failed_model, model_primary_key)
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
