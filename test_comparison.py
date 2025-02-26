from src.clients.duckdb import DuckDBClient
from src.migration_test.utils.comparison import compare_duckdb_tables

# Create a DuckDB client
client = DuckDBClient("database/new/transferroom.duckdb")

# Run the comparison
result = compare_duckdb_tables(
    duckdb_client=client,
    table1_name="raw.derived_a",
    table2_name="raw.derived_a_modified",
    primary_key=["player_id"],
    value_tolerance=0.05,
    row_tolerance=0.05,
)

# Print the results
print("\n" + "=" * 80)
print("COMPARISON RESULTS")
print("=" * 80)
print(f"Comparison passed: {result.passed}")
print(f"Total rows: {result.total_rows}")
print(
    f"Failed rows: {result.failed_row_perc * 100:.2f}% ({int(result.failed_row_perc * result.total_rows)} rows)"
)

if result.failed_columns:
    print("\nFailed columns:")
    for col, count in result.failed_columns.items():
        print(f"  {col}: {count} rows ({count / result.total_rows * 100:.2f}%)")

# Print sample failed rows
if result.sample_failed_rows:
    print("\n" + "=" * 80)
    print("SAMPLE FAILED ROWS")
    print("=" * 80)

    for i, row in enumerate(result.sample_failed_rows):
        print(f"\nFailed Row {i + 1}:")

        # Print primary key
        pk_value = row.get("player_id")
        print(f"  Primary Key: player_id = {pk_value}")

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
                    diff = target - source
                    diff_pct = (diff / source) * 100 if source != 0 else float("inf")
                    print(
                        f"    {col}: {source} → {target} (diff: {diff:+.4g}, {diff_pct:+.2f}%)"
                    )
                else:
                    print(f"    {col}: {source} → {target}")
else:
    print("\nNo failed rows found.")
