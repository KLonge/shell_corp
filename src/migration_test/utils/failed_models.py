from src.migration_test.models import ComparisonResult


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
