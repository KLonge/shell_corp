import datetime as dt

from src.clients.duckdb import DuckDBClient
from src.migration_test.models import ComparisonResult


def compare_duckdb_tables(
    duckdb_client: DuckDBClient,
    table1_name: str,
    table2_name: str,
    primary_key: list[str],
    value_tolerance: float = 0.05,
    row_tolerance: float = 0.05,
    exclude_columns: list[str] | None = None,
    timestamp_range_exclude: tuple[dt.datetime, dt.datetime] | None = None,
    trim_strings: bool = False,
) -> ComparisonResult:
    """
    Compare two tables in DuckDB and return a comparison result.

    This function compares two tables in DuckDB based on a primary key and returns
    a ComparisonResult object with information about the comparison.

    Args:
        duckdb_client: DuckDB client to use for the comparison
        table1_name: Name of the first table to compare
        table2_name: Name of the second table to compare
        primary_key: List of column names that form the primary key
        value_tolerance: Tolerance for numeric comparisons (0.05 = 5%)
        row_tolerance: Maximum percentage of rows that can fail (0.05 = 5%)
        exclude_columns: List of columns to exclude from comparison
        timestamp_range_exclude: Tuple of (start_time, end_time) to exclude from timestamp comparisons
        trim_strings: Whether to trim string values before comparison

    Returns:
        ComparisonResult: Object containing comparison results
    """
    print(f"\nCOMPARING TABLES: {table1_name} AND {table2_name}")

    # Get metadata for both tables
    meta1_query = f"""
    SELECT column_name, data_type
    FROM information_schema.columns
    WHERE table_name = '{table1_name.split(".")[-1]}'
    AND table_schema = '{table1_name.split(".")[0]}'
    """

    meta2_query = f"""
    SELECT column_name, data_type
    FROM information_schema.columns
    WHERE table_name = '{table2_name.split(".")[-1]}'
    AND table_schema = '{table2_name.split(".")[0]}'
    """

    meta1 = duckdb_client.query(meta1_query)
    meta2 = duckdb_client.query(meta2_query)

    # Convert to dictionaries for easier lookup
    meta1_dict = dict(zip(meta1["column_name"], meta1["data_type"]))
    meta2_dict = dict(zip(meta2["column_name"], meta2["data_type"]))

    print(f"meta1_dict: {meta1_dict}")
    print(f"meta2_dict: {meta2_dict}")

    # Determine columns to compare
    all_columns = set(meta1_dict.keys()) & set(meta2_dict.keys())
    if exclude_columns:
        all_columns = all_columns - set(exclude_columns)

    valid_columns = list(all_columns)

    print(f"\nColumns to compare: {len(valid_columns)}")
    if exclude_columns:
        print(f"Columns excluded: {len(exclude_columns)}")

    # Build column comparisons
    column_comparisons = {}
    for col in valid_columns:
        data_type = meta1_dict[col].lower()

        if "timestamp" in data_type:
            t1_col = f"t1_{col}"
            t2_col = f"t2_{col}"

            if timestamp_range_exclude:
                start_time, end_time = timestamp_range_exclude
                column_comparisons[col] = f"""
                    ({t1_col} = {t2_col} OR ({t1_col} IS NULL AND {t2_col} IS NULL))
                    OR (
                        {t1_col} BETWEEN '{start_time}' AND '{end_time}'
                        OR {t2_col} BETWEEN '{start_time}' AND '{end_time}'
                    )
                """
            else:
                column_comparisons[col] = (
                    f"{t1_col} = {t2_col} OR ({t1_col} IS NULL AND {t2_col} IS NULL)"
                )

        elif any(
            num_type in data_type
            for num_type in ["number", "decimal", "numeric", "float", "int"]
        ):
            column_comparisons[col] = f"""
                ABS(COALESCE(t1_{col}, 0) - COALESCE(t2_{col}, 0)) <= 
                {value_tolerance} * GREATEST(ABS(COALESCE(t1_{col}, 0)), 
                ABS(COALESCE(t2_{col}, 0)), 1) 
                OR (t1_{col} IS NULL AND t2_{col} IS NULL)
            """
        elif any(
            str_type in data_type for str_type in ["string", "varchar", "char", "text"]
        ):
            t1_col = f"t1_{col}"
            t2_col = f"t2_{col}"
            if trim_strings:
                t1_col = f"TRIM({t1_col})"
                t2_col = f"TRIM({t2_col})"
            column_comparisons[col] = (
                f"{t1_col} = {t2_col} OR ({t1_col} IS NULL AND {t2_col} IS NULL)"
            )
        else:
            # For any other data types, use direct comparison
            column_comparisons[col] = (
                f"t1_{col} = t2_{col} OR (t1_{col} IS NULL AND t2_{col} IS NULL)"
            )

    # Build the comparison query
    pk_join_condition = " AND ".join([f"t1.{pk} = t2.{pk}" for pk in primary_key])

    # Select columns for the base query
    select_columns = []
    for col in primary_key:
        select_columns.append(f"t1.{col}")

    for col in valid_columns:
        select_columns.append(f"t1.{col} AS t1_{col}")
        select_columns.append(f"t2.{col} AS t2_{col}")

    select_clause = ", ".join(select_columns)

    # Get total row counts for both tables
    total_rows_query = f"""
    SELECT 
        (SELECT COUNT(*) FROM {table1_name}) as table1_count,
        (SELECT COUNT(*) FROM {table2_name}) as table2_count
    """
    row_counts = duckdb_client.query(total_rows_query).iloc[0]
    total_rows_table1 = int(row_counts["table1_count"])
    total_rows_table2 = int(row_counts["table2_count"])

    # Check for row count mismatches
    if total_rows_table1 != total_rows_table2:
        row_diff = abs(total_rows_table1 - total_rows_table2)
        row_diff_perc = row_diff / max(total_rows_table1, total_rows_table2)

        print("\n⚠️ Row count mismatch between tables:")
        print(f"Table 1 ({table1_name}): {total_rows_table1:,} rows")
        print(f"Table 2 ({table2_name}): {total_rows_table2:,} rows")
        print(f"Difference: {row_diff:,} rows ({row_diff_perc:.2%})")

        if row_diff_perc > row_tolerance:
            print(
                f"❌ Row count difference ({row_diff_perc:.2%}) exceeds tolerance ({row_tolerance:.2%})"
            )
            return ComparisonResult(
                passed=False,
                comparison_sql="",
                failed_row_perc=row_diff_perc,
                total_rows=max(total_rows_table1, total_rows_table2),
                failed_columns={},
                value_tolerance=value_tolerance,
                row_tolerance=row_tolerance,
                sample_failed_rows=None,
            )
        else:
            print(
                f"✅ Row count difference ({row_diff_perc:.2%}) within tolerance ({row_tolerance:.2%})"
            )

    # Build the main comparison query
    comparison_query = f"""
    WITH base_keys AS (
        SELECT {select_clause}
        FROM {table1_name} t1
        INNER JOIN {table2_name} t2
        ON {pk_join_condition}
    ),
    test_data AS (
        SELECT *,
        CASE WHEN {" AND ".join([f"({comp})" for comp in column_comparisons.values()])}
            THEN TRUE ELSE FALSE END AS row_passed
        FROM base_keys
    )
    SELECT 
        COUNT(*) AS total_rows,
        SUM(CASE WHEN NOT row_passed THEN 1 ELSE 0 END) AS failed_rows,
        {", ".join([f"SUM(CASE WHEN NOT ({comp}) THEN 1 ELSE 0 END) AS {col}_fails" for col, comp in column_comparisons.items()])}
    FROM test_data
    """

    result = duckdb_client.query(comparison_query)

    # Extract results
    total_rows = int(result["total_rows"].iloc[0])
    failed_rows = int(result["failed_rows"].iloc[0])

    # Calculate failed columns
    failed_columns = {}
    for col in valid_columns:
        col_fails = int(result[f"{col}_fails"].iloc[0])
        if col_fails > 0:
            failed_columns[col] = col_fails

    # Calculate failed row percentage
    failed_row_perc = failed_rows / total_rows if total_rows > 0 else 0

    # Determine if the comparison passed
    passed = failed_row_perc <= row_tolerance

    # Print comparison results
    print("\nComparison Results:")
    print(f"Total rows compared: {total_rows:,}")
    print(f"Failed rows: {failed_rows:,} ({failed_row_perc:.2%})")
    print(f"Row tolerance: {row_tolerance:.2%}")

    if passed:
        print(
            f"\n✅ Differences ({failed_row_perc:.2%}) within acceptable row tolerance ({row_tolerance:.2%})"
        )
    else:
        print(
            f"\n❌ Differences ({failed_row_perc:.2%}) exceed row tolerance ({row_tolerance:.2%})"
        )

    if failed_columns:
        print("\nColumns with differences:")
        for col, count in failed_columns.items():
            print(f"  - {col}: {count:,} rows ({count / total_rows:.2%})")

    # If there are failed rows, get a sample of them
    sample_failed_rows = None
    if failed_rows > 0:
        # Query to get sample failed rows
        sample_query = f"""
        WITH base_keys AS (
            SELECT {select_clause}
            FROM {table1_name} t1
            INNER JOIN {table2_name} t2
            ON {pk_join_condition}
        ),
        test_data AS (
            SELECT *,
            CASE WHEN {" AND ".join([f"({comp})" for comp in column_comparisons.values()])}
                THEN TRUE ELSE FALSE END AS row_passed,
            {", ".join([f"CASE WHEN NOT ({comp}) THEN FALSE ELSE TRUE END AS {col}_passed" for col, comp in column_comparisons.items()])}
            FROM base_keys
        )
        SELECT 
            {", ".join([f"{pk}" for pk in primary_key])},
            {", ".join([f"t1_{col} AS {col}_source" for col in valid_columns])},
            {", ".join([f"t2_{col} AS {col}_target" for col in valid_columns])},
            {", ".join([f"NOT {col}_passed AS {col}_failed" for col in valid_columns])},
            row_passed
        FROM test_data
        WHERE NOT row_passed
        ORDER BY {primary_key[0]}
        LIMIT 5
        """

        print("\nSample query for failed rows:")
        print(sample_query)

        sample_failed_rows = duckdb_client.query(sample_query).to_dict(orient="records")
        print(f"Found {len(sample_failed_rows)} sample failed rows")

        # Print sample of failed rows
        if sample_failed_rows:
            print("\nShowing sample differences:")
            for i, row in enumerate(sample_failed_rows):
                print(f"\n--- Sample {i + 1} ---")
                # Print primary key values
                pk_values = ", ".join([f"{pk}={row.get(pk)}" for pk in primary_key])
                print(f"Primary Key: {pk_values}")

                # Add a list of failed columns for easier reference
                row["failed_columns"] = [
                    col for col in valid_columns if row.get(f"{col}_failed", False)
                ]

                print("Failed columns:")
                for col in row["failed_columns"]:
                    source_val = row.get(f"{col}_source")
                    target_val = row.get(f"{col}_target")
                    print(f"  - {col}: {source_val} vs {target_val}")

                # Add a dictionary of value differences for failed columns
                row["value_differences"] = {}
                for col in row["failed_columns"]:
                    source_val = row.get(f"{col}_source")
                    target_val = row.get(f"{col}_target")

                    # Add additional information for numeric differences
                    diff_info = {}
                    if isinstance(source_val, (int, float)) and isinstance(
                        target_val, (int, float)
                    ):
                        diff_info["diff"] = target_val - source_val
                        diff_info["diff_pct"] = (
                            (diff_info["diff"] / source_val * 100)
                            if source_val != 0
                            else float("inf")
                        )
                        print(
                            f"    Numeric diff: {diff_info['diff']} ({diff_info['diff_pct']:.2f}%)"
                        )

                    row["value_differences"][col] = {
                        "source": source_val,
                        "target": target_val,
                        **diff_info,
                    }

    # Create and return the comparison result
    return ComparisonResult(
        passed=passed,
        comparison_sql=comparison_query,
        failed_row_perc=failed_row_perc,
        total_rows=total_rows,
        failed_columns=failed_columns,
        value_tolerance=value_tolerance,
        row_tolerance=row_tolerance,
        sample_failed_rows=sample_failed_rows,
    )
