import datetime as dt

import pandas as pd

from unmind_data_stack.misc_scripts.snowflake_migration.models import ComparisonResult
from unmind_data_stack.resources.snowflake import (
    SnowflakeResource,
)


def are_types_compatible(type1: str, type2: str) -> bool:
    """
    Check if two Snowflake data types are compatible for comparison.

    Args:
        type1: Data type from first table
        type2: Data type from second table

    Returns:
        bool: True if types are compatible
    """
    type1, type2 = type1.upper(), type2.upper()

    # Define compatible type groups
    compatible_types = {
        "TIMESTAMP": {"TIMESTAMP_NTZ", "TIMESTAMP_TZ", "TIMESTAMP_LTZ", "TIMESTAMP"},
        "NUMBER": {"NUMBER", "DECIMAL", "NUMERIC", "INT", "INTEGER", "BIGINT", "FLOAT"},
        "STRING": {"STRING", "VARCHAR", "CHAR", "TEXT"},
        "BOOLEAN": {"BOOLEAN", "BOOL"},
    }

    # Check if types are exactly the same
    if type1 == type2:
        return True

    # Check if types belong to the same compatible group
    for group in compatible_types.values():
        type1_base = next((t for t in group if type1.startswith(t)), None)
        type2_base = next((t for t in group if type2.startswith(t)), None)
        if type1_base and type2_base:
            return True

    return False


def get_table_metadata(
    snowflake_client: SnowflakeResource, table_name: str
) -> pd.DataFrame:
    """Get table metadata from Snowflake information schema."""
    schema_name = table_name.split(".")[0]
    table_name_only = table_name.split(".")[1]

    query = f"""
    SELECT COLUMN_NAME, DATA_TYPE
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_NAME = UPPER('{table_name_only}')
    AND TABLE_SCHEMA = UPPER('{schema_name}')
    AND COLUMN_NAME NOT IN ('_DLT_LOAD_ID', '_DLT_ID')
    ORDER BY ORDINAL_POSITION
    """
    return snowflake_client.query(query)


def _build_column_comparisons(
    valid_columns: list[str],
    meta1_dict: dict[str, str],
    value_tolerance: float,
    timestamp_sql_range: tuple[str, str] | None = None,
    trim_strings: bool = False,
) -> dict[str, str]:
    """Build SQL comparison conditions for each column based on its data type.

    Args:
        valid_columns: List of columns to compare
        meta1_dict: Dictionary mapping column names to their data types
        value_tolerance: Tolerance for numeric comparisons
        timestamp_sql_range: Optional tuple of (start_time, end_time) for timestamp filtering
        trim_strings: Whether to trim string values before comparison

    Returns:
        dict: Mapping of column names to their SQL comparison conditions
    """
    column_comparisons = {}
    for col in valid_columns:
        data_type = meta1_dict[col].upper()

        if any(type_name in data_type for type_name in ["TIMESTAMP"]):
            t1_col = f"REPLACE(REPLACE(REPLACE(REPLACE(bk.t1_{col}::STRING, ' Z', ''), ' UTC', ''), ' +00', ''), ' -0800', '')"
            t2_col = f"REPLACE(REPLACE(REPLACE(REPLACE(td.t2_{col}::STRING, ' Z', ''), ' UTC', ''), ' +00', ''), ' -0800', '')"

            base_condition = (
                f"{t1_col} = {t2_col} OR (bk.t1_{col} IS NULL AND td.t2_{col} IS NULL)"
            )

            # Add timestamp range exclusion if specified
            if timestamp_sql_range:
                start_time, end_time = timestamp_sql_range
                base_condition += f"""
                    OR (
                        bk.t1_{col} BETWEEN '{start_time}' AND '{end_time}'
                        OR td.t2_{col} BETWEEN '{start_time}' AND '{end_time}'
                    )"""

            column_comparisons[col] = base_condition

        elif any(
            type_name in data_type
            for type_name in ["NUMBER", "DECIMAL", "NUMERIC", "FLOAT"]
        ):
            column_comparisons[col] = (
                f"ABS(COALESCE(bk.t1_{col}, 0) - COALESCE(td.t2_{col}, 0)) <= "
                f"{value_tolerance} * GREATEST(ABS(COALESCE(bk.t1_{col}, 0)), "
                f"ABS(COALESCE(td.t2_{col}, 0)), 1) "
                f"OR (bk.t1_{col} IS NULL AND td.t2_{col} IS NULL)"
            )

        elif any(
            type_name in data_type
            for type_name in ["STRING", "VARCHAR", "CHAR", "TEXT"]
        ):
            t1_col = f"bk.t1_{col}"
            t2_col = f"td.t2_{col}"
            if trim_strings:
                t1_col = f"TRIM({t1_col})"
                t2_col = f"TRIM({t2_col})"
            column_comparisons[col] = (
                f"{t1_col} = {t2_col} "
                f"OR (bk.t1_{col} IS NULL AND td.t2_{col} IS NULL)"
            )

        else:
            # For any other data types, use direct comparison
            column_comparisons[col] = (
                f"bk.t1_{col} = td.t2_{col} "
                f"OR (bk.t1_{col} IS NULL AND td.t2_{col} IS NULL)"
            )

    return column_comparisons


def _build_comparison_query(
    table1_name: str,
    table2_name: str,
    pk_columns: list[str],
    valid_columns: list[str],
    column_comparisons: dict[str, str],
    trim_strings: bool = False,
) -> str:
    """Build the main comparison SQL query.

    Args:
        table1_name: Name of first table
        table2_name: Name of second table
        pk_columns: List of primary key columns
        valid_columns: List of columns to compare
        column_comparisons: Dictionary of column comparison conditions
        trim_strings: Whether to trim string values before comparison

    Returns:
        str: Complete SQL query for comparing tables
    """
    # Create a concatenated surrogate key for efficient joining
    pk_concat = " || '_' || ".join(
        f"COALESCE(CAST({col} AS STRING), 'NULL')" for col in pk_columns
    )
    if trim_strings:
        pk_concat = " || '_' || ".join(
            f"COALESCE(TRIM(CAST({col} AS STRING)), 'NULL')" for col in pk_columns
        )

    # Add primary key columns to the output for reference
    pk_columns_sql = [f"t1.{col} as {col}" for col in pk_columns]

    return f"""
    WITH base_keys AS (
        SELECT 
            {pk_concat} as surrogate_key,
            {', '.join(pk_columns_sql)},
            {', '.join(f't1.{col} as t1_{col}' for col in valid_columns)}
        FROM {table1_name} t1
    ),
    target_data AS (
        SELECT 
            {pk_concat} as surrogate_key,
            {', '.join(f't2.{col} as t2_{col}' for col in valid_columns)}
        FROM {table2_name} t2
    ),
    differences AS (
        SELECT 
            bk.{', '.join(pk_columns)},
            {', '.join(
                f'''
                CASE
                    WHEN {comparison_condition} THEN NULL
                    ELSE CONCAT(
                        '{col}: ',
                        COALESCE(CAST(bk.t1_{col} AS STRING), 'NULL'),
                        ' vs ',
                        COALESCE(CAST(td.t2_{col} AS STRING), 'NULL')
                    )
                END AS {col}_diff
                '''
                for col, comparison_condition in column_comparisons.items()
            )}
        FROM base_keys bk
        INNER JOIN target_data td ON bk.surrogate_key = td.surrogate_key
        WHERE {' OR '.join(f'NOT ({condition})' for condition in column_comparisons.values())}
    )
    SELECT * FROM differences
    WHERE {' OR '.join(f'{col}_diff IS NOT NULL' for col in valid_columns)}
    """.lstrip()


def compare_snowflake_tables(
    snowflake_client: SnowflakeResource,
    table1_name: str,
    table2_name: str,
    primary_key: str | list[str],
    value_tolerance: float = 0.05,
    row_tolerance: float = 0.05,
    exclude_columns: list[str] | None = None,
    timestamp_range_exclude: tuple[dt.datetime, dt.datetime] | None = None,
    sample_size: int = 5,
    trim_strings: bool = False,
) -> ComparisonResult:
    """Compare two Snowflake tables with standardized timestamp handling.

    Args:
        snowflake_client: SnowflakeResource instance for querying
        table1_name: Name of first table to compare
        table2_name: Name of second table to compare
        primary_key: Column name(s) to use as the unique identifier for joining.
            Can be a single string or list of strings for composite keys
        value_tolerance: Numeric tolerance for floating point comparisons
        row_tolerance: Maximum acceptable proportion of rows with differences
        exclude_columns: List of column names to exclude from comparison
        timestamp_range_exclude: Optional tuple of (start_datetime, end_datetime)
            Any timestamp columns with values in this range (inclusive) will be
            ignored in the comparison
        sample_size: Number of sample differences to display in the report
        trim_strings: Whether to trim string values before comparison

    Returns:
        bool: True if tables match within both value and row tolerances
    """
    print(f"\nCOMPARING TABLES: {table1_name} AND {table2_name}")

    if timestamp_range_exclude is not None:
        start_ts, end_ts = timestamp_range_exclude
        if start_ts > end_ts:
            raise ValueError(
                f"Invalid timestamp range: start time {start_ts} "
                f"is after end time {end_ts}"
            )
        # Convert to ISO format string for SQL query
        timestamp_sql_range = (start_ts.isoformat(), end_ts.isoformat())

    # Convert single column to list for consistent handling
    pk_columns = [primary_key] if isinstance(primary_key, str) else primary_key

    # Ensure primary key columns exist in both tables
    metadata1 = get_table_metadata(snowflake_client, table1_name)
    metadata2 = get_table_metadata(snowflake_client, table2_name)

    meta1_cols = set(metadata1["column_name"])
    meta2_cols = set(metadata2["column_name"])

    missing_pks = [
        col for col in pk_columns if col not in meta1_cols or col not in meta2_cols
    ]
    if missing_pks:
        print(f"TABLE NAME: {table1_name}")
        print(f"TABLE 1 COLUMNS: {meta1_cols}")
        print(f"TABLE 2 COLUMNS: {meta2_cols}")

        raise ValueError(f"Primary key columns {missing_pks} not found in both tables")

    # Create metadata dictionaries for easier lookup
    meta1_dict = dict(zip(metadata1["column_name"], metadata1["data_type"]))
    meta2_dict = dict(zip(metadata2["column_name"], metadata2["data_type"]))

    # Find columns present in both tables, including primary keys in comparison
    common_cols = set(meta1_dict.keys()) & set(meta2_dict.keys())
    comparison_cols = list(common_cols)  # Compare all common columns

    # Check for type mismatches
    type_mismatches = {}
    valid_columns = []

    for col in comparison_cols:
        if exclude_columns and col in exclude_columns:
            continue

        type1 = meta1_dict[col]
        type2 = meta2_dict[col]

        if not are_types_compatible(type1, type2):
            type_mismatches[col] = (type1, type2)
        else:
            valid_columns.append(col)

    if type_mismatches:
        print("\n⚠️ Columns excluded due to incompatible types:")
        for col, (type1, type2) in type_mismatches.items():
            print(f"- {col}: {table1_name} is {type1}, {table2_name} is {type2}")

    if not valid_columns:
        print("\n❌ No valid columns to compare after excluding type mismatches!")
        return ComparisonResult(
            model_name=table1_name,
            passed=False,
            comparison_sql="",
            failed_row_perc=0.0,
            total_rows=0,
            failed_columns={},
            value_tolerance=value_tolerance,
            row_tolerance=row_tolerance,
        )

    # Create a concatenated surrogate key with special handling for timestamp columns
    pk_parts = []
    for col in pk_columns:
        if (
            "DATE" in meta1_dict[col].upper()
            and "TIMESTAMP" not in meta1_dict[col].upper()
        ):
            # For DATE columns, convert to epoch days to avoid time component issues
            part = f"COALESCE(CAST(DATE_PART('EPOCH', {col}) AS STRING), 'NULL')"
        elif any(
            ts_type in meta1_dict[col].upper()
            for ts_type in ["TIMESTAMP", "TIMESTAMPTZ"]
        ):
            # For timestamp columns, use millisecond precision
            part = f"COALESCE(CAST(DATE_PART('EPOCH_MILLISECOND', {col}) AS STRING), 'NULL')"
        else:
            part = f"COALESCE(CAST({col} AS STRING), 'NULL')"
            if trim_strings:
                part = f"COALESCE(TRIM(CAST({col} AS STRING)), 'NULL')"
        pk_parts.append(part)

    # Join the parts with the separator
    pk_concat = " || '_' || ".join(pk_parts)

    validation_query = f"""
    WITH source_ids AS (
        SELECT {pk_concat} as surrogate_key 
        FROM {table1_name}
    ),
    target_counts AS (
        SELECT 
            COUNT(*) as total_count,
            COUNT(CASE WHEN ({pk_concat}) IN (SELECT surrogate_key FROM source_ids) THEN 1 END) as matching_count
        FROM {table2_name}
    )
    SELECT total_count, matching_count
    FROM target_counts
    """

    counts = snowflake_client.query(query=validation_query).iloc[0]
    if counts["total_count"] != counts["matching_count"]:
        missing_rows = counts["total_count"] - counts["matching_count"]
        missing_percentage = missing_rows / counts["total_count"]

        print(f"\n⚠️ Row count mismatch in {table2_name}:")
        print(f"Total rows: {counts['total_count']:,}")
        print(f"Rows matching source IDs: {counts['matching_count']:,}")
        print(f"Missing rows: {missing_rows:,} ({missing_percentage:.2%})")

        if missing_percentage > row_tolerance:
            print(
                f"❌ Missing rows ({missing_percentage:.2%}) exceed tolerance ({row_tolerance:.2%})"
            )
            print(f"Validation query: \n{validation_query}")
            return ComparisonResult(
                model_name=table1_name,
                passed=False,
                comparison_sql=validation_query,
                failed_row_perc=missing_percentage,
                total_rows=counts["total_count"],
                failed_columns={},
                value_tolerance=value_tolerance,
                row_tolerance=row_tolerance,
            )
        else:
            print(
                f"✅ Missing rows ({missing_percentage:.2%}) within tolerance ({row_tolerance:.2%})"
            )

    # Create comparison conditions for each column
    column_comparisons = _build_column_comparisons(
        valid_columns=valid_columns,
        meta1_dict=meta1_dict,
        value_tolerance=value_tolerance,
        timestamp_sql_range=timestamp_sql_range,
        trim_strings=trim_strings,
    )

    comparison_query = _build_comparison_query(
        table1_name=table1_name,
        table2_name=table2_name,
        pk_columns=pk_columns,
        valid_columns=valid_columns,
        column_comparisons=column_comparisons,
        trim_strings=trim_strings,
    )

    # Execute comparison
    diff_df = snowflake_client.query(query=comparison_query)

    # Get total row counts for both tables
    total_rows_query = f"""
    SELECT 
        (SELECT COUNT(*) FROM {table1_name}) as table1_count,
        (SELECT COUNT(*) FROM {table2_name}) as table2_count
    """
    row_counts = snowflake_client.query(query=total_rows_query).iloc[0]
    total_rows = row_counts["table1_count"]
    total_rows_table2 = row_counts["table2_count"]

    # Calculate failed columns dictionary
    failed_columns: dict[str, int] = {}
    if not diff_df.empty:
        for col in valid_columns:
            diff_col = f"{col}_diff"
            if diff_col in diff_df.columns:
                failed_count = diff_df[diff_col].notna().sum()
                if failed_count > 0:
                    failed_columns[col] = failed_count

    print(f"\nColumns compared: {len(valid_columns)}")
    print(f"Columns skipped due to type mismatch: {len(type_mismatches)}")

    if diff_df.empty:
        print("\n✅ Compared columns match completely within specified tolerance!")
        return ComparisonResult(
            passed=True,
            comparison_sql=comparison_query,
            failed_row_perc=0.0,
            total_rows=total_rows_table2,
            failed_columns=failed_columns,
            value_tolerance=value_tolerance,
            row_tolerance=row_tolerance,
        )

    total_differences = len(diff_df)
    difference_percentage = total_differences / total_rows_table2

    print(f"\nFound {total_differences:,} rows with differences")
    print(f"Total rows table 1: {total_rows:,}")
    print(f"Total rows table 2: {total_rows_table2:,}")
    print(f"Percentage of differences: {difference_percentage:.2%}")

    if difference_percentage <= row_tolerance:
        print(
            f"\n✅ Differences ({difference_percentage:.2%}) within acceptable row tolerance ({row_tolerance:.2%})"
        )
    else:
        print(
            f"\n❌ Differences ({difference_percentage:.2%}) exceed row tolerance ({row_tolerance:.2%})"
        )

    if total_differences > sample_size:
        print(f"\nShowing {sample_size} sample differences:")
        sample_rows = diff_df.sample(n=sample_size)
    else:
        print("\nShowing all differences:")
        sample_rows = diff_df

    for _, row in sample_rows.iterrows():
        print("\n---")
        for col in diff_df.columns:
            if pd.notna(row[col]):
                print(f"- {row[col]}")

    return ComparisonResult(
        passed=difference_percentage <= row_tolerance,
        comparison_sql=comparison_query,
        failed_row_perc=difference_percentage,
        total_rows=total_rows_table2,
        failed_columns=failed_columns,
        value_tolerance=value_tolerance,
        row_tolerance=row_tolerance,
    )
