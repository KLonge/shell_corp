import datetime as dt
import random

import pandas as pd
import polars as pl


def print_debug_info(
    raw_df: pd.DataFrame, flat_df: pd.DataFrame, final_df: pl.DataFrame
) -> None:
    """Print debug information about the data at each transformation stage.

    Args:
        raw_df: The raw DataFrame from FBref
        flat_df: The flattened pandas DataFrame
        final_df: The final transformed polars DataFrame
    """
    print("\nRaw DataFrame Info:")
    print("Index names:", raw_df.index.names)
    print("\nColumn names and types:")
    print(raw_df.dtypes)
    print("\nSample data (first 2 rows as records):")
    print(raw_df.head(2).to_dict("records"))

    print("\nFlattened DataFrame Info:")
    print("Column names and types:")
    print(flat_df.dtypes)
    print("\nSample flattened data (first 2 rows as records):")
    print(flat_df.head(2).to_dict("records"))

    print("\nFinal Polars DataFrame Info:")
    print("Schema:")
    print(final_df.schema)
    print("\nSample transformed data (first 2 rows as dicts):")
    print(final_df.head(2).to_dicts())


def generate_synthetic_columns(
    df: pl.DataFrame,
    *,  # Force keyword arguments
    market_value_min: int = 1_000_000,
    market_value_max: int = 100_000_000,
    contract_min_days: int = 30,
    contract_max_days: int = 365 * 2,
) -> list[pl.Series]:
    """Generate synthetic columns for player data."""
    min_date = dt.datetime.now().date() + dt.timedelta(days=contract_min_days)
    max_date = dt.datetime.now().date() + dt.timedelta(days=contract_max_days)
    days_range = (max_date - min_date).days

    return [
        # Generate sequential IDs
        pl.int_range(1, df.height + 1, eager=True)
        .cast(pl.Int64)
        .map_elements(
            lambda x: f"PLY{int(x):08d}", return_dtype=pl.Utf8, skip_nulls=False
        )
        .alias("id"),
        # Generate market values
        pl.Series(
            [
                random.randint(market_value_min, market_value_max)
                for _ in range(df.height)
            ]
        )
        .cast(pl.Int64)
        .alias("market_value_euro"),
        # Generate contract end dates
        pl.Series(
            [
                (
                    min_date + dt.timedelta(days=random.randint(0, days_range))
                ).isoformat()
                for _ in range(df.height)
            ]
        )
        .cast(pl.Utf8)
        .alias("contract_end_date"),
        # Static transfer status
        pl.Series(["available"] * df.height).cast(pl.Utf8).alias("transfer_status"),
    ]


def transform_player_data(df: pl.DataFrame) -> pl.DataFrame:
    """Transform raw player data into the required format.

    Args:
        df: Raw player DataFrame from FBref

    Returns:
        Transformed DataFrame with standardized columns
    """
    # Extract age as integer from "age" column which has format "27-137"
    players_df = df.with_columns(
        [
            pl.col("team").alias("current_club"),
            pl.col("player").alias("player_name"),
            pl.col("pos").alias("position"),
            # Extract numeric age from "27-137" format
            pl.col("age").str.split("-").list.first().cast(pl.Int64).alias("age"),
        ]
    )

    # Select and rename relevant columns
    players_df = players_df.select(
        [
            "current_club",
            "player_name",
            "position",
            "age",
            "nation",
        ]
    )

    # Add generated columns
    players_df = players_df.with_columns(generate_synthetic_columns(players_df))

    return players_df
