import datetime as dt
import random

import pandas as pd
import polars as pl

from src.loader.constants import (
    CONTRACT_MAX_DAYS,
    CONTRACT_MIN_DAYS,
    MARKET_VALUE_MAX,
    MARKET_VALUE_MIN,
)
from src.loader.models import PlayerSchema


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
    market_value_min: int = MARKET_VALUE_MIN,
    market_value_max: int = MARKET_VALUE_MAX,
    contract_min_days: int = CONTRACT_MIN_DAYS,
    contract_max_days: int = CONTRACT_MAX_DAYS,
) -> list[pl.Series]:
    """Generate synthetic columns for player data.

    Generates synthetic data including player IDs, market values, contract end dates,
    and transfer status. All generated data follows the PlayerSchema validation rules.

    Args:
        df: Input DataFrame to generate synthetic columns for
        market_value_min: Minimum market value in euros
        market_value_max: Maximum market value in euros
        contract_min_days: Minimum days until contract end
        contract_max_days: Maximum days until contract end (must be <= 5*365)

    Returns:
        List of Polars Series containing the synthetic columns
    """
    min_date = dt.datetime.now().date() + dt.timedelta(days=contract_min_days)
    max_date = dt.datetime.now().date() + dt.timedelta(days=contract_max_days)
    days_range = (max_date - min_date).days

    # Ensure we generate some unavailable players too
    transfer_statuses = ["available", "unavailable"]

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
        # Generate contract end dates as Date type
        pl.Series(
            [
                min_date + dt.timedelta(days=random.randint(0, days_range))
                for _ in range(df.height)
            ]
        )
        .cast(pl.Date)
        .alias("contract_end_date"),
        # Generate random transfer status
        pl.Series([random.choice(transfer_statuses) for _ in range(df.height)])
        .cast(pl.Utf8)
        .alias("transfer_status"),
    ]


def transform_player_data(df: pl.DataFrame) -> pl.DataFrame:
    """Transform raw player data into the required format and validate schema.

    Args:
        df: Raw player DataFrame from FBref

    Returns:
        Transformed DataFrame with standardized columns, validated against PlayerSchema

    Raises:
        patito.ValidationError: If the transformed data doesn't match the schema
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

    players_df = players_df.with_columns(generate_synthetic_columns(players_df))

    validated_df: pl.DataFrame = PlayerSchema.validate(players_df)

    return validated_df
