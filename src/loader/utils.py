import datetime as dt
import hashlib
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


def generate_player_id(name: str, club: str, nation: str) -> str:
    """
    Generate a 'unique' (not really, but low chance of collision)
    player ID based on player attributes.

    Args:
        name: Player's name
        club: Current club
        nation: Player's nationality

    Returns:
        A unique player ID in format PLY{8 digits}
    """
    # Create a consistent string to hash
    hash_input = f"{name}:{club}:{nation}".lower()

    # Generate SHA-256 hash and take first 8 digits
    hash_obj = hashlib.sha256(hash_input.encode())
    hash_hex = hash_obj.hexdigest()

    # Take first 8 characters of hash and convert to integer
    hash_int = int(hash_hex[:8], 16)
    # Ensure it's 8 digits by modding with 100000000
    id_num = hash_int % 100000000

    return f"PLY{id_num:08d}"


def transform_player_data(df: pl.DataFrame) -> pl.DataFrame:
    """Transform raw player data into the required format.

    Args:
        df: Raw player DataFrame from FBref

    Returns:
        Transformed DataFrame with standardized columns
    """
    # Generate random future dates between 2025-03-01 and 2027-12-31
    min_date = dt.date(2025, 3, 1)
    max_date = dt.date(2027, 12, 31)
    days_range = (max_date - min_date).days

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
    players_df = players_df.with_columns(
        [
            # Generate deterministic IDs based on player attributes
            pl.struct(["player_name", "current_club", "nation"])
            .map_elements(
                lambda x: generate_player_id(
                    x["player_name"], x["current_club"], x["nation"]
                )
            )
            .alias("id"),
            # Generate random market values between 1M and 100M
            pl.Series(
                name="market_value_euro",
                values=[
                    random.randint(1_000_000, 100_000_000)
                    for _ in range(players_df.height)
                ],
            ),
            # Generate random future contract end dates
            pl.Series(
                name="contract_end_date",
                values=[
                    (
                        min_date + dt.timedelta(days=random.randint(0, days_range))
                    ).isoformat()
                    for _ in range(players_df.height)
                ],
            ),
            pl.lit("available").alias("transfer_status"),
        ]
    )

    return players_df
