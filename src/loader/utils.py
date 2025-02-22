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


def flatten_fbref_pd_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Flatten FBref's multi-index DataFrame into a simple format.

    Takes a raw FBref DataFrame with multi-index columns and hierarchical index
    and converts it to a flat DataFrame with simple column names and no index.

    Args:
        df: Raw DataFrame from FBref with multi-index columns

    Returns:
        Flattened DataFrame with standardized column names
    """
    # Create a copy to avoid modifying the input
    flat_df = df.copy()

    # Flatten multi-index columns
    flat_df.columns = pd.Index(
        [f"{col[0]}_{col[1]}" if col[1] != "" else col[0] for col in flat_df.columns]
    )

    # Reset the hierarchical index to columns
    return flat_df.reset_index()


def transform_player_data(df: pl.DataFrame) -> pl.DataFrame:
    """Transform raw player data into the required format.

    Args:
        df: Raw player DataFrame with flattened column names from FBref

    Returns:
        Transformed DataFrame with standardized columns for transfer analysis
    """
    players_df = df.select(
        [
            "team",
            "player",
            "pos",
            "age",
            "nation",
        ]
    ).rename({"team": "current_club", "player": "player_name", "pos": "position"})

    # Clean age data - extract just the years from "27-137" format
    players_df = players_df.with_columns(
        [pl.col("age").str.split("-").list.first().cast(pl.Int64).alias("age")]
    )

    # Add constant columns
    players_df = players_df.with_columns(
        [
            pl.lit(None).alias("id"),
            pl.lit(None).alias("market_value_euro"),
            pl.lit(None).alias("contract_end_date"),
            pl.lit("available").alias("transfer_status"),
        ]
    )

    return players_df
