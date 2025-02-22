import os
from typing import Any

import dlt
import polars as pl
import soccerdata as sd  # type: ignore

from src.loader.utils import print_debug_info, transform_player_data
from src.utils.pandas import flatten_pd_dataframe


def fetch_premier_league_data(season: str) -> list[dict[str, Any]]:
    """Fetch Premier League player data from FBref.

    Args:
        season: The season to fetch data for, e.g. "2024" for 2024/25 season

    Returns:
        List of player dictionaries containing stats and info
    """
    try:
        # Initialize FBref scraper and get data
        fbref: sd.FBref = sd.FBref(leagues="ENG-Premier League", seasons=season)
        raw_stats = fbref.read_player_season_stats()

        # Flatten the complex pandas DataFrame structure
        flat_df = flatten_pd_dataframe(df=raw_stats)

        # Convert to polars and transform
        pl_df = pl.from_pandas(data=flat_df)
        players_df = transform_player_data(df=pl_df)

        # Print debug info to help with test data creation
        print_debug_info(raw_df=raw_stats, flat_df=flat_df, final_df=players_df)

        return players_df.to_dicts()

    except Exception as e:
        raise Exception(f"Error fetching FBref data: {e}")


def generate_sample_data() -> list[dict]:
    """Generate football transfer listing data."""
    try:
        return fetch_premier_league_data(season="2024")
    except Exception as e:
        print(f"Error fetching real data: {e}.")
        raise Exception(f"Error fetching real data: {e}.")


def main() -> None:
    """Main function to load transfer listing data into DuckDB."""
    # Ensure database directory exists
    os.makedirs("database", exist_ok=True)

    # Create pipeline that loads to database/shell_corp.duckdb
    pipeline = dlt.pipeline(
        pipeline_name="shell_corp",
        destination=dlt.destinations.duckdb("database/shell_corp.duckdb"),
        dataset_name="raw",
    )

    # Load the data / Run the pipeline
    data = generate_sample_data()
    info = pipeline.run(
        data, table_name="transfer_listings", write_disposition="replace"
    )
    print(f"Load info: {info}")


if __name__ == "__main__":
    main()
