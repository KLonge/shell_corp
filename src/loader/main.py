import os
from itertools import islice
from typing import Any, Iterator

import dlt
import polars as pl
import soccerdata as sd  # type: ignore

from src.loader.utils import print_debug_info, transform_player_data
from src.utils.pandas import flatten_pd_dataframe


def fetch_premier_league_data(season: str) -> pl.DataFrame:
    """Fetch Premier League player data from FBref.

    Args:
        season: The season to fetch data for, e.g. "2024" for 2024/25 season

    Returns:
        Polars DataFrame containing transformed player data
    """
    try:
        fbref: sd.FBref = sd.FBref(leagues="ENG-Premier League", seasons=season)
        raw_stats_df = fbref.read_player_season_stats()

        flat_df = flatten_pd_dataframe(df=raw_stats_df)

        pl_df = pl.from_pandas(data=flat_df)
        players_df = transform_player_data(df=pl_df)

        print_debug_info(raw_df=raw_stats_df, flat_df=flat_df, final_df=players_df)

        return players_df

    except Exception as e:
        raise Exception(f"Error fetching FBref data: {e}")


@dlt.resource(name="transfer_listings", write_disposition="replace")
def generate_player_data(chunk_size: int = 100) -> Iterator[list[dict[str, Any]]]:
    """
    This function generates football transfer listing data in chunks.
    Normally, you could call an API and it would yield data in chunks, 
    but we are simulating this for demo purposes.

    Args:
        chunk_size: Number of records per chunk when yielding data

    Yields:
        Chunks of player data dictionaries
    """
    try:
        print("Fetching and transforming player data...")
        players_df = fetch_premier_league_data(season="2024")

        print(f"Total records to process: {players_df.height}")

        all_records = players_df.to_dicts()
        records_iter = iter(all_records)

        records_processed = 0
        while True:
            chunk = list(islice(records_iter, chunk_size))
            if not chunk:
                break

            records_processed += len(chunk)
            print(
                f"Yielding chunk of {len(chunk)} records. Progress: {records_processed}/{players_df.height}"
            )
            yield chunk

        print("Finished processing all records")

    except Exception as e:
        raise Exception(f"Error generating player data: {e}")


def main() -> None:
    """Main function to load transfer listing data into DuckDB."""
    # Ensure database directory exists
    os.makedirs("database", exist_ok=True)

    print("Starting data pipeline...")
    # Create pipeline that loads to database/shell_corp.duckdb
    pipeline = dlt.pipeline(
        pipeline_name="shell_corp",
        destination=dlt.destinations.duckdb("database/shell_corp.duckdb"),
        dataset_name="raw",
    )

    # Run the pipeline with the generator resource
    info = pipeline.run(generate_player_data())
    print(f"Pipeline completed. Load info: {info}")


if __name__ == "__main__":
    main()
