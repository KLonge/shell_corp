import dlt
import os
import soccerdata as sd
from typing import Sequence

def fetch_premier_league_data(season: str) -> Sequence[dict]:
    """Fetch Premier League player data from FBref.
    
    Args:
        season: The season to fetch data for, e.g. "2024" for 2024/25 season
        
    Returns:
        List of player dictionaries containing stats and info
    """
    try:
        # Initialize FBref scraper and get data
        fbref = sd.FBref(leagues="ENG-Premier League", seasons=season)
        stats = fbref.read_player_season_stats()
        
        # Reset index and select/rename columns
        players_df = stats.reset_index()[['player', 'team', 'pos', 'age']].copy()
        players_df.columns = ['player_name', 'current_club', 'position', 'age']
        
        # Clean age data - extract just the years
        players_df['age'] = players_df['age'].str.split('-').str[0]
        
        # Add constant columns
        players_df['id'] = None
        players_df['market_value_euro'] = None
        players_df['contract_end_date'] = None
        players_df['transfer_status'] = 'available'
        
        # Convert to list of dicts with proper types
        return players_df.astype({
            'player_name': str,
            'current_club': str,
            'position': str,
            'age': 'Int64'  # handles NaN values better than int
        }).to_dict('records')
        
    except Exception as e:
        raise Exception(f"Error fetching FBref data: {e}")

def generate_sample_data() -> list[dict]:
    """Generate football transfer listing data."""
    try:
        return fetch_premier_league_data(season="2024")
    except Exception as e:
        print(f"Error fetching real data: {e}. Falling back to sample data...")
        return [
            {
                "id": 1,
                "player_name": "Marcus Silva",
                "current_club": "FC Porto",
                "position": "Forward",
                "age": 23,
                "market_value_euro": 15000000,
                "contract_end_date": "2025-06-30",
                "transfer_status": "available"
            },
            {
                "id": 2,
                "player_name": "Thomas Weber",
                "current_club": "RB Leipzig",
                "position": "Midfielder",
                "age": 25,
                "market_value_euro": 22000000,
                "contract_end_date": "2025-12-31",
                "transfer_status": "loan_available"
            },
            {
                "id": 3,
                "player_name": "James Wilson",
                "current_club": "Ajax Amsterdam",
                "position": "Defender",
                "age": 21,
                "market_value_euro": 8000000,
                "contract_end_date": "2026-06-30",
                "transfer_status": "available"
            },
        ]

def main() -> None:
    """Main function to load transfer listing data into DuckDB."""
    # Ensure database directory exists
    os.makedirs("database", exist_ok=True)
    
    # Create pipeline that loads to database/shell_corp.duckdb
    pipeline = dlt.pipeline(
        pipeline_name="shell_corp",
        destination=dlt.destinations.duckdb(
            "database/shell_corp.duckdb"
        ),
        dataset_name="raw"
    )

    # Load the data / Run the pipeline
    data = generate_sample_data()
    info = pipeline.run(data, table_name="transfer_listings", write_disposition="replace")
    print(f"Load info: {info}")

if __name__ == "__main__":
    main() 