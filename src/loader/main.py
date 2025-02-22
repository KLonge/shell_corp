import dlt
import os

def generate_sample_data() -> list[dict]:
    """Generate sample football transfer listing data."""
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