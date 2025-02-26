import os
import random
from collections.abc import Iterator
from datetime import datetime, timedelta
from itertools import islice
from typing import Any

import dlt


@dlt.resource(
    name="app_a",
    write_disposition="replace",
)
def generate_app_a_data(
    num_records: int = 100, chunk_size: int = 20
) -> Iterator[list[dict[str, Any]]]:
    """Generate dummy data for players table.

    Creates records with player information including name, position, age,
    nationality, and current club to simulate a database of football players.

    Args:
        num_records: Total number of records to generate
        chunk_size: Number of records per chunk when yielding data

    Yields:
        Chunks of player data dictionaries
    """
    try:
        print(f"Generating {num_records} records for players...")

        positions = ["Goalkeeper", "Defender", "Midfielder", "Forward"]
        nationalities = [
            "England",
            "Spain",
            "France",
            "Germany",
            "Italy",
            "Brazil",
            "Argentina",
            "Portugal",
            "Netherlands",
            "Belgium",
        ]
        clubs = [
            "Manchester United",
            "Liverpool",
            "Chelsea",
            "Arsenal",
            "Barcelona",
            "Real Madrid",
            "Bayern Munich",
            "PSG",
            "Juventus",
            "AC Milan",
            "Inter Milan",
            "Borussia Dortmund",
            "Ajax",
            "Porto",
            "Benfica",
            "Atletico Madrid",
        ]

        all_records: list[dict[str, Any]] = []
        for i in range(1, num_records + 1):
            age = random.randint(17, 38)
            position = random.choice(positions)

            # Adjust market value based on age and position
            base_value = random.uniform(0.5, 30.0)
            if 23 <= age <= 29:  # Prime age
                base_value *= 1.5
            elif age > 32:  # Older players
                base_value *= 0.7

            if position == "Forward":  # Forwards typically more expensive
                base_value *= 1.3

            market_value = round(base_value, 2)

            record = {
                "player_id": f"P{i:04d}",
                "name": f"Player {i}",
                "position": position,
                "age": age,
                "nationality": random.choice(nationalities),
                "current_club": random.choice(clubs),
                "market_value_millions": market_value,
                "contract_end_date": (
                    datetime.now() + timedelta(days=random.randint(30, 1825))
                ).strftime("%Y-%m-%d"),  # Contract length between 1 month and 5 years
            }
            all_records.append(record)

        records_iter: Iterator[dict[str, Any]] = iter(all_records)

        records_processed = 0
        while True:
            chunk: list[dict[str, Any]] = list(islice(records_iter, chunk_size))
            if not chunk:
                break

            records_processed += len(chunk)
            print(
                f"Yielding chunk of {len(chunk)} records for players. Progress: {records_processed}/{num_records}"
            )
            yield chunk

        print("Finished processing all player records")

    except Exception as e:
        raise Exception(f"Error generating player data: {e}")


@dlt.resource(
    name="app_b",
    write_disposition="replace",
)
def generate_app_b_data(
    num_records: int = 100, chunk_size: int = 20
) -> Iterator[list[dict[str, Any]]]:
    """Generate dummy data for clubs table.

    Creates records with club information including name, league, stadium,
    budget, and performance metrics to simulate a database of football clubs.

    Args:
        num_records: Total number of records to generate
        chunk_size: Number of records per chunk when yielding data

    Yields:
        Chunks of club data dictionaries
    """
    try:
        print(f"Generating {num_records} records for clubs...")

        leagues = {
            "Premier League": "England",
            "La Liga": "Spain",
            "Bundesliga": "Germany",
            "Serie A": "Italy",
            "Ligue 1": "France",
            "Eredivisie": "Netherlands",
            "Primeira Liga": "Portugal",
            "Championship": "England",
            "MLS": "United States",
            "Liga MX": "Mexico",
        }
        league_names = list(leagues.keys())

        all_records: list[dict[str, Any]] = []
        for i in range(1, num_records + 1):
            league = random.choice(league_names)
            country = leagues[league]

            # Adjust transfer budget based on league
            if league in [
                "Premier League",
                "La Liga",
                "Bundesliga",
                "Serie A",
                "Ligue 1",
            ]:
                budget = round(random.uniform(20, 200), 2)
            else:
                budget = round(random.uniform(2, 50), 2)

            record = {
                "club_id": f"C{i:03d}",
                "name": f"Club {i}",
                "league": league,
                "country": country,
                "stadium": f"Stadium {i}",
                "capacity": random.randint(15000, 90000),
                "transfer_budget_millions": budget,
                "league_position": random.randint(1, 20),
                "european_competition": random.choice(
                    ["Champions League", "Europa League", "Conference League", "None"]
                ),
                "last_updated": datetime.now().isoformat(),
            }
            all_records.append(record)

        records_iter: Iterator[dict[str, Any]] = iter(all_records)

        records_processed = 0
        while True:
            chunk: list[dict[str, Any]] = list(islice(records_iter, chunk_size))
            if not chunk:
                break

            records_processed += len(chunk)
            print(
                f"Yielding chunk of {len(chunk)} records for clubs. Progress: {records_processed}/{num_records}"
            )
            yield chunk

        print("Finished processing all club records")

    except Exception as e:
        raise Exception(f"Error generating club data: {e}")


@dlt.resource(
    name="app_c",
    write_disposition="replace",
)
def generate_app_c_data(
    num_records: int = 100, chunk_size: int = 20
) -> Iterator[list[dict[str, Any]]]:
    """Generate dummy data for transfers table.

    Creates records with transfer information including player, selling club,
    buying club, transfer fee, and contract details to simulate a database
    of football transfers.

    Args:
        num_records: Total number of records to generate
        chunk_size: Number of records per chunk when yielding data

    Yields:
        Chunks of transfer data dictionaries
    """
    try:
        print(f"Generating {num_records} records for transfers...")

        transfer_types = [
            "Permanent",
            "Loan",
            "Free Transfer",
            "Loan with Option to Buy",
        ]
        transfer_windows = ["Summer", "Winter"]

        # Generate random years for past transfers (last 5 years)
        current_year = datetime.now().year
        years = list(range(current_year - 5, current_year + 1))

        all_records: list[dict[str, Any]] = []
        for i in range(1, num_records + 1):
            # Random player and club IDs (referencing the other tables)
            player_id = f"P{random.randint(1, 100):04d}"
            selling_club_id = f"C{random.randint(1, 100):03d}"

            # Ensure buying club is different from selling club
            buying_club_id = selling_club_id
            while buying_club_id == selling_club_id:
                buying_club_id = f"C{random.randint(1, 100):03d}"

            transfer_type = random.choice(transfer_types)

            # Set transfer fee based on transfer type
            if transfer_type == "Free Transfer":
                transfer_fee = 0.0
            elif transfer_type == "Loan":
                transfer_fee = round(random.uniform(0, 5), 2)  # Loan fee
            else:
                transfer_fee = round(random.uniform(1, 100), 2)

            # Random transfer date
            year = random.choice(years)
            window = random.choice(transfer_windows)

            if window == "Summer":
                transfer_date = datetime(
                    year, random.randint(6, 8), random.randint(1, 28)
                )
            else:
                transfer_date = datetime(
                    year, random.randint(1, 2), random.randint(1, 28)
                )

            record = {
                "transfer_id": f"T{i:05d}",
                "player_id": player_id,
                "selling_club_id": selling_club_id,
                "buying_club_id": buying_club_id,
                "transfer_fee_millions": transfer_fee,
                "transfer_type": transfer_type,
                "transfer_window": window,
                "transfer_date": transfer_date.strftime("%Y-%m-%d"),
                "contract_length_years": random.randint(1, 5)
                if transfer_type != "Loan"
                else random.choice([0.5, 1]),
                "salary_thousands_weekly": round(random.uniform(5, 500), 2),
                "status": random.choice(["Completed", "Pending", "Failed", "Rumor"]),
            }
            all_records.append(record)

        records_iter: Iterator[dict[str, Any]] = iter(all_records)

        records_processed = 0
        while True:
            chunk: list[dict[str, Any]] = list(islice(records_iter, chunk_size))
            if not chunk:
                break

            records_processed += len(chunk)
            print(
                f"Yielding chunk of {len(chunk)} records for transfers. Progress: {records_processed}/{num_records}"
            )
            yield chunk

        print("Finished processing all transfer records")

    except Exception as e:
        raise Exception(f"Error generating transfer data: {e}")


def main() -> None:
    """Main function to load transferroom data into DuckDB tables."""
    # Ensure database directory exists
    os.makedirs("database/legacy", exist_ok=True)
    os.makedirs("database/new", exist_ok=True)

    print("Starting TransferRoom legacy data pipeline...")
    # Create pipeline that loads to database/legacy/transferroom.duckdb
    legacy_transferroom_pipeline: dlt.Pipeline = dlt.pipeline(
        pipeline_name="transferroom",
        destination=dlt.destinations.duckdb("database/legacy/transferroom.duckdb"),
        dataset_name="prod",  # going to just presume that the legacy schema is just one big schema called prod where both raw and derived tables are stored
    )

    # Run the pipeline with all three generator resources
    info = legacy_transferroom_pipeline.run(
        [
            generate_app_a_data(num_records=100),
            generate_app_b_data(num_records=100),
            generate_app_c_data(num_records=100),
        ]
    )
    print(f"Legacy TransferRoom pipeline completed. Load info: {info}")

    print("Starting new TransferRoom data pipeline...")
    # Create pipeline that loads to database/new/transferroom.duckdb
    new_transferroom_pipeline: dlt.Pipeline = dlt.pipeline(
        pipeline_name="transferroom",
        destination=dlt.destinations.duckdb("database/new/transferroom.duckdb"),
        dataset_name="raw",  # calling this raw as it will be the raw schema in the new database (and it's how we will do the testing between the two)
    )

    # Run the pipeline with all three generator resources
    info = new_transferroom_pipeline.run(
        [
            generate_app_a_data(num_records=100),
            generate_app_b_data(num_records=100),
            generate_app_c_data(num_records=100),
        ]
    )
    print(f"New TransferRoom pipeline completed. Load info: {info}")


if __name__ == "__main__":
    main()
