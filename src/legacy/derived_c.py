"""Legacy module for derived table C using Pandas transformation approach."""

from typing import Any

import duckdb
import pandas as pd


def fetch_source_data() -> tuple[pd.DataFrame, pd.DataFrame]:
    """Fetch data from derived tables A and B.

    This function demonstrates the dependency on other derived tables
    by fetching data from derived_a and derived_b.

    Returns:
        Tuple of DataFrames (top_players_df, high_value_transfers_df)
    """
    # Connect to the database
    conn = duckdb.connect("database/legacy/transferroom.duckdb")

    # Check if the derived tables exist
    tables = conn.execute("""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'prod' 
        AND table_name IN ('derived_a', 'derived_b')
    """).fetchall()

    existing_tables = [t[0] for t in tables if t is not None]

    if "derived_a" not in existing_tables or "derived_b" not in existing_tables:
        raise ValueError(
            "Derived tables A and B must be created before running derived table C. "
            "Please run derived_a.py and derived_b.py first."
        )

    # Fetch data from derived table A
    top_players_df = conn.execute("""
        SELECT * FROM prod.derived_a
    """).fetchdf()

    # Fetch data from derived table B
    high_value_transfers_df = conn.execute("""
        SELECT * FROM prod.derived_b
    """).fetchdf()

    # Close the connection
    conn.close()

    return top_players_df, high_value_transfers_df


def transform_data(
    top_players_df: pd.DataFrame, high_value_transfers_df: pd.DataFrame
) -> pd.DataFrame:
    """Transform data using Pandas operations.

    This function demonstrates complex Pandas transformations that would be
    difficult to express in SQL alone.

    Args:
        top_players_df: DataFrame of top players by value
        high_value_transfers_df: DataFrame of high-value transfers

    Returns:
        Transformed DataFrame for derived table C
    """
    print("Performing Pandas transformations...")

    # 1. Calculate average transfer fees by position
    avg_fees_by_position = (
        high_value_transfers_df.groupby("position")["transfer_fee_millions"]
        .agg(avg_fee="mean", max_fee="max", min_fee="min", count="count")
        .reset_index()
    )

    # 2. Calculate average market value by position
    avg_value_by_position = (
        top_players_df.groupby("position")["market_value_millions"]
        .agg(avg_value="mean", max_value="max", min_value="min", count="count")
        .reset_index()
    )

    # 3. Merge the position statistics
    position_stats = pd.merge(
        avg_fees_by_position, avg_value_by_position, on="position", how="outer"
    )

    # 4. Calculate value-to-fee ratio
    position_stats["value_to_fee_ratio"] = (
        position_stats["avg_value"] / position_stats["avg_fee"]
    )

    # 5. Calculate league statistics from transfers
    league_stats = (
        high_value_transfers_df.groupby("buying_league")
        .agg(
            total_spent=pd.NamedAgg(column="transfer_fee_millions", aggfunc="sum"),
            avg_spent=pd.NamedAgg(column="transfer_fee_millions", aggfunc="mean"),
            num_transfers=pd.NamedAgg(column="transfer_id", aggfunc="count"),
        )
        .reset_index()
    )

    # 6. Calculate net spend by league (buying - selling)
    selling_by_league = pd.DataFrame(
        {
            "league": high_value_transfers_df["selling_league"],
            "total_income": high_value_transfers_df["transfer_fee_millions"],
        }
    )
    selling_by_league = (
        selling_by_league.groupby("league")["total_income"].sum().reset_index()
    )

    # Merge buying and selling stats
    league_stats = pd.merge(
        league_stats.rename(columns={"buying_league": "league"}),
        selling_by_league,
        on="league",
        how="outer",
    ).fillna(0)

    # Calculate net spend
    league_stats["net_spend"] = (
        league_stats["total_spent"] - league_stats["total_income"]
    )

    # 7. Calculate player age statistics for transfers
    age_stats = pd.merge(
        high_value_transfers_df,
        top_players_df[["player_id", "age"]],
        on="player_id",
        how="left",
    )

    age_bins = [15, 20, 25, 30, 35, 40]
    age_labels = ["Under 20", "20-24", "25-29", "30-34", "35+"]

    age_stats["age_group"] = pd.cut(
        age_stats["age"], bins=age_bins, labels=age_labels, right=False
    )

    age_group_stats = (
        age_stats.groupby("age_group", observed=True)
        .agg(
            avg_fee=pd.NamedAgg(column="transfer_fee_millions", aggfunc="mean"),
            total_spent=pd.NamedAgg(column="transfer_fee_millions", aggfunc="sum"),
            count=pd.NamedAgg(column="transfer_id", aggfunc="count"),
        )
        .reset_index()
    )

    # 8. Create a combined insights DataFrame
    # This is our final output that combines all the analyses

    # First, create a unique ID for each row
    position_stats["insight_id"] = "POS_" + position_stats["position"]
    position_stats["insight_type"] = "Position Analysis"
    position_stats["insight_name"] = position_stats["position"] + " Position Insights"

    league_stats["insight_id"] = "LEAGUE_" + league_stats["league"]
    league_stats["insight_type"] = "League Analysis"
    league_stats["insight_name"] = league_stats["league"] + " League Insights"

    # Convert the categorical column to string before concatenation
    age_group_stats["age_group_str"] = age_group_stats["age_group"].astype(str)
    age_group_stats["insight_id"] = "AGE_" + age_group_stats["age_group_str"]
    age_group_stats["insight_type"] = "Age Analysis"
    age_group_stats["insight_name"] = (
        age_group_stats["age_group_str"] + " Age Group Insights"
    )

    # Convert each DataFrame to a format suitable for combining
    # We'll use a dictionary representation for the metrics

    def prepare_position_row(row: pd.Series) -> dict[str, Any]:
        return {
            "insight_id": row["insight_id"],
            "insight_type": row["insight_type"],
            "insight_name": row["insight_name"],
            "primary_dimension": "position",
            "dimension_value": row["position"],
            "metrics": {
                "avg_transfer_fee": row["avg_fee"],
                "max_transfer_fee": row["max_fee"],
                "min_transfer_fee": row["min_fee"],
                "transfer_count": row["count_x"],
                "avg_market_value": row["avg_value"],
                "max_market_value": row["max_value"],
                "min_market_value": row["min_value"],
                "player_count": row["count_y"],
                "value_to_fee_ratio": row["value_to_fee_ratio"],
            },
        }

    def prepare_league_row(row: pd.Series) -> dict[str, Any]:
        return {
            "insight_id": row["insight_id"],
            "insight_type": row["insight_type"],
            "insight_name": row["insight_name"],
            "primary_dimension": "league",
            "dimension_value": row["league"],
            "metrics": {
                "total_spent": row["total_spent"],
                "avg_spent": row["avg_spent"],
                "num_transfers": row["num_transfers"],
                "total_income": row["total_income"],
                "net_spend": row["net_spend"],
            },
        }

    def prepare_age_row(row: pd.Series) -> dict[str, Any]:
        return {
            "insight_id": row["insight_id"],
            "insight_type": row["insight_type"],
            "insight_name": row["insight_name"],
            "primary_dimension": "age_group",
            "dimension_value": row["age_group_str"],
            "metrics": {
                "avg_transfer_fee": row["avg_fee"],
                "total_spent": row["total_spent"],
                "transfer_count": row["count"],
            },
        }

    # Apply the conversion functions
    position_insights = position_stats.apply(prepare_position_row, axis=1).tolist()
    league_insights = league_stats.apply(prepare_league_row, axis=1).tolist()
    age_insights = age_group_stats.apply(prepare_age_row, axis=1).tolist()

    # Combine all insights
    all_insights = position_insights + league_insights + age_insights

    # Convert to DataFrame
    insights_df = pd.DataFrame(all_insights)

    # Convert metrics dictionary to string for storage
    insights_df["metrics_json"] = insights_df["metrics"].apply(lambda x: str(x))

    # Add timestamp
    insights_df["created_at"] = pd.Timestamp.now()

    return insights_df


def create_derived_table_c() -> None:
    """Create derived table C using Pandas transformation approach.

    This function fetches data from derived tables A and B,
    performs complex transformations using Pandas, and stores
    the result as derived table C.
    """
    try:
        # Fetch data from derived tables A and B
        top_players_df, high_value_transfers_df = fetch_source_data()

        # Transform the data using Pandas
        insights_df = transform_data(top_players_df, high_value_transfers_df)

        # Connect to the database
        conn = duckdb.connect("database/legacy/transferroom.duckdb")

        # Create the derived table
        print("Creating derived table C (transfer_market_insights)...")

        # Create the table from the DataFrame
        conn.execute("CREATE SCHEMA IF NOT EXISTS prod")
        conn.execute("DROP TABLE IF EXISTS prod.derived_c")
        conn.register("temp_insights", insights_df)
        conn.execute("CREATE TABLE prod.derived_c AS SELECT * FROM temp_insights")

        # Print the number of rows
        result = conn.execute("SELECT COUNT(*) FROM prod.derived_c").fetchone()
        if result is not None:
            print(f"Created derived table C with {result[0]} rows")
        else:
            print("Created derived table C but couldn't get row count")

        # Close the connection
        conn.close()

    except Exception as e:
        print(f"Error creating derived table C: {e}")
        raise


if __name__ == "__main__":
    create_derived_table_c()
