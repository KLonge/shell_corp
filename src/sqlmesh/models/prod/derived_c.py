"""SQLMesh Python model for derived table C using Pandas transformation approach."""

from typing import Any

import pandas as pd

from sqlmesh.core.model import PythonModel, model


@model(
    name="prod.derived_c",
    kind="full",
    dialect="duckdb",
    description="Transfer market insights - migrated from legacy derived table C",
    owner="data_team",
    depends_on=[
        "prod.derived_a",
        "prod.derived_b",
    ],
    columns={
        "insight_id": "VARCHAR",
        "insight_type": "VARCHAR",
        "insight_name": "VARCHAR",
        "primary_dimension": "VARCHAR",
        "dimension_value": "VARCHAR",
        "metrics_json": "VARCHAR",
        "created_at": "TIMESTAMP",
    },
)
class DerivedTableC(PythonModel):  # type: ignore
    """SQLMesh Python model for derived table C with pandas transformations.

    This model uses pandas to perform complex transformations on data from
    derived tables A and B to generate market insights.
    """

    def transform(self, context: dict[str, Any]) -> pd.DataFrame:
        """Transform data using Pandas operations.

        This function performs complex Pandas transformations that would be
        difficult to express in SQL alone.

        Args:
            context: The execution context containing input DataFrames

        Returns:
            Transformed DataFrame for derived table C
        """
        # Get input DataFrames from context
        top_players_df = context["prod.derived_a"]
        high_value_transfers_df = context["prod.derived_b"]

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
                total_spent=("transfer_fee_millions", "sum"),
                avg_spent=("transfer_fee_millions", "mean"),
                num_transfers=("transfer_id", "count"),
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
                avg_fee=("transfer_fee_millions", "mean"),
                total_spent=("transfer_fee_millions", "sum"),
                count=("transfer_id", "count"),
            )
            .reset_index()
        )

        # 8. Create a combined insights DataFrame
        # First, create a unique ID for each row
        position_stats["insight_id"] = "POS_" + position_stats["position"]
        position_stats["insight_type"] = "Position Analysis"
        position_stats["insight_name"] = (
            position_stats["position"] + " Position Insights"
        )
        position_stats["primary_dimension"] = "position"
        position_stats["dimension_value"] = position_stats["position"]

        league_stats["insight_id"] = "LEAGUE_" + league_stats["league"]
        league_stats["insight_type"] = "League Analysis"
        league_stats["insight_name"] = league_stats["league"] + " League Insights"
        league_stats["primary_dimension"] = "league"
        league_stats["dimension_value"] = league_stats["league"]

        # Convert the categorical column to string before concatenation
        age_group_stats["age_group_str"] = age_group_stats["age_group"].astype(str)
        age_group_stats["insight_id"] = "AGE_" + age_group_stats["age_group_str"]
        age_group_stats["insight_type"] = "Age Analysis"
        age_group_stats["insight_name"] = (
            age_group_stats["age_group_str"] + " Age Group Insights"
        )
        age_group_stats["primary_dimension"] = "age_group"
        age_group_stats["dimension_value"] = age_group_stats["age_group_str"]

        # Create metrics JSON for each insight type
        position_stats["metrics_json"] = position_stats.apply(
            lambda row: str(
                {
                    "avg_transfer_fee": row["avg_fee"],
                    "max_transfer_fee": row["max_fee"],
                    "min_transfer_fee": row["min_fee"],
                    "transfer_count": row["count_x"],
                    "avg_market_value": row["avg_value"],
                    "max_market_value": row["max_value"],
                    "min_market_value": row["min_value"],
                    "player_count": row["count_y"],
                    "value_to_fee_ratio": row["value_to_fee_ratio"],
                }
            ),
            axis=1,
        )

        league_stats["metrics_json"] = league_stats.apply(
            lambda row: str(
                {
                    "total_spent": row["total_spent"],
                    "avg_spent": row["avg_spent"],
                    "num_transfers": row["num_transfers"],
                    "total_income": row["total_income"],
                    "net_spend": row["net_spend"],
                }
            ),
            axis=1,
        )

        age_group_stats["metrics_json"] = age_group_stats.apply(
            lambda row: str(
                {
                    "avg_transfer_fee": row["avg_fee"],
                    "total_spent": row["total_spent"],
                    "transfer_count": row["count"],
                }
            ),
            axis=1,
        )

        # Select and rename columns for position insights
        position_insights = position_stats[
            [
                "insight_id",
                "insight_type",
                "insight_name",
                "primary_dimension",
                "dimension_value",
                "metrics_json",
            ]
        ].copy()

        # Select and rename columns for league insights
        league_insights = league_stats[
            [
                "insight_id",
                "insight_type",
                "insight_name",
                "primary_dimension",
                "dimension_value",
                "metrics_json",
            ]
        ].copy()

        # Select and rename columns for age insights
        age_insights = age_group_stats[
            [
                "insight_id",
                "insight_type",
                "insight_name",
                "primary_dimension",
                "dimension_value",
                "metrics_json",
            ]
        ].copy()

        # Combine all insights
        insights_df = pd.concat(
            [position_insights, league_insights, age_insights], ignore_index=True
        )

        # Add timestamp
        insights_df["created_at"] = pd.Timestamp.now()

        return insights_df
