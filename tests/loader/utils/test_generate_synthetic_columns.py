import datetime as dt
from typing import Any

import polars as pl
import pytest

from src.loader.utils import generate_synthetic_columns


@pytest.fixture
def sample_df() -> pl.DataFrame:
    """Create a sample DataFrame for testing."""
    return pl.DataFrame(
        {
            "team": ["Arsenal", "Chelsea"],
            "player": ["Player 1", "Player 2"],
            "pos": ["FW", "MF"],
            "age": ["25-100", "27-200"],
            "nation": ["ENG", "BRA"],
        }
    )


def test_given_sample_df_when_generating_columns_then_returns_correct_expressions() -> (
    None
):
    """Test that generate_synthetic_columns returns the expected expressions."""
    # Given
    df = pl.DataFrame({"dummy": range(3)})  # Simple df with 3 rows

    # When
    expressions = generate_synthetic_columns(df)

    # Then
    assert len(expressions) == 4  # Should have 4 synthetic columns

    # Apply expressions to get actual column names
    result = df.with_columns(expressions)
    assert result.columns == [
        "dummy",  # Original column
        "id",
        "market_value_euro",
        "contract_end_date",
        "transfer_status",
    ]


def test_given_df_when_applying_expressions_then_generates_valid_data() -> None:
    """Test that the generated expressions produce valid data when applied."""
    # Given
    df = pl.DataFrame({"dummy": range(3)})
    market_min = 2_000_000
    market_max = 50_000_000
    min_days = 60
    max_days = 180

    # When
    expressions = generate_synthetic_columns(
        df,
        market_value_min=market_min,
        market_value_max=market_max,
        contract_min_days=min_days,
        contract_max_days=max_days,
    )
    result = df.with_columns(expressions)

    # Then
    assert result.shape[0] == 3  # Should maintain row count

    # Check ID format
    ids = result.get_column("id").to_list()
    assert all(isinstance(id_, str) for id_ in ids)
    assert all(id_.startswith("PLY") for id_ in ids)
    assert all(len(id_) == 11 for id_ in ids)  # PLY + 8 digits

    # Check market values are within specified range
    market_values = result.get_column("market_value_euro").to_list()
    assert all(isinstance(mv, int) for mv in market_values)
    assert all(market_min <= mv <= market_max for mv in market_values)

    # Check contract end dates are within specified range
    dates = result.get_column("contract_end_date").to_list()
    min_allowed = dt.datetime.now().date() + dt.timedelta(days=min_days)
    max_allowed = dt.datetime.now().date() + dt.timedelta(days=max_days)

    for date_str in dates:
        date = dt.date.fromisoformat(date_str)
        assert min_allowed <= date <= max_allowed

    # Check transfer status
    statuses = result.get_column("transfer_status").to_list()
    assert all(status == "available" for status in statuses)


def test_given_empty_df_when_generating_columns_then_handles_gracefully() -> None:
    """Test that the function handles empty DataFrames appropriately."""
    # Given
    df = pl.DataFrame({"dummy": []})

    # When
    expressions = generate_synthetic_columns(df)
    result = df.with_columns(expressions)

    # Then
    assert result.shape[0] == 0
    assert all(
        col in result.columns
        for col in ["id", "market_value_euro", "contract_end_date", "transfer_status"]
    )


if __name__ == "__main__":
    pytest.main([__file__])
