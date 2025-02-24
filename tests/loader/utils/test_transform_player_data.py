import patito as pt  # type: ignore
import polars as pl
import pytest

from src.loader.constants import MARKET_VALUE_MAX, MARKET_VALUE_MIN
from src.loader.utils import transform_player_data


def test_given_raw_player_data_when_transforming_then_returns_expected_format() -> None:
    # Given
    input_data = {
        "league": ["ENG-Premier League", "ENG-Premier League"],
        "season": ["2425", "2425"],
        "team": ["Arsenal", "Arsenal"],
        "player": ["Ben White", "Bukayo Saka"],
        "nation": ["ENG", "ENG"],
        "pos": ["DF", "FW,MF"],
        "age": ["27-137", "23-170"],
        "born": [1997, 2001],
        "Playing Time_MP": [9, 16],
        "Playing Time_Starts": [7, 16],
        "Playing Time_Min": [695, 1268],
        "Playing Time_90s": [7.7, 14.1],
        "Performance_Gls": [0, 5],
        "Performance_Ast": [1, 10],
        "Performance_G+A": [1, 15],
        "Performance_G-PK": [0, 4],
        "Performance_PK": [0, 1],
        "Performance_PKatt": [0, 1],
        "Performance_CrdY": [2, 3],
        "Performance_CrdR": [0, 0],
        "Expected_xG": [0.1, 4.2],
        "Expected_npxG": [0.1, 3.4],
        "Expected_xAG": [0.4, 5.8],
        "Expected_npxG+xAG": [0.6, 9.2],
        "Progression_PrgC": [4, 72],
        "Progression_PrgP": [30, 53],
        "Progression_PrgR": [15, 195],
        "Per 90 Minutes_Gls": [0.0, 0.35],
        "Per 90 Minutes_Ast": [0.13, 0.71],
        "Per 90 Minutes_G+A": [0.13, 1.06],
        "Per 90 Minutes_G-PK": [0.0, 0.28],
        "Per 90 Minutes_G+A-PK": [0.13, 0.99],
        "Per 90 Minutes_xG": [0.02, 0.3],
        "Per 90 Minutes_xAG": [0.06, 0.41],
        "Per 90 Minutes_xG+xAG": [0.07, 0.71],
        "Per 90 Minutes_npxG": [0.02, 0.24],
        "Per 90 Minutes_npxG+xAG": [0.07, 0.65],
    }
    input_df = pl.DataFrame(input_data)

    # When
    result = transform_player_data(input_df)

    # Then
    assert result.shape[0] == 2
    assert all(
        col in result.columns
        for col in [
            "current_club",
            "player_name",
            "position",
            "age",
            "nation",
            "id",
            "market_value_euro",
            "contract_end_date",
            "transfer_status",
        ]
    )
    assert result.get_column("age").to_list() == [27, 23]
    assert result.get_column("position").to_list() == ["DF", "FW,MF"]
    assert result.get_column("nation").to_list() == ["ENG", "ENG"]


def test_given_invalid_age_when_transforming_then_raises_validation_error() -> None:
    # Arrange
    input_df = pl.DataFrame(
        {
            "team": ["Manchester United"],
            "player": ["Marcus Rashford"],
            "pos": ["FW"],
            "age": ["120-123"],  # Age too high (over 100)
            "nation": ["ENG"],
        }
    )

    # Act & Assert
    with pytest.raises(pt.DataFrameValidationError):
        data = transform_player_data(input_df)
        print(data)


def test_given_valid_data_when_transforming_then_returns_valid_market_values() -> None:
    """Test that market values are within the expected range."""
    # Given
    input_df = pl.DataFrame(
        {
            "team": ["Manchester United"],
            "player": ["Marcus Rashford"],
            "pos": ["FW"],
            "age": ["25-123"],
            "nation": ["ENG"],
        }
    )

    # When
    result = transform_player_data(input_df)

    # Then
    market_values = result.get_column("market_value_euro")
    assert all(value >= MARKET_VALUE_MIN for value in market_values), (
        "Market value below minimum"
    )
    assert all(value <= MARKET_VALUE_MAX for value in market_values), (
        "Market value above maximum"
    )


if __name__ == "__main__":
    pytest.main([__file__])
