import pandas as pd
import pytest

from src.utils.pandas import flatten_pd_dataframe


def test_given_multiindex_dataframe_when_flattening_then_returns_flat_structure() -> (
    None
):
    # Given
    columns = pd.MultiIndex.from_tuples(
        [
            ("nation", ""),
            ("pos", ""),
            ("age", ""),
            ("Playing Time", "MP"),
            ("Performance", "Gls"),
        ]
    )
    index = pd.MultiIndex.from_tuples(
        [("ENG-Premier League", "2425", "Arsenal", "Ben White")],
        names=["league", "season", "team", "player"],
    )

    data = [["ENG", "DF", "27-137", "9", "0"]]
    df = pd.DataFrame(data, columns=columns, index=index)

    # When
    result = flatten_pd_dataframe(df=df)

    # Then
    expected_columns = [
        "league",
        "season",
        "team",
        "player",
        "nation",
        "pos",
        "age",
        "Playing Time_MP",
        "Performance_Gls",
    ]
    assert all(col in result.columns for col in expected_columns)
    assert result.shape[0] == 1
    assert result.loc[0, "player"] == "Ben White"


if __name__ == "__main__":
    pytest.main([__file__])
