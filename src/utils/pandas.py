import pandas as pd


def flatten_pd_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Flatten a pandas DataFrame with multi-index columns and hierarchical index.

    Takes a DataFrame with multi-index columns and hierarchical index and converts
    it to a flat DataFrame with simple column names and no index. Column names are
    joined with underscore, empty second levels are dropped.

    Args:
        df: DataFrame with multi-index columns and/or hierarchical index

    Returns:
        Flattened DataFrame with standardized column names
    """
    # Create a copy to avoid modifying the input
    flat_df = df.copy()

    # Flatten multi-index columns using pandas built-in method
    flat_df.columns = flat_df.columns.to_flat_index().map(
        lambda x: f"{x[0]}_{x[1]}" if x[1] else x[0]
    )

    # Reset the hierarchical index to columns
    return flat_df.reset_index() 