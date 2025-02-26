from pathlib import Path
from typing import Any, Literal, overload

import duckdb
import pandas as pd
from duckdb import DuckDBPyConnection


class DuckDBClient:
    """
    A configurable resource for working with DuckDB databases.

    This class provides methods for querying DuckDB databases and working with
    both local and S3-stored data.

    Attributes:
        database_path (str | Path): Path to the database file. Use ":memory:" for in-memory database.
        s3_access (bool): Whether AWS credentials have been configured for S3 access.
    """

    def __init__(self, database_path: str | Path | None = None) -> None:
        self._connection = (
            duckdb.connect(database=str(database_path))
            if database_path != ":memory:"
            else duckdb.connect()
        )

    def get_connection(
        self, new_connection: bool = False, database_path: str | Path | None = None
    ) -> DuckDBPyConnection:
        """
        Get or create a connection to DuckDB database.

        Args:
            new_connection: If True, create a new connection even if one exists.
            database_path: Path to the database file. Use ":memory:" for in-memory database.
        Returns:
            DuckDBPyConnection: Database connection object.
        """
        if new_connection or self._connection is None:
            self._connection = (
                duckdb.connect(database=str(database_path))
                if database_path != ":memory:"
                else duckdb.connect()
            )

            # Set case insensitivity at connection time
            self._connection.execute("SET preserve_identifier_case = false;")

        return self._connection

    @overload
    def query(
        self, query: str, *, database_path: str | Path | None = None
    ) -> pd.DataFrame: ...

    @overload
    def query(
        self,
        query: str,
        return_df: Literal[True],
        *,
        database_path: str | Path | None = None,
    ) -> pd.DataFrame: ...

    @overload
    def query(
        self,
        query: str,
        return_df: Literal[False],
        *,
        database_path: str | Path | None = None,
    ) -> duckdb.DuckDBPyRelation: ...

    def query(
        self,
        query: str,
        return_df: bool = True,
        new_connection: bool = False,
        database_path: str | Path | None = None,
    ) -> pd.DataFrame | duckdb.DuckDBPyRelation:
        """
        Execute a SQL query and optionally return the result as a DataFrame.

        Args:
            query: The SQL query to execute
            return_df: If True, return results as DataFrame. If False, return connection.
            new_connection: If True, create a new connection for this query.

        Returns:
            Either a pandas DataFrame with the query results or the DuckDB connection
        """
        conn = self.get_connection(
            new_connection=new_connection, database_path=database_path
        )
        result = conn.sql(query)

        if return_df:
            return result.df()
        return result

    def table_exists(self, schema: str, table: str) -> bool:
        """
        Check if a table exists in the database.

        Args:
            schema: The schema name
            table: The table name

        Returns:
            bool: True if the table exists, False otherwise
        """
        query = f"""
        SELECT COUNT(*) AS count 
        FROM information_schema.tables
        WHERE table_schema = '{schema}'
        AND table_name = '{table}'
        """
        result = self.query(query)
        return bool(result.iloc[0, 0])

    def __enter__(self) -> "DuckDBClient":
        """Enable context manager support."""
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Cleanup resources when exiting context manager."""
        self.close()

    def close(self) -> None:
        """Close the database connection."""
        if self._connection is not None:
            self._connection.close()
