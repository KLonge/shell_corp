import datetime as dt  # type: ignore
from typing import Literal

import patito as pt  # type: ignore
import polars as pl

from src.loader.constants import (
    CONTRACT_MAX_DAYS,
    CONTRACT_MIN_DAYS,
    MARKET_VALUE_MAX,
    MARKET_VALUE_MIN,
)


class PlayerSchema(pt.Model):
    """Schema definition for transformed player data.

    This schema enforces data quality rules and documents the expected
    structure of our player dataset. Includes validation for:
    - Player IDs following PLY{8 digits} format
    - Age range between 15-100 years
    - Market value within configured min/max bounds
    - Contract end dates between today and 5 years in future
    - Transfer status as either 'available' or 'unavailable'
    """

    id: str = pt.Field(pattern=r"^PLY[0-9]{8}$")  # Format: PLY{8 digits}
    current_club: str
    player_name: str
    position: str
    age: int = pt.Field(ge=15, le=100)
    nation: str
    market_value_euro: int = pt.Field(ge=MARKET_VALUE_MIN, le=MARKET_VALUE_MAX)
    contract_end_date: dt.date = pt.Field(
        constraints=pl.col("contract_end_date").is_between(
            dt.date.today() + dt.timedelta(days=CONTRACT_MIN_DAYS),
            dt.date.today() + dt.timedelta(days=CONTRACT_MAX_DAYS),
        )
    )
    transfer_status: Literal["available", "unavailable"]

    class Config:
        """Configuration for schema validation."""

        str_strip_whitespace = True
        str_to_lower = False
