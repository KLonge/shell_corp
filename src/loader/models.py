import patito as pt  # type: ignore


class PlayerSchema(pt.Model):
    """Schema definition for transformed player data.

    This schema enforces data quality rules and documents the expected
    structure of our player dataset.
    """

    id: str = pt.Field(pattern=r"^PLY[0-9]{8}$")  # Format: PLY{8 digits}
    current_club: str
    player_name: str
    position: str
    age: int = pt.Field(ge=15, le=45)  # Age constraints
    nation: str
    market_value_euro: int = pt.Field(
        ge=1_000_000, le=100_000_000
    )  # Market value constraints
    contract_end_date: str  # ISO format date string
    transfer_status: str  # Currently always "available"

    class Config:
        """Configuration for schema validation."""

        str_strip_whitespace = True
        str_to_lower = False
