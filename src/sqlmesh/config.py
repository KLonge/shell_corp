from sqlmesh import Config
from sqlmesh.core.config import (
    DuckDBConnectionConfig,
    GatewayConfig,
    ModelDefaultsConfig,
)

config = Config(
    model_defaults=ModelDefaultsConfig(dialect="duckdb"),
    gateways={
        "local": GatewayConfig(
            connection=DuckDBConnectionConfig(database="database/shell_corp.duckdb")
        )
    },
    default_gateway="local",
)
