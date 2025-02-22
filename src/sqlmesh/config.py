from sqlmesh import Config
from sqlmesh.core.config import ModelDefaultsConfig, GatewayConfig, DuckDBConnectionConfig

config = Config(
    model_defaults=ModelDefaultsConfig(
        dialect="duckdb"
    ),
    gateways={
        "local": GatewayConfig(
            connection=DuckDBConnectionConfig(
                database="database/shell_corp.duckdb"
            )
        )
    },
    default_gateway="local"
) 