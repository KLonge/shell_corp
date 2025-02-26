import os

from sqlmesh import Config
from sqlmesh.core.config import (
    DuckDBConnectionConfig,
    GatewayConfig,
    ModelDefaultsConfig,
)

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
DB_PATH = os.path.join(PROJECT_ROOT, "database/new/transferroom.duckdb")

config = Config(
    model_defaults=ModelDefaultsConfig(dialect="duckdb"),
    gateways={
        "local": GatewayConfig(connection=DuckDBConnectionConfig(database=DB_PATH))
    },
    default_gateway="local",
)
