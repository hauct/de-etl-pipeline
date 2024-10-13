from dagster import IOManager, InputContext, OutputContext
from contextlib import contextmanager
from sqlalchemy import create_engine
import polars as pl


def connect_mysql(config) -> str:
    if not all(key in config for key in ["user", "password", "host", "port", "database"]):
        raise ValueError("MySQL configuration is incomplete")
    
    conn_info = (
        f"mysql://{config['user']}:{config['password']}"
        + f"@{config['host']}:{config['port']}"
        + f"/{config['database']}"
    )
    return conn_info

class MySQLIOManager(IOManager):
    def __init__(self, config):
        self._config = config

    def handle_output(self, context: "OutputContext", obj: pl.DataFrame):
        # Implement logic to handle the output, e.g., save to a file or database
        pass

    def load_input(self, context: "InputContext"):
        pass

    def extract_data(self, sql: str) -> pl.DataFrame:
        conn_info = connect_mysql(self._config)
        df_data = pl.read_database_uri(query=sql, uri=conn_info)
        return df_data