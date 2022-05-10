import logging
import textwrap
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.sql import text
from sqlalchemy.engine import Connection
from azureml.core import Workspace, Datastore


CONFIG_PATH = "/content/feast-azure-demo-aitech/config.json" # change if needed
DATA_DIR = "../data"


def create_payments(con: Connection):
    query = """\
    CREATE TABLE dbo.payments (
        event_id INT,
        player_id NVARCHAR(5),
        ts DATETIME2,
        amount FLOAT,
        transactions INT
    )"""
    query = textwrap.dedent(query)
    logging.info(f"Creating payments table, executing query... \n{query}")
    con.execute(query)
    logging.info(f"Query executed")


def create_stats(con: Connection):
    query = """\
    CREATE TABLE dbo.stats (
        event_id INT,
        player_id NVARCHAR(5),
        ts DATETIME2,
        win_loss_ratio FLOAT,
        games_played INT,
        time_in_game FLOAT
    )"""
    query = textwrap.dedent(query)
    logging.info(f"Creating stats table, executing query... \n{query}")
    con.execute(query)
    logging.info(f"Query executed")


def load_data(con: Connection, dls_url_with_name: str, table_name: str):
    query = f"""\
    COPY INTO dbo.{table_name}
    FROM '{dls_url_with_name}/{table_name}.csv'
    WITH
    (
        FILE_TYPE = 'CSV'
        ,MAXERRORS = 0
        ,FIRSTROW = 2
    )"""
    query = textwrap.dedent(query)
    logging.info(f"Loading csv data {table_name} table, executing query... \n{query}")
    con.execute(query)
    logging.info(f"Query executed")


def main():
    logging.basicConfig(level=logging.INFO)
    ws = Workspace.from_config(path=CONFIG_PATH)
    kv = ws.get_default_keyvault()

    dfs_url = kv.get_secret("FEAST-OFFLINE-STORE-DFS-URL")
    dfs_name = kv.get_secret("FEAST-OFFLINE-STORE-DFS-NAME")
    dls_url_with_name = f"{dfs_url}{dfs_name}"
    
    engine = create_engine(kv.get_secret("FEAST-OFFLINE-STORE-CONN"))

    with engine.connect() as con:
        create_payments(con)
        create_stats(con)

        load_data(con, dls_url_with_name, "payments")
        load_data(con, dls_url_with_name, "stats")  

if __name__ == "__main__":
    main()
