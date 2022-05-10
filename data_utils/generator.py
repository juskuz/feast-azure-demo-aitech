from datetime import date, datetime, timedelta
import string
from typing import Dict, Optional, List
from pathlib import Path
import logging
import shutil


import numpy as np
import pandas as pd
from pandas.core.frame import DataFrame


ALPHABET = np.array(list(string.ascii_uppercase + string.digits))
PLAYER_ID_LEN = 3
PAYMENTS_SAMPLE_FRAC = 0.1
STATS_SAMPLE_FRAC = 0.75

def set_seed(seed: int) -> None:
    np.random.seed(seed)


def generate_player_ids(users_count: int) -> List[str]:
    """
    Generates list of randomly created player ids based on user_counts.
    """
    # 
    if users_count > len(ALPHABET) ** PLAYER_ID_LEN:
        raise ValueError("Cannot create more players than unique player ids ")
    
    ids = set()
    while len(ids) < users_count:
        id_ = "".join(np.random.choice(ALPHABET, PLAYER_ID_LEN))
        ids.add(id_)
    return sorted(list(ids)) 


def generate_stats(players_ts: DataFrame) -> DataFrame:
    """
    Generates stats table (win_loss_ratio, games_played, time_in_game columns).
    Output table containes only sampled part of events from players_ts.  
    """
    stats = players_ts.sample(frac=STATS_SAMPLE_FRAC).sort_index()
    stats["win_loss_ratio"] = np.random.uniform(0, 1, size=len(stats))
    stats["games_played"] = (np.round(np.random.pareto(2, size=len(stats)) * 100) + 1).astype(np.int32)
    stats["time_in_game"] = np.random.uniform(1, 3600, size=len(stats))
    logging.info(f"{len(stats)} stats rows generated")
    return stats


def generate_payments(players_ts: DataFrame) -> DataFrame:
    """
    Generates stats table (amount, transactions columns).
    Output table containes only sampled part of events from players_ts.
    Only first player from players_ts contains random data for each timestep. 
    """
    first_id = players_ts["player_id"].iloc[0]
    payments = pd.concat([
        players_ts[players_ts["player_id"] == first_id], 
        players_ts[players_ts["player_id"] != first_id].sample(frac=PAYMENTS_SAMPLE_FRAC).sort_index()
    ])
    payments["amount"] = np.round(np.random.uniform(10, 1000, size=len(payments)), 2)
    payments["transactions"] = np.round(np.random.uniform(1, 10, size=len(payments)), 0).astype(np.int32)
    logging.info(f"{len(payments)} payments rows generated")
    return payments

def generate_players_ts(start_date: date, end_date: date, users_count: int) -> DataFrame:
    """
    Generates time series for each player based on start_date and end_date.
    Timestep is one hour.
    """
    logging.info(f"Generating synthetic data from {start_date} to {end_date} for {users_count} players...")
    player_ids = generate_player_ids(users_count)
    ts = pd.date_range(start_date, end_date, freq="1H").to_series(name="ts")
    return pd.DataFrame({"player_id": player_ids}).merge(ts, how="cross")


def main():
    set_seed(0)
    logging.basicConfig(level=logging.INFO)
    
    start_date = datetime.today().date() - timedelta(days=7)
    end_date = datetime.today().date() + timedelta(days=7)
    base_dir = Path("../data/")
    users_count = 1000

    players_ts = generate_players_ts(start_date, end_date, users_count)
    stats = generate_stats(players_ts)
    payments = generate_payments(players_ts)

    shutil.rmtree(base_dir.resolve(), True)
    base_dir.mkdir()

    logging.info(f"Saving data to {base_dir.resolve()}")
    stats.to_csv((base_dir / "stats.csv").resolve(), index_label="event_id")
    payments.to_csv((base_dir / "payments.csv").resolve(), index_label="event_id")

if __name__ == "__main__":
    main()
