from datetime import datetime, timedelta
import textwrap

from utils.prepare_feast import create_fs


def main():
    fs = create_fs()
    PLAYER_ID1 = "01C"
    PLAYER_ID2 = "EKS"

    query = f"""\
    SELECT DISTINCT * FROM
    (     
        SELECT player_id, ts FROM stats WHERE player_id in ('{PLAYER_ID1}', '{PLAYER_ID2}') 
        UNION SELECT player_id, ts FROM payments WHERE player_id in ('{PLAYER_ID1}', '{PLAYER_ID2}') 
    ) res    
    """
    query = textwrap.dedent(query)

    print()
    print("----------------------HIST DATA FRAME----------------------")
    print(query)
    
    training_df = fs.get_historical_features(
        entity_df=query,
        features=[
            "payments:amount",
            "payments:transactions",
            "stats:win_loss_ratio",
            "stats:games_played",
            "stats:time_in_game",
        ]
    ).to_df()

    print()
    print("----------------------OFFLINE DATA FRAME PLAYER1----------------------")
    print(training_df[training_df["player_id"] == PLAYER_ID1])

    print()
    print("----------------------OFFLINE DATA FRAME PLAYER2----------------------")
    print(training_df[training_df["player_id"] == PLAYER_ID2])

    entity_rows = [{"player_id": PLAYER_ID1}, {"player_id": PLAYER_ID2}]
    online_df = fs.get_online_features(
        features=[
            "payments:amount",
            "payments:transactions",
            "stats:win_loss_ratio",
            "stats:games_played",
            "stats:time_in_game",
        ],
        entity_rows=entity_rows
    ).to_df()

    print()
    print("----------------------ONLINE DATA FRAME----------------------")
    print(online_df[["player_id", "amount", "transactions", "win_loss_ratio", "games_played", "time_in_game"]])


if __name__ == "__main__":
    main()
