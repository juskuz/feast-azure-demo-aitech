from datetime import timedelta

from utils.prepare_feast import create_fs

from feast import Entity, FeatureView, ValueType, Feature
from feast_azure_provider.mssqlserver_source import MsSqlServerSource


def main():
    fs = create_fs()

    payments_source = MsSqlServerSource(
        table_ref="payments",
        event_timestamp_column="ts",
    )

    stats_source = MsSqlServerSource(
        table_ref="stats",
        event_timestamp_column="ts",
    )

    player_id = Entity(name="player_id", value_type=ValueType.STRING, description="player id")

    payments_view = FeatureView(
        name="payments",
        entities=["player_id"],
        ttl=timedelta(hours=1),
        features=[
            Feature(name="amount", dtype=ValueType.FLOAT),
            Feature(name="transactions", dtype=ValueType.INT32),
        ],
        batch_source=payments_source,
    )

    stat_view = FeatureView(
        name="stats",
        entities=["player_id"],
        ttl=timedelta(hours=1),
        features=[
            Feature(name="win_loss_ratio", dtype=ValueType.FLOAT),
            Feature(name="games_played", dtype=ValueType.INT32),
            Feature(name="time_in_game", dtype=ValueType.FLOAT),
        ],
        batch_source=stats_source,
    )

    fs.apply([player_id, payments_view, stat_view], partial=False)


if __name__ == "__main__":
    main()
