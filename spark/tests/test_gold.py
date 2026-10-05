import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import date_format, to_date, to_timestamp

from transforms.gold import (
    aggregate_delay_by_line,
    aggregate_station_delay_daily,
)


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder
        .appName("test-gold")
        .master("local[2]")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )

    try:
        yield session
    finally:
        session.stop()


@pytest.fixture
def silver(spark):
    events = [
        ("2026-10-05 12:00:00", "TRAIN-J-001", "J", 0.0),
        ("2026-10-05 12:05:00", "TRAIN-J-002", "J", 5.0),
        ("2026-10-05 12:10:00", "TRAIN-J-003", "J", 6.0),
        ("2026-10-05 12:11:00", "TRAIN-J-003", "J", 12.0),
        ("2026-10-05 12:15:00", "TRAIN-J-004", "J", 3.0),
        ("2026-10-05 12:05:00", "TRAIN-H-001", "H", 10.0),
        ("2026-10-06 12:00:00", "TRAIN-J-005", "J", 4.0),
    ]

    rows = [
        (
            timestamp,
            trip_id,
            route_id,
            f"Ligne {route_id} (simulation)",
            f"STOP-{route_id}",
            f"Arrêt simulé {route_id}",
            delay,
        )
        for timestamp, trip_id, route_id, delay in events
    ]

    return (
        spark.createDataFrame(
            rows,
            schema=(
                "event_timestamp string, trip_id string, "
                "route_id string, line_name string, "
                "stop_id string, stop_name string, "
                "delay_minutes double"
            ),
        )
        .withColumn(
            "event_timestamp",
            to_timestamp("event_timestamp"),
        )
        .withColumn(
            "event_date",
            to_date("event_timestamp"),
        )
    )


def test_gold_delay_by_line(silver):
    rows = (
        aggregate_delay_by_line(silver)
        .withColumn(
            "start_text",
            date_format("window_start", "yyyy-MM-dd HH:mm:ss"),
        )
        .withColumn(
            "end_text",
            date_format("window_end", "yyyy-MM-dd HH:mm:ss"),
        )
        .collect()
    )

    actual = {
        (row["start_text"], row["route_id"]): row
        for row in rows
    }

    # Fin de fenêtre, événements, trains en retard, moyenne, maximum.
    expected = {
        ("2026-10-05 12:00:00", "J"): (
            "2026-10-05 12:15:00", 4, 1, 5.75, 12.0
        ),
        ("2026-10-05 12:15:00", "J"): (
            "2026-10-05 12:30:00", 1, 0, 3.0, 3.0
        ),
        ("2026-10-05 12:00:00", "H"): (
            "2026-10-05 12:15:00", 1, 1, 10.0, 10.0
        ),
        ("2026-10-06 12:00:00", "J"): (
            "2026-10-06 12:15:00", 1, 0, 4.0, 4.0
        ),
    }

    assert len(rows) == len(expected)
    assert actual.keys() == expected.keys()

    for key, values in expected.items():
        end, event_count, delayed_count, average, maximum = values
        row = actual[key]

        assert row["line_name"] == f"Ligne {key[1]} (simulation)"
        assert row["end_text"] == end
        assert row["event_count"] == event_count
        assert row["delayed_trip_count"] == delayed_count
        assert row["average_delay_minutes"] == pytest.approx(average)
        assert row["max_delay_minutes"] == pytest.approx(maximum)


def test_gold_station_delay_daily(silver):
    rows = aggregate_station_delay_daily(silver).collect()

    actual = {
        (row["event_date"].isoformat(), row["stop_id"]): row
        for row in rows
    }

    # Événements, événements en retard, moyenne, maximum.
    expected = {
        ("2026-10-05", "STOP-J"): (5, 2, 5.2, 12.0),
        ("2026-10-05", "STOP-H"): (1, 1, 10.0, 10.0),
        ("2026-10-06", "STOP-J"): (1, 0, 4.0, 4.0),
    }

    assert len(rows) == len(expected)
    assert actual.keys() == expected.keys()

    for key, values in expected.items():
        event_count, delayed_count, average, maximum = values
        row = actual[key]

        route_id = key[1].removeprefix("STOP-")
        assert row["stop_name"] == f"Arrêt simulé {route_id}"
        assert row["event_count"] == event_count
        assert row["delayed_event_count"] == delayed_count
        assert row["average_delay_minutes"] == pytest.approx(average)
        assert row["max_delay_minutes"] == pytest.approx(maximum)