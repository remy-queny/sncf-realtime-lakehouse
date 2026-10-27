from datetime import date, datetime
from types import SimpleNamespace
from unittest.mock import Mock, patch

import pytest

from jobs import publish_postgres as publisher


def test_publication_rejects_target_before_spark_or_database_access():
    lines = Mock()
    stations = Mock()

    with patch.object(publisher.psycopg, "connect") as connect:
        with pytest.raises(ValueError):
            publisher.publish_gold_dataframes(
                lines=lines,
                stations=stations,
                lakehouse_root=publisher.PROJECT_ROOT / "data/lakehouse_real",
                target_schema="sncf_active_test",
            )

    lines.select.assert_not_called()
    connect.assert_not_called()


def test_publication_reuses_upserts_for_both_tables(monkeypatch):
    for name, value in (
        ("POSTGRES_DB", "test_db"),
        ("POSTGRES_USER", "test_user"),
        ("POSTGRES_PASSWORD", "test_password"),
    ):
        monkeypatch.setenv(name, value)

    line = SimpleNamespace(
        window_start_utc="2026-10-27 17:00:00",
        window_end_utc="2026-10-27 17:15:00",
        route_id="J", line_name="Ligne J simulation",
        event_count=3, delayed_trip_count=1,
        average_delay_minutes=4.0, max_delay_minutes=12.0,
    )
    station = SimpleNamespace(
        event_date=date(2026, 10, 27), stop_id="STOP-J",
        stop_name="Arrêt simulé J", delayed_event_count=1,
        average_delay_minutes=4.0, max_delay_minutes=12.0,
        event_count=3,
    )
    lines = Mock()
    stations = Mock()

    with (
        patch.object(publisher, "date_format") as date_format,
        patch.object(publisher, "collect_small_table", side_effect=[[line], [station]]) as collect,
        patch.object(publisher.psycopg, "connect") as connect,
    ):
        result = publisher.publish_gold_dataframes(
            lines=lines,
            stations=stations,
            lakehouse_root=publisher.PROJECT_ROOT / "data/validation/active-stream-unit/lakehouse",
            target_schema="sncf_active_test",
        )

    assert result == (1, 1)
    assert date_format.call_count == 2
    assert collect.call_count == 2
    cursor = connect.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value
    calls = cursor.executemany.call_args_list
    assert len(calls) == 2
    assert calls[0].args[1] == [(
        datetime(2026, 10, 27, 17, 0),
        datetime(2026, 10, 27, 17, 15),
        "J", "Ligne J simulation", 3, 1, 4.0, 12.0,
    )]
    assert calls[1].args[1] == [(
        date(2026, 10, 27), "STOP-J", "Arrêt simulé J", 1, 4.0, 12.0, 3,
    )]
    line_query = calls[0].args[0].as_string()
    station_query = calls[1].args[0].as_string()
    assert '"sncf_active_test"."gold_delay_by_line_15min"' in line_query
    assert '"sncf_active_test"."gold_station_delay_daily"' in station_query
    assert "ON CONFLICT" in line_query
    assert "ON CONFLICT" in station_query
