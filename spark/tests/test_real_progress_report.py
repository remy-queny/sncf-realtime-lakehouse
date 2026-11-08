import json

import pytest

from inspect_real_progress import build_report, read_events


def test_reader_ignores_noise_and_incomplete_lines(tmp_path):
    path = tmp_path / "ingestion.log"
    path.write_text(
        'Message Java\n'
        '{"stage": "collection_started", "timestamp": "2026-11-08T17:00:00Z"}\n'
        '{"stage":\n',
        encoding="utf-8",
    )

    assert read_events(path) == [{
        "stage": "collection_started",
        "timestamp": "2026-11-08T17:00:00Z",
    }]


def test_missing_log_has_no_events(tmp_path):
    assert read_events(tmp_path / "missing.log") == []


def test_completed_ingestion_is_inactive_but_gold_remains_monitored():
    ingestion = [
        {
            "stage": "collection_completed",
            "timestamp": "2026-11-08T17:00:00Z",
            "new_snapshot_count": 0,
        },
        {"status": "session_completed"},
    ]
    gold = [{
        "stage": "gold_check_completed",
        "timestamp": "2026-11-08T17:59:30Z",
        "publication_performed": False,
    }]

    report = build_report(
        ingestion, gold, now="2026-11-08T18:00:00Z"
    )

    assert report["ingestion_completed"] is True
    assert report["last_collection_observation"] == "no_new_snapshot"
    assert report["stages"]["collection"]["status"] == "inactive"
    assert report["stages"]["kafka"]["status"] == "inactive"
    assert report["stages"]["gold"]["status"] == "waiting"
    assert report["session_state"] == "not_confirmed_stopped"


@pytest.mark.parametrize(
    "exit_code,expected_state",
    [(0, "stopped"), (1, "failed")],
)
def test_stopped_session_preserves_exit_status(exit_code, expected_state):
    report = build_report(
        [], [],
        now="2026-11-08T18:00:00Z",
        exit_code=exit_code,
    )

    assert report["session_state"] == expected_state
    assert all(
        stage["status"] == "inactive"
        for stage in report["stages"].values()
    )


def test_missing_signals_do_not_imply_healthy_processes():
    report = build_report(
        [], [], now="2026-11-08T18:00:00Z"
    )

    assert all(
        stage["status"] == "unknown"
        for stage in report["stages"].values()
    )
    assert report["last_collection_observation"] == "unknown"