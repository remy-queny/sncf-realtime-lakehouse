import pytest

from progress_health import assess_stage


def assess(events, **overrides):
    options = {
        "started_stage": "collection_started",
        "completed_stage": "collection_completed",
        "now": "2026-11-08T18:00:00Z",
        "timeout_seconds": 120,
        "expected_active": True,
    }
    options.update(overrides)
    return assess_stage(events, **options)


@pytest.mark.parametrize(
    "stage,timestamp,expected",
    [
        ("collection_started", "2026-11-08T17:59:00Z", "running"),
        ("collection_started", "2026-11-08T17:58:00Z", "running"),
        ("collection_started", "2026-11-08T17:57:59Z", "overdue"),
        ("collection_completed", "2026-11-08T17:59:00Z", "waiting"),
        ("collection_completed", "2026-11-08T17:58:00Z", "waiting"),
        ("collection_completed", "2026-11-08T17:57:59Z", "stale"),
    ],
)
def test_stage_status(stage, timestamp, expected):
    result = assess([{"stage": stage, "timestamp": timestamp}])

    assert result["status"] == expected


def test_missing_events_are_unknown():
    result = assess([])

    assert result["status"] == "unknown"
    assert result["age_seconds"] is None


def test_inactive_stage_does_not_raise_staleness_alert():
    result = assess(
        [{
            "stage": "collection_completed",
            "timestamp": "2026-11-08T17:00:00Z",
        }],
        expected_active=False,
    )

    assert result["status"] == "inactive"


def test_events_are_ordered_by_timestamp():
    result = assess([
        {
            "stage": "collection_completed",
            "timestamp": "2026-11-08T17:59:30Z",
        },
        {
            "stage": "collection_started",
            "timestamp": "2026-11-08T17:59:00Z",
        },
    ])

    assert result["status"] == "waiting"
    assert result["age_seconds"] == 30


def test_unrelated_events_do_not_count_as_progress():
    result = assess([{
        "stage": "kafka_publication_completed",
        "timestamp": "2026-11-08T17:59:30Z",
    }])

    assert result["status"] == "unknown"


@pytest.mark.parametrize(
    "threshold",
    [0, -1, True, float("nan"), float("inf")],
)
def test_invalid_threshold_is_rejected(threshold):
    with pytest.raises(ValueError, match="seuil"):
        assess([], timeout_seconds=threshold)


def test_future_event_is_rejected():
    with pytest.raises(ValueError, match="futur"):
        assess([{
            "stage": "collection_started",
            "timestamp": "2026-11-08T18:01:00Z",
        }])


def test_timestamp_without_timezone_is_rejected():
    with pytest.raises(ValueError, match="fuseau"):
        assess([], now="2026-11-08T18:00:00")