import pytest

from jobs.freshness import calculate_batch_delays
from jobs.freshness import measure_batch_freshness


def test_calculates_source_and_collection_delays():
    result = calculate_batch_delays(
        source_published_at="2026-11-08T16:00:00Z",
        collected_at="2026-11-08T16:00:20+00:00",
        postgres_published_at="2026-11-08T16:01:05+00:00",
    )

    assert result == {
        "source_to_postgres_seconds": 65.0,
        "collection_to_postgres_seconds": 45.0,
    }


@pytest.mark.parametrize(
    "field",
    [
        "source_published_at",
        "collected_at",
        "postgres_published_at",
    ],
)
def test_rejects_timestamps_without_timezone(field):
    timestamps = {
        "source_published_at": "2026-11-08T16:00:00Z",
        "collected_at": "2026-11-08T16:00:20Z",
        "postgres_published_at": "2026-11-08T16:01:05Z",
    }
    timestamps[field] = "2026-11-08T16:00:00"

    with pytest.raises(ValueError, match="UTC"):
        calculate_batch_delays(**timestamps)


@pytest.mark.parametrize(
    "source,collected,postgres",
    [
        (
            "2026-11-08T16:00:30Z",
            "2026-11-08T16:00:20Z",
            "2026-11-08T16:01:05Z",
        ),
        (
            "2026-11-08T16:00:00Z",
            "2026-11-08T16:01:20Z",
            "2026-11-08T16:01:05Z",
        ),
    ],
)
def test_rejects_inconsistent_chronology(source, collected, postgres):
    with pytest.raises(ValueError, match="Chronologie"):
        calculate_batch_delays(
            source_published_at=source,
            collected_at=collected,
            postgres_published_at=postgres,
        )

def batch_measurement(present_event_ids, expected_event_ids=None):
    return measure_batch_freshness(
        batch_id="batch-test",
        expected_event_ids=(
            ["event-1", "event-2"]
            if expected_event_ids is None
            else expected_event_ids
        ),
        present_event_ids=present_event_ids,
        source_published_at="2026-11-08T16:00:00Z",
        collected_at="2026-11-08T16:00:20Z",
        postgres_published_at="2026-11-08T16:01:05Z",
    )


def test_complete_batch_has_delays():
    result = batch_measurement({"event-1", "event-2", "other-event"})

    assert result["status"] == "complete"
    assert result["expected_event_count"] == 2
    assert result["matched_event_count"] == 2
    assert result["source_to_postgres_seconds"] == 65.0
    assert result["collection_to_postgres_seconds"] == 45.0


@pytest.mark.parametrize(
    "present,matched_count",
    [
        (set(), 0),
        ({"event-1"}, 1),
        ({"unrelated-event"}, 0),
    ],
)
def test_incomplete_batch_has_no_delay(present, matched_count):
    result = batch_measurement(present)

    assert result["status"] == "incomplete"
    assert result["matched_event_count"] == matched_count
    assert result["source_to_postgres_seconds"] is None
    assert result["collection_to_postgres_seconds"] is None


def test_empty_batch_is_rejected():
    with pytest.raises(ValueError, match="vide"):
        batch_measurement(set(), expected_event_ids=[])


def test_duplicate_expected_ids_are_rejected():
    with pytest.raises(ValueError, match="uniques"):
        batch_measurement(
            {"event-1"},
            expected_event_ids=["event-1", "event-1"],
        )