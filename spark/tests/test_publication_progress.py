import json
from unittest.mock import Mock

import pytest

import real_publication_cycle as cycle


@pytest.mark.parametrize("published_events", [0, 100])
def test_publication_reports_actual_progress(
    tmp_path, monkeypatch, published_events
):
    result = {
        "published_batches": int(published_events > 0),
        "published_events": published_events,
        "skipped_published_batches": int(published_events == 0),
    }
    publish = Mock(return_value=result)
    monkeypatch.setattr(cycle, "run_publication_cycle", publish)

    messages = []
    returned = cycle.publish_with_progress(
        tmp_path,
        tmp_path,
        limit=100,
        bootstrap_servers="localhost:9092",
        reporter=messages.append,
    )

    assert returned is result
    publish.assert_called_once_with(
        tmp_path,
        tmp_path,
        limit=100,
        bootstrap_servers="localhost:9092",
    )

    events = [json.loads(message) for message in messages]
    assert [event["stage"] for event in events] == [
        "kafka_publication_started",
        "kafka_publication_completed",
    ]
    assert all(event["timestamp"].endswith("+00:00") for event in events)
    assert events[1]["duration_seconds"] >= 0
    assert events[1]["published_events"] == published_events
    assert events[1]["published_batches"] == result["published_batches"]
    assert events[1]["skipped_published_batches"] == (
        result["skipped_published_batches"]
    )


def test_failed_publication_does_not_report_success(
    tmp_path, monkeypatch
):
    monkeypatch.setattr(
        cycle,
        "run_publication_cycle",
        Mock(side_effect=cycle.UncertainPublication("Diagnostic requis")),
    )
    messages = []

    with pytest.raises(cycle.UncertainPublication, match="Diagnostic"):
        cycle.publish_with_progress(
            tmp_path,
            tmp_path,
            reporter=messages.append,
        )

    events = [json.loads(message) for message in messages]
    assert [event["stage"] for event in events] == [
        "kafka_publication_started"
    ]