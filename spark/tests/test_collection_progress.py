import json
from unittest.mock import Mock

import pytest

from run_active_real_collection import collect_with_progress


@pytest.mark.parametrize("new_snapshot", [True, False])
def test_successful_collection_reports_progress(tmp_path, new_snapshot):
    root = tmp_path / "collection"
    registry = root / "registry"
    registry.mkdir(parents=True)
    (registry / "existing.json").write_text("{}", encoding="utf-8")

    messages = []

    def collect(source, collection):
        assert source == "https://example.invalid/feed"
        assert collection == root

        if new_snapshot:
            (registry / "new.json").write_text(
                "{}", encoding="utf-8"
            )

    collect_with_progress(
        "https://example.invalid/feed",
        root,
        collect,
        messages.append,
    )

    events = [json.loads(message) for message in messages]

    assert [event["stage"] for event in events] == [
        "collection_started",
        "collection_completed",
    ]
    assert all(event["timestamp"].endswith("+00:00") for event in events)

    completed = events[1]
    assert completed["duration_seconds"] >= 0
    assert completed["new_snapshot_count"] == int(new_snapshot)
    assert completed["registered_snapshot_count"] == (
        2 if new_snapshot else 1
    )


def test_failed_collection_does_not_report_success(tmp_path):
    messages = []
    collect = Mock(side_effect=RuntimeError("Échec HTTP"))

    with pytest.raises(RuntimeError, match="Échec HTTP"):
        collect_with_progress(
            "https://example.invalid/feed",
            tmp_path,
            collect,
            messages.append,
        )

    events = [json.loads(message) for message in messages]
    assert [event["stage"] for event in events] == [
        "collection_started"
    ]