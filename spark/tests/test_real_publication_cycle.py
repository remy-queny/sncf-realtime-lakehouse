import sys
from pathlib import Path
from unittest.mock import Mock

import pytest



ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts"))

import real_publication_cycle as cycle


def batch(identifier, status, published_at="2026-10-29T12:00:00Z"):
    return {
        "batch_id": identifier,
        "status": status,
        "published_at": published_at,
    }


@pytest.fixture
def setup(monkeypatch):
    prepare = Mock(return_value={"selected_events": 200})
    inspect = Mock()
    publish = Mock(
        return_value={
            "published_now": True,
            "event_count": 100,
            "status": "published",
        }
    )

    monkeypatch.setattr(cycle, "prepare_publications", prepare)
    monkeypatch.setattr(cycle, "inspect_batches", inspect)
    monkeypatch.setattr(cycle, "publish_one", publish)

    return prepare, inspect, publish


def test_publishes_pending_and_skips_published(setup, tmp_path):
    prepare, inspect, publish = setup
    inspect.return_value = {
        "batches": [
            batch("done", "published"),
            batch("new", "pending"),
        ]
    }

    result = cycle.run_publication_cycle(tmp_path, ROOT, limit=100)

    prepare.assert_called_once_with(
        tmp_path.resolve(),
        limit=100,
        bootstrap_servers="localhost:9092",
    )
    publish.assert_called_once_with(tmp_path.resolve(), "new", ROOT)
    assert result["published_batches"] == 1
    assert result["published_events"] == 100
    assert result["skipped_published_batches"] == 1


@pytest.mark.parametrize("status", ["publishing", "uncertain"])
def test_uncertain_state_blocks_all_sends(setup, tmp_path, status):
    _, inspect, publish = setup
    inspect.return_value = {
        "batches": [
            batch("new", "pending"),
            batch("blocked", status),
        ]
    }

    with pytest.raises(cycle.UncertainPublication, match="Diagnostic"):
        cycle.run_publication_cycle(tmp_path, ROOT)

    publish.assert_not_called()


def test_publishes_in_source_time_order(setup, tmp_path):
    _, inspect, publish = setup
    inspect.return_value = {
        "batches": [
            batch("later", "pending", "2026-10-29T12:02:00Z"),
            batch("earlier", "pending", "2026-10-29T12:00:00Z"),
        ]
    }

    cycle.run_publication_cycle(tmp_path, ROOT)

    assert [
        call.args[1] for call in publish.call_args_list
    ] == ["earlier", "later"]


def test_stops_after_publication_failure(setup, tmp_path):
    _, inspect, publish = setup
    inspect.return_value = {
        "batches": [
            batch("first", "pending"),
            batch("second", "pending"),
        ]
    }
    publish.side_effect = RuntimeError("Échec de publication")

    with pytest.raises(RuntimeError, match="Échec de publication"):
        cycle.run_publication_cycle(tmp_path, ROOT)

    assert publish.call_count == 1


def test_all_published_means_no_send(setup, tmp_path):
    _, inspect, publish = setup
    inspect.return_value = {
        "batches": [batch("done", "published")]
    }

    result = cycle.run_publication_cycle(tmp_path, ROOT)

    publish.assert_not_called()
    assert result["published_batches"] == 0
    assert result["published_events"] == 0
    assert result["skipped_published_batches"] == 1