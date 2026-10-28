import importlib.util
import json
from pathlib import Path
from unittest.mock import Mock

import pytest

ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location(
    "publication_state", ROOT / "scripts/publication_state.py"
)
publication = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(publication)


@pytest.fixture
def setup(tmp_path):
    path = tmp_path / "events.jsonl"
    events = [
        {
            "event_id": f"event-{index}", "source": "sncf_gtfs_rt",
            "event_type": "trip_update", "schema_version": 1, "trip_id": "trip-one",
        }
        for index in range(3)
    ]
    path.write_text("".join(json.dumps(event) + "\n" for event in events), encoding="utf-8")
    metadata = {
        "snapshot_sha256": "a" * 64,
        "jsonl_path": str(path.resolve()),
        "jsonl_sha256": publication.sha256(path),
        "event_count": 2,
        "selection": "first_n",
        "topic": publication.REAL_TOPIC,
        "bootstrap_servers": "localhost:9092",
    }
    return publication.PublicationRegistry(tmp_path / "publication"), metadata


def receipt(metadata):
    return {
        "topic": metadata["topic"],
        "bootstrap_servers": metadata["bootstrap_servers"],
        "acknowledgements": [
            {"event_id": f"event-{index}", "topic": metadata["topic"], "partition": 0, "offset": index + 10}
            for index in range(metadata["event_count"])
        ],
    }


def test_prepare_creates_pending_batch_without_sending(setup):
    registry, metadata = setup
    state = registry.prepare(metadata)
    assert state["status"] == "pending"
    assert state["attempts"] == 0
    assert "receipt" not in state


def test_prepare_again_does_not_reset_or_rewrite_state(setup):
    registry, metadata = setup
    state = registry.prepare(metadata)
    path = registry.path(state["batch_id"])
    before = path.read_bytes()
    assert registry.prepare(metadata) == state
    assert path.read_bytes() == before


def test_cannot_change_selection_for_same_snapshot_and_target(setup):
    registry, metadata = setup
    registry.prepare(metadata)
    changed = dict(metadata, event_count=3)
    with pytest.raises(ValueError, match="sélection ou provenance différente"):
        registry.prepare(changed)


def test_publishes_only_after_complete_acknowledgements(setup):
    registry, metadata = setup
    batch = registry.prepare(metadata)
    def sender(meta):
        assert registry.get(batch["batch_id"])["status"] == "publishing"
        return receipt(meta)
    result = registry.publish(batch["batch_id"], sender)
    assert result["published_now"] is True
    assert result["state"]["status"] == "published"
    assert result["state"]["attempts"] == 1


def test_published_batch_is_not_sent_again_after_registry_recreation(setup):
    registry, metadata = setup
    batch = registry.prepare(metadata)
    registry.publish(batch["batch_id"], receipt)
    restarted = publication.PublicationRegistry(registry.directory)
    sender = Mock()
    result = restarted.publish(batch["batch_id"], sender)
    assert result["published_now"] is False
    sender.assert_not_called()


def test_sender_failure_is_uncertain_and_not_retried(setup):
    registry, metadata = setup
    batch = registry.prepare(metadata)
    sender = Mock(side_effect=RuntimeError("Échec après un envoi possible"))
    with pytest.raises(RuntimeError):
        registry.publish(batch["batch_id"], sender)
    assert registry.get(batch["batch_id"])["status"] == "uncertain"
    with pytest.raises(publication.UncertainPublication):
        registry.publish(batch["batch_id"], sender)
    assert sender.call_count == 1


def test_process_loss_in_publishing_state_blocks_blind_replay(setup):
    registry, metadata = setup
    batch = registry.prepare(metadata)
    batch["status"] = "publishing"
    batch["attempts"] = 1
    publication.atomic_json(registry.path(batch["batch_id"]), batch)
    sender = Mock()
    with pytest.raises(publication.UncertainPublication):
        registry.publish(batch["batch_id"], sender)
    sender.assert_not_called()


def test_incomplete_receipt_does_not_mark_batch_published(setup):
    registry, metadata = setup
    batch = registry.prepare(metadata)
    def incomplete(meta):
        result = receipt(meta)
        result["acknowledgements"].pop()
        return result
    with pytest.raises(ValueError, match="incomplets"):
        registry.publish(batch["batch_id"], incomplete)
    assert registry.get(batch["batch_id"])["status"] == "uncertain"


def test_wrong_offset_positions_are_rejected(setup):
    registry, metadata = setup
    batch = registry.prepare(metadata)
    def wrong(meta):
        result = receipt(meta)
        result["acknowledgements"][1]["offset"] = 10
        return result
    with pytest.raises(ValueError, match="Position Kafka dupliquée"):
        registry.publish(batch["batch_id"], wrong)
    assert registry.get(batch["batch_id"])["status"] == "uncertain"


def test_modified_jsonl_is_rejected_before_sender_call(setup):
    registry, metadata = setup
    batch = registry.prepare(metadata)
    Path(metadata["jsonl_path"]).write_text("modified", encoding="utf-8")
    sender = Mock()
    with pytest.raises(ValueError, match="JSONL absent ou modifié"):
        registry.publish(batch["batch_id"], sender)
    sender.assert_not_called()


def test_simulation_topic_is_rejected(setup):
    registry, metadata = setup
    with pytest.raises(ValueError, match="topic réel"):
        registry.prepare(dict(metadata, topic="sncf.tripupdates.raw"))


def test_interrupt_does_not_leave_batch_pending(setup):
    registry, metadata = setup
    batch = registry.prepare(metadata)
    def interrupted(meta):
        raise KeyboardInterrupt()
    with pytest.raises(KeyboardInterrupt):
        registry.publish(batch["batch_id"], interrupted)
    assert registry.get(batch["batch_id"])["status"] == "uncertain"


def test_prepare_existing_uncertain_batch_does_not_reset_it(setup):
    registry, metadata = setup
    batch = registry.prepare(metadata)
    with pytest.raises(RuntimeError):
        registry.publish(batch["batch_id"], Mock(side_effect=RuntimeError("Failed")))
    assert registry.prepare(metadata)["status"] == "uncertain"


def test_empty_or_excessive_batch_is_rejected(setup):
    registry, metadata = setup
    for count in (0, -1, 20001, True):
        with pytest.raises(ValueError, match="Nombre d'événements"):
            registry.prepare(dict(metadata, event_count=count))


def test_jsonl_with_too_few_events_is_rejected(setup):
    registry, metadata = setup
    with pytest.raises(ValueError, match="pas assez d'événements"):
        registry.prepare(dict(metadata, event_count=4))