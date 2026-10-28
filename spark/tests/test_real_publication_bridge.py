import json
import signal
import subprocess
import sys
from pathlib import Path
from unittest.mock import Mock

import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts"))
import publication_state as publication
import publish_real_publication as bridge


@pytest.fixture
def setup(tmp_path):
    root = tmp_path / "collection"
    folder = root / "snapshots/one"
    folder.mkdir(parents=True)
    snapshot = folder / "tripupdates.pb"
    snapshot.write_bytes(b"raw-fixture")
    output = folder / "tripupdates.canonical.jsonl"
    events = [{
        "event_id": f"event-{number}", "source": "sncf_gtfs_rt",
        "event_type": "trip_update", "schema_version": 1, "trip_id": "trip-one",
    } for number in range(3)]
    output.write_text("".join(json.dumps(event) + "\n" for event in events), encoding="utf-8")
    archive = tmp_path / "gtfs.zip"
    archive.write_bytes(b"gtfs-fixture")
    references = tmp_path / "references"
    references.mkdir()
    for name, content in (("routes.csv", "routes"), ("stops.csv", "stops"), ("manifest.json", "{}")):
        (references / name).write_text(content, encoding="utf-8")
    hashes = {name: publication.sha256(references / name) for name in ("routes.csv", "stops.csv", "manifest.json")}
    record = {
        "snapshot_path": str(snapshot), "snapshot_sha256": publication.sha256(snapshot),
        "published_at": "2026-10-28T15:11:00Z", "jsonl_path": str(output),
        "jsonl_sha256": publication.sha256(output), "event_count": 3,
    }
    manifest = {
        "manifest_version": 1, "stage": "normalized", "collection_root": str(root),
        "snapshots": [record], "reference_root": str(references),
        "reference_files_sha256": hashes,
        "gtfs_archive_path": str(archive), "gtfs_archive_sha256": publication.sha256(archive),
    }
    (root / "normalization_manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    metadata = dict(record)
    metadata.pop("event_count")
    metadata.update({
        "collection_root": str(root), "event_count": 2, "available_events": 3,
        "selection": "first_n", "selection_limit": 2,
        "topic": publication.REAL_TOPIC, "bootstrap_servers": "localhost:9092",
        "reference_root": str(references), "reference_manifest_sha256": hashes["manifest.json"],
        "gtfs_archive_sha256": publication.sha256(archive),
    })
    registry = publication.PublicationRegistry(root / "publication")
    state = registry.prepare(metadata)
    return root, registry, state, manifest


def receipt(metadata):
    return {
        "topic": metadata["topic"], "bootstrap_servers": metadata["bootstrap_servers"],
        "acknowledgements": [
            {"event_id": event_id, "topic": metadata["topic"], "partition": 0, "offset": index}
            for index, event_id in enumerate(publication.validate_metadata(metadata))
        ],
    }


def test_inspection_does_not_change_state_or_create_receipts(setup):
    root, registry, state, manifest = setup
    before = registry.path(state["batch_id"]).read_bytes()
    result = bridge.inspect_batches(root)
    assert result["mode"] == "inspect"
    assert result["kafka_send_performed"] is False
    assert result["pending_batches"] == 1
    assert result["selected_events"] == 2
    assert registry.path(state["batch_id"]).read_bytes() == before
    assert not (root / "publication/receipts").exists()


def test_sender_uses_five_explicit_java_arguments(setup, tmp_path):
    root, registry, state, manifest = setup
    receipt_path = tmp_path / "receipt.json"
    log_path = tmp_path / "producer.log"
    commands = []
    def fake_java(command, project, stdout=None):
        commands.append(command)
        stdout.write("mock producer\n")
        receipt_path.write_text(json.dumps(receipt(state["metadata"])), encoding="utf-8")
    sender = bridge.make_java_sender(
        ROOT, receipt_path, log_path, runner=fake_java, prefix=["fake-java", "-cp", "fixture"]
    )
    result = sender(state["metadata"])
    assert commands[0][-5:] == [
        state["metadata"]["jsonl_path"], "2", "localhost:9092",
        str(receipt_path), state["metadata"]["jsonl_sha256"],
    ]
    assert len(result["acknowledgements"]) == 2
    assert log_path.read_text(encoding="utf-8") == "mock producer\n"


def test_receipt_is_validated_before_state_becomes_published(setup):
    root, registry, state, manifest = setup
    def factory(receipt_path, log_path):
        def sender(metadata):
            assert registry.get(state["batch_id"])["status"] == "publishing"
            result = receipt(metadata)
            receipt_path.write_text(json.dumps(result), encoding="utf-8")
            return result
        return sender
    result = bridge.publish_one(root, state["batch_id"], ROOT, factory)
    assert result["published_now"] is True
    assert result["status"] == "published"
    assert result["event_count"] == 2
    assert Path(result["receipt_file"]).is_file()


def test_second_execution_skips_sender_for_completed_batch(setup):
    root, registry, state, manifest = setup
    registry.publish(state["batch_id"], receipt)
    factory = Mock()
    result = bridge.publish_one(root, state["batch_id"], ROOT, factory)
    assert result["published_now"] is False
    factory.assert_not_called()


def test_uncertain_batch_is_not_sent_again(setup):
    root, registry, state, manifest = setup
    with pytest.raises(RuntimeError):
        registry.publish(state["batch_id"], Mock(side_effect=RuntimeError("possible partial send")))
    factory = Mock()
    with pytest.raises(publication.UncertainPublication):
        bridge.publish_one(root, state["batch_id"], ROOT, factory)
    factory.assert_not_called()


def test_existing_receipt_blocks_pending_batch_before_sender_creation(setup):
    root, registry, state, manifest = setup
    receipts = root / "publication/receipts"
    receipts.mkdir()
    (receipts / f"{state['batch_id']}.json").write_text("existing", encoding="utf-8")
    factory = Mock()
    with pytest.raises(ValueError, match="diagnostic requis"):
        bridge.publish_one(root, state["batch_id"], ROOT, factory)
    assert registry.get(state["batch_id"])["status"] == "pending"
    factory.assert_not_called()


def test_changed_references_block_publication_before_sender(setup):
    root, registry, state, manifest = setup
    (Path(manifest["reference_root"]) / "routes.csv").write_text("changed", encoding="utf-8")
    factory = Mock()
    with pytest.raises(ValueError, match="Références modifiées"):
        bridge.publish_one(root, state["batch_id"], ROOT, factory)
    assert registry.get(state["batch_id"])["status"] == "pending"
    factory.assert_not_called()


def test_java_failure_leaves_batch_uncertain(setup):
    root, registry, state, manifest = setup
    def factory(receipt_path, log_path):
        return Mock(side_effect=subprocess.CalledProcessError(1, ["java"]))
    with pytest.raises(subprocess.CalledProcessError):
        bridge.publish_one(root, state["batch_id"], ROOT, factory)
    assert registry.get(state["batch_id"])["status"] == "uncertain"


def test_interruption_leaves_batch_uncertain(setup):
    root, registry, state, manifest = setup
    def factory(receipt_path, log_path):
        def sender(metadata):
            raise bridge.StopRequested(signal.SIGINT)
        return sender
    with pytest.raises(bridge.StopRequested):
        bridge.publish_one(root, state["batch_id"], ROOT, factory)
    assert registry.get(state["batch_id"])["status"] == "uncertain"


def test_incomplete_receipt_is_never_reported_as_published(setup):
    root, registry, state, manifest = setup
    def factory(receipt_path, log_path):
        def sender(metadata):
            result = receipt(metadata)
            result["acknowledgements"].pop()
            return result
        return sender
    with pytest.raises(ValueError, match="incomplets"):
        bridge.publish_one(root, state["batch_id"], ROOT, factory)
    assert registry.get(state["batch_id"])["status"] == "uncertain"


def test_execute_without_batch_id_is_rejected(monkeypatch, setup):
    monkeypatch.setattr(sys, "argv", ["publish_real_publication.py", str(setup[0]), "--execute"])
    with pytest.raises(SystemExit) as error:
        bridge.main()
    assert error.value.code == 2


def test_missing_registry_does_not_create_directories(tmp_path):
    root = tmp_path / "absent"
    with pytest.raises(ValueError, match="Registre de publication absent"):
        bridge.inspect_batches(root)
    assert not root.exists()