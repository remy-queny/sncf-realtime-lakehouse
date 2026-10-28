import json
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts"))
import prepare_real_publication as planner
import publication_state as publication


@pytest.fixture
def setup(tmp_path):
    root = tmp_path / "collection"
    (root / "registry").mkdir(parents=True)
    archive = tmp_path / "gtfs.zip"
    archive.write_bytes(b"gtfs")
    references = tmp_path / "references"
    references.mkdir()
    (references / "routes.csv").write_text("routes", encoding="utf-8")
    (references / "stops.csv").write_text("stops", encoding="utf-8")
    (references / "manifest.json").write_text(json.dumps({
        "source": "sncf_gtfs_static", "archive_sha256": publication.sha256(archive),
    }), encoding="utf-8")
    records = []
    for index in range(2):
        folder = root / "snapshots" / f"snapshot-{index}"
        folder.mkdir(parents=True)
        snapshot = folder / "tripupdates.pb"
        snapshot.write_bytes(f"raw-{index}".encode("utf-8"))
        output = folder / "tripupdates.canonical.jsonl"
        events = [{
            "event_id": f"event-{index}-{number}", "trip_id": "trip-one",
            "source": "sncf_gtfs_rt", "event_type": "trip_update", "schema_version": 1,
        } for number in range(3)]
        output.write_text("".join(json.dumps(event) + "\n" for event in events), encoding="utf-8")
        key = publication.sha256(snapshot)
        published_at = f"2026-10-28T15:1{index}:00Z"
        (root / "registry" / f"{key}.json").write_text(json.dumps({
            "sha256": key, "snapshotPath": str(snapshot),
            "publishedAt": published_at, "sizeBytes": snapshot.stat().st_size,
        }), encoding="utf-8")
        records.append({
            "snapshot_path": str(snapshot), "snapshot_sha256": key,
            "published_at": published_at, "jsonl_path": str(output),
            "jsonl_sha256": publication.sha256(output), "event_count": 3,
        })
    manifest = {
        "manifest_version": 1, "stage": "normalized", "collection_root": str(root),
        "gtfs_archive_path": str(archive), "gtfs_archive_sha256": publication.sha256(archive),
        "reference_root": str(references),
        "reference_files_sha256": {
            name: publication.sha256(references / name)
            for name in ("routes.csv", "stops.csv", "manifest.json")
        },
        "snapshots": records, "total_events": 6,
    }
    (root / "normalization_manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    return root, manifest


def states(root):
    return sorted((root / "publication").glob("*.json"))


def test_prepares_bounded_pending_batches_without_kafka(setup):
    root, manifest = setup
    result = planner.prepare_publications(root, limit=2)
    assert result["snapshots_considered"] == 2
    assert result["available_events"] == 6
    assert result["selected_events"] == 4
    assert result["pending_batches"] == 2
    assert result["published_batches"] == 0
    assert result["kafka_send_performed"] is False
    assert len(states(root)) == 2
    for path in states(root):
        state = json.loads(path.read_text(encoding="utf-8"))
        assert state["metadata"]["selection"] == "first_n"
        assert state["metadata"]["event_count"] == 2
        assert state["metadata"]["available_events"] == 3
        assert state["attempts"] == 0


def test_preparing_again_does_not_rewrite_states_or_normalization(setup):
    root, manifest = setup
    normalization_before = (root / "normalization_manifest.json").read_bytes()
    planner.prepare_publications(root, limit=2)
    before = {path.name: path.read_bytes() for path in states(root)}
    planner.prepare_publications(root, limit=2)
    assert {path.name: path.read_bytes() for path in states(root)} == before
    assert (root / "normalization_manifest.json").read_bytes() == normalization_before


def test_does_not_reset_an_already_published_batch(setup):
    root, manifest = setup
    planner.prepare_publications(root, limit=2)
    state_path = states(root)[0]
    state = json.loads(state_path.read_text(encoding="utf-8"))
    registry = publication.PublicationRegistry(root / "publication")
    def sender(metadata):
        ids = publication.validate_metadata(metadata)
        return {
            "topic": metadata["topic"], "bootstrap_servers": metadata["bootstrap_servers"],
            "acknowledgements": [
                {"event_id": event_id, "topic": metadata["topic"], "partition": 0, "offset": index}
                for index, event_id in enumerate(ids)
            ],
        }
    registry.publish(state["batch_id"], sender)
    before = state_path.read_bytes()
    result = planner.prepare_publications(root, limit=2)
    assert result["published_batches"] == 1
    assert result["pending_batches"] == 1
    assert state_path.read_bytes() == before


def test_cannot_change_selection_for_existing_batches(setup):
    root, manifest = setup
    planner.prepare_publications(root, limit=2)
    before = {path.name: path.read_bytes() for path in states(root)}
    with pytest.raises(ValueError, match="sélection ou provenance différente"):
        planner.prepare_publications(root, limit=3)
    assert {path.name: path.read_bytes() for path in states(root)} == before


def test_rejects_modified_jsonl_before_registry_creation(setup):
    root, manifest = setup
    Path(manifest["snapshots"][0]["jsonl_path"]).write_text("modified", encoding="utf-8")
    with pytest.raises(ValueError, match="Export JSONL modifié"):
        planner.prepare_publications(root)
    assert not (root / "publication").exists()


def test_rejects_modified_snapshot(setup):
    root, manifest = setup
    Path(manifest["snapshots"][0]["snapshot_path"]).write_bytes(b"changed")
    with pytest.raises(ValueError, match="Instantané brut modifié"):
        planner.prepare_publications(root)
    assert not (root / "publication").exists()


def test_rejects_modified_references(setup):
    root, manifest = setup
    (Path(manifest["reference_root"]) / "routes.csv").write_text("changed", encoding="utf-8")
    with pytest.raises(ValueError, match="Référence modifiée"):
        planner.prepare_publications(root)
    assert not (root / "publication").exists()


def test_rejects_inconsistent_normalization_total(setup):
    root, manifest = setup
    manifest["total_events"] = 7
    (root / "normalization_manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    with pytest.raises(ValueError, match="Total incohérent"):
        planner.prepare_publications(root)


def test_rejects_invalid_limit(setup):
    for limit in (0, -1, 20001, True):
        with pytest.raises(ValueError, match="La limite"):
            planner.prepare_publications(setup[0], limit=limit)


def test_selects_all_available_events_when_fewer_than_limit(setup):
    result = planner.prepare_publications(setup[0], limit=100)
    assert result["selected_events"] == 6
    assert result["per_snapshot_limit"] == 100


def test_rejects_jsonl_outside_snapshot_directory(setup, tmp_path):
    root, manifest = setup
    record = manifest["snapshots"][0]
    external = tmp_path / "external.jsonl"
    external.write_bytes(Path(record["jsonl_path"]).read_bytes())
    record["jsonl_path"] = str(external)
    (root / "normalization_manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    with pytest.raises(ValueError, match="Chemin JSONL incompatible"):
        planner.prepare_publications(root)