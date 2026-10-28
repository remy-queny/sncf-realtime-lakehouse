import importlib.util
import json
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location(
    "real_snapshot_normalizer", ROOT / "scripts/normalize_real_snapshots.py"
)
normalizer = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(normalizer)

PUBLISHED_AT = "2026-10-28T14:37:11Z"


@pytest.fixture
def setup(tmp_path):
    collection = tmp_path / "collection"
    (collection / "registry").mkdir(parents=True)
    snapshot = collection / "snapshots/one/tripupdates.pb"
    snapshot.parent.mkdir(parents=True)
    snapshot.write_bytes(b"simulated-protobuf")
    key = normalizer.sha256(snapshot)
    entry = {
        "sha256": key,
        "snapshotPath": str(snapshot),
        "sizeBytes": snapshot.stat().st_size,
        "publishedAt": PUBLISHED_AT,
    }
    (collection / "registry" / f"{key}.json").write_text(json.dumps(entry), encoding="utf-8")
    archive = tmp_path / "gtfs.zip"
    archive.write_bytes(b"simulated-gtfs")
    references = tmp_path / "references"
    references.mkdir()
    (references / "routes.csv").write_text("route_id,line_name\nR,Route\n", encoding="utf-8")
    (references / "stops.csv").write_text("stop_id,stop_name\nS,Stop\n", encoding="utf-8")
    (references / "manifest.json").write_text(json.dumps({
        "source": "sncf_gtfs_static", "archive_sha256": normalizer.sha256(archive),
    }), encoding="utf-8")
    return collection, archive, references, snapshot


def write_export(snapshot, archive, output):
    event = {
        "event_id": "event-one", "event_type": "trip_update",
        "source": "sncf_gtfs_rt", "schema_version": 1,
        "event_timestamp": PUBLISHED_AT,
        "trip_id": "T", "route_id": "R", "stop_id": "S",
    }
    output.write_text(json.dumps(event) + "\n", encoding="utf-8")


def run(setup, exporter=write_export):
    collection, archive, references, snapshot = setup
    return normalizer.normalize_pending(collection, archive, references, exporter)


def test_normalizes_already_collected_snapshot_without_new_download(setup):
    result = run(setup)
    assert result["newly_normalized"] == 1
    assert result["already_normalized"] == 0
    assert result["indexed_events"] == 1
    manifest = normalizer.read_json(setup[0] / "normalization_manifest.json")
    assert manifest["stage"] == "normalized"
    assert "published" not in manifest


def test_second_run_skips_export_and_leaves_manifest_unchanged(setup):
    run(setup)
    path = setup[0] / "normalization_manifest.json"
    before = path.read_bytes()
    def forbidden(*args):
        pytest.fail("Un export déjà validé ne doit pas être relancé.")
    result = run(setup, forbidden)
    assert result["newly_normalized"] == 0
    assert result["already_normalized"] == 1
    assert path.read_bytes() == before


def test_accepts_existing_manual_manifest_without_intent_file(setup):
    run(setup)
    snapshot = setup[3]
    snapshot.with_name("normalization.intent.json").unlink()
    path = setup[0] / "normalization_manifest.json"
    manifest = normalizer.read_json(path)
    manifest.pop("updated_at")
    normalizer.atomic_json(path, manifest)
    assert run(setup)["already_normalized"] == 1


def test_quarantines_owned_partial_and_retries_normalization(setup):
    output = setup[3].with_name("tripupdates.canonical.jsonl")
    partial = output.with_name(output.name + ".partial")
    def interrupted_export(snapshot, archive, destination):
        partial.write_text("incomplete", encoding="utf-8")
        raise RuntimeError("Interruption simulée")
    with pytest.raises(RuntimeError, match="Interruption simulée"):
        run(setup, interrupted_export)
    assert not (setup[0] / "normalization_manifest.json").exists()
    result = run(setup)
    assert result["newly_normalized"] == 1
    assert result["quarantined_partials"] == 1
    assert not partial.exists()
    abandoned = list(output.parent.glob("*.abandoned"))
    assert len(abandoned) == 1
    assert abandoned[0].read_text(encoding="utf-8") == "incomplete"


def test_recovers_completed_export_before_manifest_commit(setup):
    def interrupted_after_export(snapshot, archive, output):
        write_export(snapshot, archive, output)
        raise RuntimeError("Arrêt avant le manifeste")
    with pytest.raises(RuntimeError):
        run(setup, interrupted_after_export)
    def forbidden(*args):
        pytest.fail("L'export complet doit être récupéré, pas régénéré.")
    result = run(setup, forbidden)
    assert result["newly_normalized"] == 1
    assert result["indexed_events"] == 1


def test_rejects_modified_validated_export(setup):
    run(setup)
    output = setup[3].with_name("tripupdates.canonical.jsonl")
    output.write_text("modified", encoding="utf-8")
    with pytest.raises(ValueError, match="Export validé modifié"):
        run(setup)


def test_rejects_changed_reference_files(setup):
    run(setup)
    (setup[2] / "routes.csv").write_text("changed", encoding="utf-8")
    with pytest.raises(ValueError, match="références différentes"):
        run(setup)


def test_rejects_gtfs_archive_not_matching_reference_manifest(setup):
    setup[1].write_bytes(b"another-archive")
    with pytest.raises(ValueError, match="Archive GTFS différente"):
        run(setup)


def test_rejects_unowned_existing_export_without_deleting_it(setup):
    output = setup[3].with_name("tripupdates.canonical.jsonl")
    output.write_text("unowned", encoding="utf-8")
    with pytest.raises(ValueError, match="sans provenance"):
        run(setup)
    assert output.read_text(encoding="utf-8") == "unowned"


def test_rejects_modified_raw_snapshot(setup):
    setup[3].write_bytes(b"changed-protobuf")
    with pytest.raises(ValueError, match="Instantané modifié"):
        run(setup)


def test_does_not_register_invalid_export(setup):
    def invalid_export(snapshot, archive, output):
        output.write_text("{}\n", encoding="utf-8")
    with pytest.raises(ValueError, match="event_id"):
        run(setup, invalid_export)
    assert not (setup[0] / "normalization_manifest.json").exists()