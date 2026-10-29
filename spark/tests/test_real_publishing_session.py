import hashlib
import json
import sys
from pathlib import Path
from unittest.mock import Mock

import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts"))

import real_publication_cycle as cycle


@pytest.fixture
def setup(tmp_path, monkeypatch):
    archive = tmp_path / "gtfs.zip"
    archive.write_bytes(b"gtfs-fixture")

    references = tmp_path / "references"
    references.mkdir()
    (references / "routes.csv").write_text(
        "routes", encoding="utf-8"
    )
    (references / "stops.csv").write_text(
        "stops", encoding="utf-8"
    )
    (references / "manifest.json").write_text(
        json.dumps({
            "source": "sncf_gtfs_static",
            "archive_sha256": hashlib.sha256(
                archive.read_bytes()
            ).hexdigest(),
        }),
        encoding="utf-8",
    )

    normalize = Mock(return_value={
        "newly_normalized": 1,
        "already_normalized": 0,
        "indexed_snapshots": 1,
        "indexed_events": 100,
    })
    publish = Mock(return_value={
        "published_batches": 1,
        "published_events": 100,
    })

    monkeypatch.setattr(cycle, "normalize_pending", normalize)
    monkeypatch.setattr(cycle, "run_publication_cycle", publish)

    return tmp_path / "collection", archive, references, normalize, publish


def run(setup, collect, **options):
    root, archive, references, _, _ = setup

    return cycle.run_publishing_session(
        "https://example.invalid/feed",
        root,
        archive,
        references,
        project_root=ROOT,
        collect=collect,
        exporter=Mock(),
        waiter=lambda seconds: None,
        reporter=lambda message: None,
        **options,
    )


def test_each_collection_cycle_normalizes_then_publishes(setup):
    _, _, _, normalize, publish = setup
    events = []

    def normalized(*args):
        events.append("normalize")
        return {
            "newly_normalized": 1,
            "indexed_snapshots": 1,
            "indexed_events": 100,
        }

    def published(*args, **kwargs):
        events.append("publish")
        return {"published_batches": 1, "published_events": 100}

    normalize.side_effect = normalized
    publish.side_effect = published

    result = run(setup, Mock(), cycles=2)

    assert events == ["normalize", "publish", "normalize", "publish"]
    assert result["kafka_enabled"] is True
    assert result["published_batches"] == 2
    assert result["published_events"] == 200


def test_unchanged_snapshot_still_checks_publication_registry(setup):
    _, _, _, normalize, publish = setup
    normalize.return_value["newly_normalized"] = 0
    publish.return_value = {
        "published_batches": 0,
        "published_events": 0,
    }

    result = run(setup, Mock(), cycles=2)

    assert publish.call_count == 2
    assert result["published_events"] == 0


def test_existing_collection_is_refused_before_collecting(setup):
    root, _, _, _, publish = setup
    root.mkdir()
    collect = Mock()

    with pytest.raises(ValueError, match="nouveau dossier"):
        run(setup, collect)

    collect.assert_not_called()
    publish.assert_not_called()


def test_publication_failure_stops_following_collection_cycles(setup):
    _, _, _, _, publish = setup
    collect = Mock()
    publish.side_effect = RuntimeError("Publication interrompue")

    with pytest.raises(RuntimeError, match="Publication interrompue"):
        run(setup, collect, cycles=3)

    assert collect.call_count == 1
    assert publish.call_count == 1