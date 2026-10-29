import hashlib
import json
import sys
from pathlib import Path
from unittest.mock import Mock

import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts"))

import real_session_config as runner
from run_active_real_collection import session_configuration


@pytest.fixture
def setup(tmp_path, monkeypatch):
    collection = tmp_path / "data/source_checks/template/collection"
    collection.mkdir(parents=True)

    archive = tmp_path / "gtfs.zip"
    archive.write_bytes(b"gtfs-fixture")

    references = tmp_path / "data/source_checks/fresh/references"
    references.mkdir(parents=True)
    (references / "routes.csv").write_text(
        "route_id,line_name\nR,Ligne R\n", encoding="utf-8"
    )
    (references / "stops.csv").write_text(
        "stop_id,stop_name\nS,Arrêt S\n", encoding="utf-8"
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

    config = session_configuration(
        "https://example.invalid/feed",
        archive,
        references,
    )
    (collection / "collection_config.json").write_text(
        json.dumps(config), encoding="utf-8"
    )

    monkeypatch.setattr(runner, "validate_java", Mock())
    return tmp_path, collection, references, config


def test_loads_unchanged_template(setup):
    project, collection, references, config = setup

    assert runner.load_ingestion_template(
        project, collection, references
    ) == config


def test_changed_references_are_refused(setup):
    project, collection, references, _ = setup
    (references / "routes.csv").write_text(
        "modified", encoding="utf-8"
    )

    with pytest.raises(ValueError, match="ont changé"):
        runner.load_ingestion_template(
            project, collection, references
        )


def test_different_silver_references_are_refused(setup):
    project, collection, _, _ = setup

    with pytest.raises(ValueError, match="mêmes références"):
        runner.load_ingestion_template(
            project, collection, project / "other-references"
        )


def test_command_targets_new_collection_not_template(setup):
    project, old_collection, _, config = setup
    new_collection = project / "data/source_checks/new/collection"

    command = runner.build_ingestion_command(
        project, config, new_collection
    )

    assert command[3] == str(new_collection)
    assert str(old_collection) not in command
    assert command[-1] == "--execute"
    assert command[command.index("--cycles") + 1] == "3"


def test_existing_collection_is_refused(setup):
    project, collection, _, config = setup

    with pytest.raises(ValueError, match="inexistante"):
        runner.build_ingestion_command(
            project, config, collection
        )