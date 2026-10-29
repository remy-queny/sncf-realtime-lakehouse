import json
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts"))

from real_active_profile import build_real_environment


@pytest.fixture
def setup(tmp_path):
    references = tmp_path / "data/source_checks/fresh/references"
    references.mkdir(parents=True)

    (references / "manifest.json").write_text(
        json.dumps({"source": "sncf_gtfs_static"}),
        encoding="utf-8",
    )
    (references / "routes.csv").write_text(
        "route_id,line_name\nR,Ligne R\n",
        encoding="utf-8",
    )
    (references / "stops.csv").write_text(
        "stop_id,stop_name\nS,Arrêt S\n",
        encoding="utf-8",
    )
    return tmp_path, references


def test_real_profile_overrides_simulation_environment(setup):
    project, references = setup

    env = build_real_environment(
        project,
        references,
        interval=30,
        base={
            "KAFKA_TRIP_UPDATES_TOPIC": "sncf.trip_updates.raw",
            "LAKEHOUSE_ROOT": "/simulation",
            "CHECKPOINT_ROOT": "/simulation-checkpoints",
            "GTFS_REFERENCE_ROOT": "/simulation-references",
            "POSTGRES_TARGET_SCHEMA": "sncf_active_test",
            "STREAMING_MODE": "available_now",
        },
    )

    assert env["KAFKA_TRIP_UPDATES_TOPIC"] == (
        "sncf.trip_updates.real.raw"
    )
    assert env["LAKEHOUSE_ROOT"] == str(
        project / "data/lakehouse_real"
    )
    assert env["CHECKPOINT_ROOT"] == str(
        project / "data/checkpoints_real"
    )
    assert env["BRONZE_TRIP_UPDATES_CHECKPOINT_PATH"] == str(
        project / "data/checkpoints_real/bronze_trip_updates"
    )
    assert env["GTFS_REFERENCE_ROOT"] == str(references)
    assert env["POSTGRES_TARGET_SCHEMA"] == "sncf_real"
    assert env["STREAMING_MODE"] == "active"
    assert env["STREAMING_INTERVAL_SECONDS"] == "30"


@pytest.mark.parametrize("interval", [0, -1, True, 1.5])
def test_invalid_interval_is_rejected(setup, interval):
    project, references = setup

    with pytest.raises(ValueError, match="Intervalle"):
        build_real_environment(project, references, interval)


def test_simulation_reference_path_is_rejected(setup):
    project, _ = setup

    with pytest.raises(ValueError, match="périmètre réel"):
        build_real_environment(
            project, project / "spark/resources/simulation"
        )


def test_non_real_manifest_is_rejected(setup):
    project, references = setup
    (references / "manifest.json").write_text(
        json.dumps({"source": "simulation"}),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="référentiel SNCF réel"):
        build_real_environment(project, references)


def test_incomplete_reference_headers_are_rejected(setup):
    project, references = setup
    (references / "routes.csv").write_text(
        "route_id\nR\n", encoding="utf-8"
    )

    with pytest.raises(ValueError, match="Colonnes absentes"):
        build_real_environment(project, references)


def test_building_profile_does_not_create_lakehouse_or_checkpoints(setup):
    project, references = setup

    build_real_environment(project, references)

    assert not (project / "data/lakehouse_real").exists()
    assert not (project / "data/checkpoints_real").exists()
    