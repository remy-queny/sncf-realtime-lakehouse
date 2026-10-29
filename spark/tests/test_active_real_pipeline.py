import json
import sys
from pathlib import Path
from unittest.mock import Mock

import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts"))

import run_active_real_pipeline as runner


@pytest.fixture
def setup(tmp_path, monkeypatch):
    references = tmp_path / "data/source_checks/fresh/references"
    references.mkdir(parents=True)

    (references / "manifest.json").write_text(
        json.dumps({"source": "sncf_gtfs_static"}),
        encoding="utf-8",
    )
    (references / "routes.csv").write_text(
        "route_id,line_name\nR,Ligne R\n", encoding="utf-8"
    )
    (references / "stops.csv").write_text(
        "stop_id,stop_name\nS,Arrêt S\n", encoding="utf-8"
    )

    for relative in (
        "data/lakehouse_real/bronze/trip_updates/_delta_log",
        "data/lakehouse_real/silver/trip_delays/_delta_log",
        "data/lakehouse_real/silver/rejected_events/_delta_log",
        "data/checkpoints_real/bronze_trip_updates",
        "data/checkpoints_real/silver_trip_delays",
        "data/checkpoints_real/silver_rejected_events",
    ):
        (tmp_path / relative).mkdir(parents=True)

    for relative in (
        "spark/src/jobs/stream_bronze.py",
        "spark/src/jobs/stream_silver.py",
        "scripts/run_active_real_gold.py",
    ):
        path = tmp_path / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("# fixture\n", encoding="utf-8")

    supervise = Mock(return_value=0)
    monkeypatch.setattr(runner, "PROJECT_ROOT", tmp_path)
    monkeypatch.setattr(runner, "supervise_real_jobs", supervise)

    return tmp_path, references, supervise


def test_check_only_starts_nothing(setup):
    project, references, supervise = setup

    assert runner.main([str(references), "--check-only"]) == 0

    supervise.assert_not_called()
    assert not (project / "data/.active-real-pipeline.lock").exists()
    assert not list(
        (project / "data/source_checks").glob(
            "active-real-processing-*"
        )
    )


def test_execute_passes_real_environment(setup):
    _, references, supervise = setup

    assert runner.main([str(references), "--execute"]) == 0

    env = supervise.call_args.kwargs["env"]
    assert env["POSTGRES_TARGET_SCHEMA"] == "sncf_real"
    assert env["STREAMING_MODE"] == "active"
    assert env["GTFS_REFERENCE_ROOT"] == str(references)


def test_missing_checkpoint_blocks_execution(setup):
    project, references, supervise = setup
    (project / "data/checkpoints_real/silver_trip_delays").rmdir()

    assert runner.main([str(references), "--execute"]) == 1

    supervise.assert_not_called()


def test_supervision_failure_is_propagated(setup):
    _, references, supervise = setup
    supervise.return_value = 1

    assert runner.main([str(references), "--execute"]) == 1


def test_second_supervisor_is_refused(setup):
    project, references, supervise = setup

    with (project / "data/.active-real-pipeline.lock").open("a") as lock:
        runner.fcntl.flock(
            lock.fileno(),
            runner.fcntl.LOCK_EX | runner.fcntl.LOCK_NB,
        )

        assert runner.main([str(references), "--execute"]) == 1

    supervise.assert_not_called()