import os
import sys
from pathlib import Path
from unittest.mock import Mock, patch

import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts"))

import run_active_real_gold as runner


@pytest.fixture
def setup(tmp_path, monkeypatch):
    lakehouse = tmp_path / "data/lakehouse_real"
    checkpoints = tmp_path / "data/checkpoints_real"
    (lakehouse / "silver/trip_delays/_delta_log").mkdir(
        parents=True
    )
    checkpoints.mkdir()

    env = {
        "LAKEHOUSE_ROOT": str(lakehouse),
        "CHECKPOINT_ROOT": str(checkpoints),
        "POSTGRES_TARGET_SCHEMA": "sncf_real",
    }
    profile = Mock(return_value=env)
    spark = Mock()
    create = Mock(return_value=spark)
    load = Mock()
    loop = Mock()

    monkeypatch.setattr(runner, "PROJECT_ROOT", tmp_path)
    monkeypatch.setattr(runner, "build_real_environment", profile)
    monkeypatch.setattr(
        runner, "create_interruptible_spark_session", create
    )
    monkeypatch.setattr(runner, "load_postgres_environment", load)
    monkeypatch.setattr(runner, "run_refresh_loop", loop)

    with patch.dict(os.environ):
        yield tmp_path, lakehouse, spark, create, load, loop


def test_once_uses_real_target_and_stops_spark(setup):
    project, lakehouse, spark, _, _, loop = setup

    code = runner.main([
        str(project / "references"),
        "--once",
        "--interval", "60",
    ])

    assert code == 0
    assert loop.call_args.kwargs["lakehouse_root"] == lakehouse
    assert loop.call_args.kwargs["target_schema"] == "sncf_real"
    assert loop.call_args.kwargs["once"] is True
    assert loop.call_args.kwargs["interval"] == 60
    spark.stop.assert_called_once()


def test_missing_silver_blocks_before_spark(setup):
    project, lakehouse, _, create, load, loop = setup
    (lakehouse / "silver/trip_delays/_delta_log").rmdir()

    code = runner.main([str(project / "references"), "--once"])

    assert code == 1
    create.assert_not_called()
    load.assert_not_called()
    loop.assert_not_called()


def test_cycle_failure_still_stops_spark(setup):
    project, _, spark, _, _, loop = setup
    loop.side_effect = RuntimeError("Échec de publication")

    code = runner.main([str(project / "references"), "--once"])

    assert code == 1
    spark.stop.assert_called_once()


def test_second_real_gold_launcher_is_refused(setup):
    project, _, _, create, _, loop = setup
    lock_path = project / "data/.active-real-gold.lock"

    with lock_path.open("a") as lock:
        runner.fcntl.flock(
            lock.fileno(),
            runner.fcntl.LOCK_EX | runner.fcntl.LOCK_NB,
        )

        code = runner.main([
            str(project / "references"), "--once"
        ])

    assert code == 1
    create.assert_not_called()
    loop.assert_not_called()


def test_spark_stop_failure_is_reported(setup):
    project, _, spark, _, _, _ = setup
    spark.stop.side_effect = RuntimeError("Arrêt Spark impossible")

    assert runner.main([
        str(project / "references"), "--once"
    ]) == 1