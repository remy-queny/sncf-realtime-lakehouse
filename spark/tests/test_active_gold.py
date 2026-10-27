import importlib.util
from pathlib import Path
from unittest.mock import Mock, patch

import pytest

from jobs.gold_cycle import GoldCycleResult

ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location(
    "active_gold_runner", ROOT / "scripts/run_active_gold.py"
)
runner = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(runner)


@pytest.fixture
def cycle_result():
    return GoldCycleResult(5, 3, 3, 3, 10.0)


def test_unchanged_version_is_skipped():
    spark = Mock()
    root = ROOT / "data/validation/active-stream-unit/lakehouse"
    with (
        patch.object(runner, "get_latest_silver_version", return_value=5),
        patch.object(runner, "run_gold_cycle") as cycle,
    ):
        assert runner.refresh_if_needed(spark, root, 5) is None
    cycle.assert_not_called()


def test_new_version_is_passed_explicitly(cycle_result):
    spark = Mock()
    root = ROOT / "data/validation/active-stream-unit/lakehouse"
    with (
        patch.object(runner, "get_latest_silver_version", return_value=5),
        patch.object(runner, "run_gold_cycle", return_value=cycle_result) as cycle,
    ):
        assert runner.refresh_if_needed(spark, root, 4) == cycle_result
    cycle.assert_called_once_with(
        spark=spark, lakehouse_root=root,
        target_schema="sncf_active_test", silver_version=5,
    )


def test_once_runs_without_sleeping(cycle_result):
    stop = Mock()
    stop.is_set.return_value = False
    with patch.object(runner, "refresh_if_needed", return_value=cycle_result) as refresh:
        runner.run_refresh_loop(Mock(), ROOT, 60, stop, once=True)
    assert refresh.call_count == 1
    assert refresh.call_args.args[2] is None
    stop.wait.assert_not_called()


def test_successful_version_is_remembered(cycle_result):
    stop = Mock()
    stop.is_set.side_effect = [False, False, True]
    with patch.object(runner, "refresh_if_needed", side_effect=[cycle_result, None]) as refresh:
        runner.run_refresh_loop(Mock(), ROOT, 60, stop)
    assert [call.args[2] for call in refresh.call_args_list] == [None, 5]
    assert stop.wait.call_count == 2


def test_failed_cycle_is_not_retried_silently():
    stop = Mock()
    stop.is_set.return_value = False
    with patch.object(runner, "refresh_if_needed", side_effect=RuntimeError("failure")) as refresh:
        with pytest.raises(RuntimeError, match="failure"):
            runner.run_refresh_loop(Mock(), ROOT, 60, stop)
    assert refresh.call_count == 1
    stop.wait.assert_not_called()


def test_already_requested_stop_does_not_start_cycle():
    stop = Mock()
    stop.is_set.return_value = True
    with patch.object(runner, "refresh_if_needed") as refresh:
        runner.run_refresh_loop(Mock(), ROOT, 60, stop)
    refresh.assert_not_called()


def test_missing_credentials_are_rejected(monkeypatch):
    monkeypatch.delenv("POSTGRES_USER", raising=False)
    with patch.object(runner.subprocess, "check_output", return_value=""):
        with pytest.raises(RuntimeError, match="POSTGRES_USER"):
            runner.load_postgres_environment()


def test_credentials_are_loaded_without_printing(monkeypatch, capsys):
    for variable in ("POSTGRES_USER", "POSTGRES_DB", "POSTGRES_PASSWORD"):
        monkeypatch.delenv(variable, raising=False)
    with patch.object(runner.subprocess, "check_output", side_effect=["test_user", "test_db", "test_secret"]):
        runner.load_postgres_environment()
    assert runner.os.environ["POSTGRES_PASSWORD"] == "test_secret"
    captured = capsys.readouterr()
    assert "test_secret" not in captured.out + captured.err

def test_stop_handlers_are_installed_after_spark_creation():
    stop_requested = runner.Event()
    spark = Mock()
    events = []
    handlers = {}

    def create_session():
        events.append("spark_created")
        return spark

    def install_handler(signum, handler):
        events.append(signum)
        handlers[signum] = handler

    with (
        patch.object(
            runner,
            "create_spark_session",
            side_effect=create_session,
        ),
        patch.object(
            runner.signal,
            "signal",
            side_effect=install_handler,
        ),
    ):
        result = runner.create_interruptible_spark_session(
            stop_requested
        )

    assert result is spark
    assert events == [
        "spark_created",
        runner.signal.SIGINT,
        runner.signal.SIGTERM,
    ]

    handlers[runner.signal.SIGINT](
        runner.signal.SIGINT,
        None,
    )
    assert stop_requested.is_set()

    stop_requested.clear()

    handlers[runner.signal.SIGTERM](
        runner.signal.SIGTERM,
        None,
    )
    assert stop_requested.is_set()