import sys
from pathlib import Path
from unittest.mock import Mock, patch

import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(ROOT / "spark/src"))

import run_active_gold as runner
from jobs.gold_cycle import GoldCycleResult


def test_real_target_is_forwarded_to_gold_cycle():
    spark = Mock()
    lakehouse = ROOT / "data/lakehouse_real"
    result = GoldCycleResult(7, 3, 1, 2, 0.1)

    with (
        patch.object(
            runner, "get_latest_silver_version", return_value=7
        ),
        patch.object(
            runner, "run_gold_cycle", return_value=result
        ) as cycle,
    ):
        actual = runner.refresh_if_needed(
            spark,
            lakehouse,
            None,
            target_schema="sncf_real",
        )

    assert actual is result
    cycle.assert_called_once_with(
        spark=spark,
        lakehouse_root=lakehouse,
        target_schema="sncf_real",
        silver_version=7,
    )


def test_wrong_schema_blocks_before_silver_read():
    with (
        patch.object(runner, "get_latest_silver_version") as latest,
        patch.object(runner, "run_gold_cycle") as cycle,
    ):
        with pytest.raises(ValueError):
            runner.refresh_if_needed(
                Mock(),
                ROOT / "data/lakehouse_real",
                None,
                target_schema="sncf_active_test",
            )

    latest.assert_not_called()
    cycle.assert_not_called()


def test_unchanged_real_silver_skips_publication():
    with (
        patch.object(
            runner, "get_latest_silver_version", return_value=7
        ),
        patch.object(runner, "run_gold_cycle") as cycle,
    ):
        result = runner.refresh_if_needed(
            Mock(),
            ROOT / "data/lakehouse_real",
            7,
            target_schema="sncf_real",
        )

    assert result is None
    cycle.assert_not_called()


def test_loop_forwards_real_schema():
    spark = Mock()
    lakehouse = ROOT / "data/lakehouse_real"
    stop = Mock()
    stop.is_set.return_value = False
    result = GoldCycleResult(7, 3, 1, 2, 0.1)

    with patch.object(
        runner, "refresh_if_needed", return_value=result
    ) as refresh:
        runner.run_refresh_loop(
            spark,
            lakehouse,
            60,
            stop,
            once=True,
            target_schema="sncf_real",
        )

    refresh.assert_called_once_with(
        spark,
        lakehouse,
        None,
        target_schema="sncf_real",
    )
    stop.wait.assert_not_called()