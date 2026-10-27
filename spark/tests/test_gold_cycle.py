from contextlib import ExitStack
from unittest.mock import Mock, patch

import pytest

from jobs import gold_cycle


def test_snapshot_read_is_pinned_to_requested_version():
    spark = Mock()
    path = gold_cycle.PROJECT_ROOT / "data/validation/active-stream-unit/lakehouse/silver/trip_delays"
    frame = gold_cycle.read_silver_snapshot(spark, path, 7)
    reader = spark.read.format.return_value
    spark.read.format.assert_called_once_with("delta")
    reader.option.assert_called_once_with("versionAsOf", 7)
    reader.option.return_value.load.assert_called_once_with(str(path))
    assert frame is reader.option.return_value.load.return_value


@pytest.mark.parametrize("version", [-1, True, "7"])
def test_snapshot_rejects_invalid_versions(version):
    spark = Mock()
    with pytest.raises(ValueError):
        gold_cycle.read_silver_snapshot(spark, gold_cycle.PROJECT_ROOT, version)
    spark.read.format.assert_not_called()


def cycle_setup(stack):
    spark = Mock()
    spark.conf.get.return_value = "UTC"
    silver = Mock()
    silver.persist.return_value = silver
    silver.count.return_value = 3
    lines = Mock()
    lines.persist.return_value = lines
    stations = Mock()
    stations.persist.return_value = stations
    patches = {}
    for name in (
        "check_duplicate_event_ids", "check_required_fields",
        "check_delay_consistency", "check_enrichment",
        "publish_gold_dataframes", "get_latest_silver_version",
        "read_silver_snapshot", "aggregate_delay_by_line",
        "aggregate_station_delay_daily", "_gold_metrics",
    ):
        patches[name] = stack.enter_context(patch.object(gold_cycle, name))
    patches["get_latest_silver_version"].return_value = 7
    patches["read_silver_snapshot"].return_value = silver
    patches["aggregate_delay_by_line"].return_value = lines
    patches["aggregate_station_delay_daily"].return_value = stations
    patches["_gold_metrics"].side_effect = [(2, 3), (3, 3)]
    return spark, silver, lines, stations, patches


def test_cycle_checks_and_publishes_the_same_snapshot():
    with ExitStack() as stack:
        spark, silver, lines, stations, patches = cycle_setup(stack)
        root = gold_cycle.PROJECT_ROOT / "data/validation/active-stream-unit/lakehouse"
        result = gold_cycle.run_gold_cycle(spark, root, "sncf_active_test")

        assert result.silver_version == 7
        assert result.observation_count == 3
        assert result.line_aggregate_count == 2
        assert result.station_aggregate_count == 3
        patches["read_silver_snapshot"].assert_called_once_with(spark, root / "silver/trip_delays", 7)
        for name in ("check_duplicate_event_ids", "check_required_fields", "check_delay_consistency", "check_enrichment"):
            patches[name].assert_called_once_with(silver)
        patches["aggregate_delay_by_line"].assert_called_once_with(silver)
        patches["aggregate_station_delay_daily"].assert_called_once_with(silver)
        patches["publish_gold_dataframes"].assert_called_once_with(
            lines=lines, stations=stations, lakehouse_root=root, target_schema="sncf_active_test"
        )
        lines.write.format.return_value.mode.return_value.save.assert_called_once_with(str(root / "gold/delay_by_line_15min"))
        stations.write.format.return_value.mode.return_value.save.assert_called_once_with(str(root / "gold/station_delay_daily"))
        for frame in (silver, lines, stations):
            frame.unpersist.assert_called_once()


def test_quality_failure_blocks_gold_and_publication():
    with ExitStack() as stack:
        spark, silver, lines, stations, patches = cycle_setup(stack)
        patches["check_enrichment"].side_effect = RuntimeError("enrichissement incomplet")
        with pytest.raises(RuntimeError, match="enrichissement incomplet"):
            gold_cycle.run_gold_cycle(spark, gold_cycle.PROJECT_ROOT / "data/lakehouse", "public")
        patches["aggregate_delay_by_line"].assert_not_called()
        patches["publish_gold_dataframes"].assert_not_called()
        silver.unpersist.assert_called_once()


def test_incorrect_volumes_block_all_writes():
    with ExitStack() as stack:
        spark, silver, lines, stations, patches = cycle_setup(stack)
        patches["_gold_metrics"].side_effect = [(2, 2), (3, 3)]
        with pytest.raises(RuntimeError, match="Volumes Silver/Gold"):
            gold_cycle.run_gold_cycle(spark, gold_cycle.PROJECT_ROOT / "data/lakehouse", "public")
        lines.write.format.assert_not_called()
        stations.write.format.assert_not_called()
        patches["publish_gold_dataframes"].assert_not_called()


def test_explicit_version_does_not_read_latest_version():
    with ExitStack() as stack:
        spark, silver, lines, stations, patches = cycle_setup(stack)
        result = gold_cycle.run_gold_cycle(spark, gold_cycle.PROJECT_ROOT / "data/lakehouse", "public", silver_version=4)
        assert result.silver_version == 4
        patches["get_latest_silver_version"].assert_not_called()
        assert patches["read_silver_snapshot"].call_args.args[2] == 4


def test_limit_blocks_writes_before_publication():
    with ExitStack() as stack:
        spark, silver, lines, stations, patches = cycle_setup(stack)
        patches["_gold_metrics"].side_effect = [(gold_cycle.MAX_ROWS_PER_TABLE + 1, 3), (3, 3)]
        with pytest.raises(ValueError, match="agrégats"):
            gold_cycle.run_gold_cycle(spark, gold_cycle.PROJECT_ROOT / "data/lakehouse", "public")
        lines.write.format.assert_not_called()
        stations.write.format.assert_not_called()
        patches["publish_gold_dataframes"].assert_not_called()


def test_non_utc_session_is_rejected():
    spark = Mock()
    spark.conf.get.return_value = "Europe/Paris"
    with pytest.raises(ValueError, match="UTC"):
        gold_cycle.run_gold_cycle(spark, gold_cycle.PROJECT_ROOT / "data/lakehouse", "public")


def test_incorrect_target_is_rejected_before_spark_access():
    spark = Mock()
    with pytest.raises(ValueError):
        gold_cycle.run_gold_cycle(spark, gold_cycle.PROJECT_ROOT / "data/lakehouse_real", "sncf_active_test")
    spark.conf.get.assert_not_called()