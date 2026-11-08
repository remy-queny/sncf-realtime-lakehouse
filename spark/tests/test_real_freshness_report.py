import json
from unittest.mock import Mock

import pytest

import measure_real_freshness as analyzer


@pytest.mark.parametrize("complete", [True, False])
def test_report_uses_first_complete_publication(
    tmp_path, monkeypatch, complete
):
    monkeypatch.setattr(analyzer, "PROJECT_ROOT", tmp_path)

    session = tmp_path / "data/source_checks/session-test"
    session.mkdir(parents=True)
    (session / "exit-code.json").write_text(
        json.dumps({"exit_code": 0}),
        encoding="utf-8",
    )

    batch = {
        "batch_id": "batch-test",
        "expected_event_ids": ["event-1", "event-2"],
        "source_published_at": "2026-11-08T17:00:00Z",
        "collected_at": "2026-11-08T17:00:10Z",
    }
    cycles = [
        {
            "silver_version": version,
            "postgres_published_at": timestamp,
        }
        for version, timestamp in [
            (1, "2026-11-08T17:00:20+00:00"),
            (2, "2026-11-08T17:00:30+00:00"),
            (3, "2026-11-08T17:00:40+00:00"),
        ]
    ]

    monkeypatch.setattr(analyzer, "load_batches", lambda _: [batch])
    monkeypatch.setattr(analyzer, "load_cycles", lambda _: cycles)

    spark = Mock()
    monkeypatch.setattr(
        analyzer, "create_spark_session", lambda: spark
    )
    monkeypatch.setattr(analyzer.F, "col", Mock())

    inspected_versions = []

    def read_snapshot(spark, path, version):
        inspected_versions.append(version)
        ids = ["event-1"]

        if complete and version >= 2:
            ids.append("event-2")

        frame = Mock()
        frame.filter.return_value.select.return_value \
            .limit.return_value.collect.return_value = [
                {"event_id": event_id} for event_id in ids
            ]
        return frame

    monkeypatch.setattr(
        analyzer, "read_silver_snapshot", read_snapshot
    )
    monkeypatch.setattr(
        analyzer.sys, "argv",
        ["measure_real_freshness.py", str(session)],
    )

    code = analyzer.main()
    report = json.loads(
        (session / "freshness_report.json").read_text(
            encoding="utf-8"
        )
    )

    spark.stop.assert_called_once()

    if complete:
        assert code == 0
        assert inspected_versions == [1, 2]
        assert report["all_batches_measured"] is True
        assert report["unmeasured_batch_ids"] == []

        result = report["batches"][0]
        assert result["silver_version"] == 2
        assert result["source_to_postgres_seconds"] == 30.0
        assert result["collection_to_postgres_seconds"] == 20.0
    else:
        assert code == 1
        assert inspected_versions == [1, 2, 3]
        assert report["all_batches_measured"] is False
        assert report["unmeasured_batch_ids"] == ["batch-test"]
        assert report["batches"] == []


def test_load_cycles_orders_publications_and_ignores_noise(tmp_path):
    path = tmp_path / "gold.log"
    path.write_text(
        "\n".join([
            "Un message Spark",
            json.dumps({
                "silver_version": 2,
                "postgres_published_at": "2026-11-08T17:00:30Z",
            }),
            json.dumps({
                "silver_version": 1,
                "postgres_published_at": "2026-11-08T17:00:20Z",
            }),
        ]),
        encoding="utf-8",
    )

    cycles = analyzer.load_cycles(path)

    assert [item["silver_version"] for item in cycles] == [1, 2]


def test_load_cycles_rejects_missing_publication_timestamp(tmp_path):
    path = tmp_path / "gold.log"
    path.write_text(
        json.dumps({"silver_version": 1}),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="postgres_published_at"):
        analyzer.load_cycles(path)


@pytest.mark.parametrize("content", ["", "Un message Spark\n"])
def test_load_cycles_rejects_log_without_cycles(tmp_path, content):
    path = tmp_path / "gold.log"
    path.write_text(content, encoding="utf-8")

    with pytest.raises(ValueError, match="Aucun cycle"):
        analyzer.load_cycles(path)