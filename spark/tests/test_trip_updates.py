import json
import pytest

from pyspark.sql import SparkSession

from transforms.trip_updates import (
    enrich_trip_updates,
    parse_and_validate_trip_updates,
)
from jobs.inspect_silver import (
    check_delay_consistency,
    check_duplicate_event_ids,
    check_required_fields,
)
from datetime import datetime, timezone


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("test-trip-updates")
        .master("local[2]")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def test_valid_trip_update_is_parsed() -> None:
    spark = create_spark_session()

    try:
        payload = """
        {
          "event_id": "event-001",
          "event_type": "trip_update",
          "source": "simulation_java",
          "schema_version": 1,
          "trip_id": "TRIP-001",
          "route_id": "RER-A",
          "stop_id": "STOP-001",
          "scheduled_timestamp": "2026-09-22T16:00:00Z",
          "estimated_timestamp": "2026-09-22T16:02:30Z",
          "delay_seconds": 150,
          "event_timestamp": "2026-09-22T16:00:00Z"
        }
        """

        bronze_dataframe = spark.createDataFrame(
            [(payload,)],
            ["payload_json"],
        )

        result = parse_and_validate_trip_updates(
            bronze_dataframe
        ).first()

        assert result["event_id"] == "event-001"
        assert result["trip_id"] == "TRIP-001"
        assert result["route_id"] == "RER-A"
        assert result["delay_seconds"] == 150
        assert result["delay_minutes"] == 2.5
        assert result["validation_error"] is None
    finally:
        spark.stop()


def test_missing_trip_id_is_rejected() -> None:
    spark = create_spark_session()

    try:
        payload = """
        {
          "event_id": "event-002",
          "event_type": "trip_update",
          "delay_seconds": 120,
          "event_timestamp": "2026-09-22T16:00:00Z"
        }
        """

        bronze_dataframe = spark.createDataFrame(
            [(payload,)],
            ["payload_json"],
        )

        result = parse_and_validate_trip_updates(
            bronze_dataframe
        ).first()

        assert result["validation_error"] == "missing_trip_id"
    finally:
        spark.stop()


def test_invalid_json_is_rejected() -> None:
    spark = create_spark_session()

    try:
        bronze_dataframe = spark.createDataFrame(
            [("{invalid-json}",)],
            ["payload_json"],
        )

        result = parse_and_validate_trip_updates(
            bronze_dataframe
        ).first()

        assert result["validation_error"] == "invalid_json_or_schema"
    finally:
        spark.stop()

@pytest.mark.parametrize(
    ("field", "value", "expected_error"),
    [
        ("event_id", None, "missing_event_id"),
        ("event_id", "", "missing_event_id"),
        ("event_id", "   ", "missing_event_id"),
        ("trip_id", None, "missing_trip_id"),
        ("trip_id", "", "missing_trip_id"),
        ("trip_id", "   ", "missing_trip_id"),
        (
            "event_timestamp",
            None,
            "invalid_or_missing_event_timestamp",
        ),
        (
            "event_timestamp",
            "not-a-timestamp",
            "invalid_or_missing_event_timestamp",
        ),
        (
            "delay_seconds",
            None,
            "invalid_or_missing_delay_seconds",
        ),
    ],
    ids=[
        "null-event-id",
        "empty-event-id",
        "blank-event-id",
        "null-trip-id",
        "empty-trip-id",
        "blank-trip-id",
        "null-event-timestamp",
        "invalid-event-timestamp",
        "null-delay-seconds",
    ],
)
def test_invalid_trip_update_fields_are_rejected(
    field: str,
    value: object,
    expected_error: str,
) -> None:
    spark = create_spark_session()

    try:
        payload = {
            "event_id": "event-validation-001",
            "event_type": "trip_update",
            "source": "simulation_java",
            "schema_version": 1,
            "trip_id": "TRAIN-J-002",
            "route_id": "J",
            "stop_id": "stop_87271003",
            "scheduled_timestamp": "2026-10-01T16:00:00Z",
            "estimated_timestamp": "2026-10-01T16:02:00Z",
            "delay_seconds": 120,
            "event_timestamp": "2026-10-01T15:55:00Z",
        }
        payload[field] = value

        bronze_dataframe = spark.createDataFrame(
            [(json.dumps(payload),)],
            ["payload_json"],
        )

        result = parse_and_validate_trip_updates(
            bronze_dataframe
        ).first()

        assert result is not None
        assert result["validation_error"] == expected_error
    finally:
        spark.stop()


@pytest.mark.parametrize(
    ("delay_seconds", "expected_minutes", "estimated_timestamp"),
    [
        (0, 0.0, "2026-10-01T16:00:00Z"),
        (30, 0.5, "2026-10-01T16:00:30Z"),
        (720, 12.0, "2026-10-01T16:12:00Z"),
    ],
    ids=[
        "zero-delay",
        "fractional-minute",
        "twelve-minute-delay",
    ],
)
def test_valid_delays_are_converted_to_minutes(
    delay_seconds: int,
    expected_minutes: float,
    estimated_timestamp: str,
) -> None:
    spark = create_spark_session()

    try:
        payload = {
            "event_id": "event-delay-001",
            "event_type": "trip_update",
            "source": "simulation_java",
            "schema_version": 1,
            "trip_id": "TRAIN-TEST-001",
            "route_id": "J",
            "stop_id": "stop_87271003",
            "scheduled_timestamp": "2026-10-01T16:00:00Z",
            "estimated_timestamp": estimated_timestamp,
            "delay_seconds": delay_seconds,
            "event_timestamp": "2026-10-01T15:55:00Z",
        }

        bronze_dataframe = spark.createDataFrame(
            [(json.dumps(payload),)],
            ["payload_json"],
        )

        result = parse_and_validate_trip_updates(
            bronze_dataframe
        ).first()

        assert result is not None
        assert result["validation_error"] is None
        assert result["delay_seconds"] == delay_seconds
        assert result["delay_minutes"] == pytest.approx(
            expected_minutes
        )
    finally:
        spark.stop()

@pytest.mark.parametrize(
    ("route_id", "stop_id", "expected_line", "expected_stop"),
    [
        (
            "J",
            "stop_87271003",
            "Ligne J (simulation)",
            "Arrêt simulé J",
        ),
        (
            "UNKNOWN",
            "stop_87271003",
            None,
            "Arrêt simulé J",
        ),
        (
            "J",
            "UNKNOWN",
            "Ligne J (simulation)",
            None,
        ),
        (
            "UNKNOWN",
            "UNKNOWN",
            None,
            None,
        ),
    ],
    ids=[
        "all-references-found",
        "missing-route",
        "missing-stop",
        "all-references-missing",
    ],
)
def test_trip_update_reference_enrichment(
    route_id: str,
    stop_id: str,
    expected_line: str | None,
    expected_stop: str | None,
) -> None:
    spark = create_spark_session()

    try:
        valid_dataframe = spark.createDataFrame(
            [("event-enrichment-001", route_id, stop_id)],
            ["event_id", "route_id", "stop_id"],
        )

        routes = spark.createDataFrame(
            [("J", "Ligne J (simulation)")],
            ["route_id", "line_name"],
        )

        stops = spark.createDataFrame(
            [("stop_87271003", "Arrêt simulé J")],
            ["stop_id", "stop_name"],
        )

        rows = enrich_trip_updates(
            valid_dataframe,
            routes,
            stops,
        ).collect()

        assert len(rows) == 1

        result = rows[0]
        assert result["event_id"] == "event-enrichment-001"
        assert result["route_id"] == route_id
        assert result["stop_id"] == stop_id
        assert result["line_name"] == expected_line
        assert result["stop_name"] == expected_stop
    finally:
        spark.stop()

def test_duplicate_check_accepts_unique_event_ids() -> None:
    spark = create_spark_session()

    try:
        valid = spark.createDataFrame(
            [
                ("event-001", "TRAIN-J-002"),
                ("event-002", "TRAIN-J-002"),
            ],
            ["event_id", "trip_id"],
        )

        check_duplicate_event_ids(valid)
    finally:
        spark.stop()


def test_duplicate_check_rejects_repeated_event_ids() -> None:
    spark = create_spark_session()

    try:
        valid = spark.createDataFrame(
            [
                ("event-001", "TRAIN-J-002"),
                ("event-001", "TRAIN-J-002"),
            ],
            ["event_id", "trip_id"],
        )

        with pytest.raises(
            RuntimeError,
            match="des event_id sont présents plusieurs fois",
        ):
            check_duplicate_event_ids(valid)
    finally:
        spark.stop()

REQUIRED_FIELDS_SCHEMA = (
    "event_id string, "
    "trip_id string, "
    "event_timestamp timestamp, "
    "delay_seconds int"
)


def test_required_fields_accept_zero_delay() -> None:
    spark = create_spark_session()

    try:
        valid = spark.createDataFrame(
            [
                (
                    "event-001",
                    "TRAIN-H-005",
                    datetime(2026, 10, 5, 12, 0, tzinfo=timezone.utc),
                    0,
                )
            ],
            schema=REQUIRED_FIELDS_SCHEMA,
        )

        check_required_fields(valid)
    finally:
        spark.stop()


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("event_id", None),
        ("event_id", ""),
        ("event_id", "   "),
        ("trip_id", None),
        ("trip_id", ""),
        ("trip_id", "   "),
        ("event_timestamp", None),
        ("delay_seconds", None),
    ],
    ids=[
        "null-event-id",
        "empty-event-id",
        "blank-event-id",
        "null-trip-id",
        "empty-trip-id",
        "blank-trip-id",
        "null-event-timestamp",
        "null-delay-seconds",
    ],
)
def test_required_fields_reject_invalid_rows(
    field: str,
    value: object,
) -> None:
    spark = create_spark_session()

    try:
        row = {
            "event_id": "event-001",
            "trip_id": "TRAIN-J-002",
            "event_timestamp": datetime(
                2026, 10, 5, 12, 0, tzinfo=timezone.utc
            ),
            "delay_seconds": 120,
        }
        row[field] = value

        valid = spark.createDataFrame(
            [
                (
                    row["event_id"],
                    row["trip_id"],
                    row["event_timestamp"],
                    row["delay_seconds"],
                )
            ],
            schema=REQUIRED_FIELDS_SCHEMA,
        )

        with pytest.raises(
            RuntimeError,
            match="des champs obligatoires sont absents ou vides",
        ):
            check_required_fields(valid)
    finally:
        spark.stop()

DELAY_CONSISTENCY_SCHEMA = (
    "event_id string, "
    "delay_seconds int, "
    "delay_minutes double"
)


@pytest.mark.parametrize(
    ("delay_seconds", "delay_minutes"),
    [
        (0, 0.0),
        (30, 0.5),
        (720, 12.0),
    ],
    ids=[
        "zero-delay",
        "fractional-minute",
        "twelve-minute-delay",
    ],
)
def test_delay_consistency_accepts_correct_conversion(
    delay_seconds: int,
    delay_minutes: float,
) -> None:
    spark = create_spark_session()

    try:
        valid = spark.createDataFrame(
            [("event-001", delay_seconds, delay_minutes)],
            schema=DELAY_CONSISTENCY_SCHEMA,
        )

        check_delay_consistency(valid)
    finally:
        spark.stop()


@pytest.mark.parametrize(
    ("delay_seconds", "delay_minutes"),
    [
        (120, 3.0),
        (None, 2.0),
        (120, None),
        (120, float("nan")),
        (120, float("inf")),
    ],
    ids=[
        "incorrect-conversion",
        "null-seconds",
        "null-minutes",
        "nan-minutes",
        "infinite-minutes",
    ],
)
def test_delay_consistency_rejects_invalid_conversion(
    delay_seconds: int | None,
    delay_minutes: float | None,
) -> None:
    spark = create_spark_session()

    try:
        valid = spark.createDataFrame(
            [("event-001", delay_seconds, delay_minutes)],
            schema=DELAY_CONSISTENCY_SCHEMA,
        )

        with pytest.raises(
            RuntimeError,
            match="conversion secondes/minutes incohérente",
        ):
            check_delay_consistency(valid)
    finally:
        spark.stop()