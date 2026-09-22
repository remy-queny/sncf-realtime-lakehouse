from pyspark.sql import SparkSession

from transforms.trip_updates import parse_and_validate_trip_updates


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