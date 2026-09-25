from pathlib import Path

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, to_date

from transforms.trip_updates import parse_and_validate_trip_updates


PROJECT_ROOT = Path(__file__).resolve().parents[3]

BRONZE_PATH = PROJECT_ROOT / "data" / "lakehouse" / "bronze" / "trip_updates"

SILVER_TRIP_DELAYS_PATH = (
    PROJECT_ROOT / "data" / "lakehouse" / "silver" / "trip_delays"
)

SILVER_REJECTED_EVENTS_PATH = (
    PROJECT_ROOT / "data" / "lakehouse" / "silver" / "rejected_events"
)

SILVER_TRIP_DELAYS_CHECKPOINT = (
    PROJECT_ROOT / "data" / "checkpoints" / "silver_trip_delays"
)

SILVER_REJECTED_EVENTS_CHECKPOINT = (
    PROJECT_ROOT / "data" / "checkpoints" / "silver_rejected_events"
)

REFERENCE_ROOT = PROJECT_ROOT / "spark" / "resources" / "simulation"


def create_spark_session() -> SparkSession:
    builder = (
        SparkSession.builder
        .appName("sncf-silver-trip-delays")
        .master("local[*]")
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.session.timeZone", "UTC")
    )

    return configure_spark_with_delta_pip(builder).getOrCreate()


def main() -> None:
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    try:
        routes = (
            spark.read
            .option("header", "true")
            .csv(str(REFERENCE_ROOT / "routes.csv"))
            .select("route_id", "line_name")
        )

        stops = (
            spark.read
            .option("header", "true")
            .csv(str(REFERENCE_ROOT / "stops.csv"))
            .select("stop_id", "stop_name")
        )

        bronze_stream = (
            spark.readStream
            .format("delta")
            .load(str(BRONZE_PATH))
        )

        parsed_stream = parse_and_validate_trip_updates(bronze_stream)

        rejected_stream = (
            parsed_stream
            .filter(col("validation_error").isNotNull())
            .withColumn("rejected_at", current_timestamp())
            .select(
                "kafka_key",
                "payload_json",
                "kafka_topic",
                "kafka_partition",
                "kafka_offset",
                "kafka_timestamp",
                "ingested_at",
                "ingestion_date",
                "event_id",
                "trip_id",
                "route_id",
                "stop_id",
                "delay_seconds",
                "event_timestamp",
                "validation_error",
                "rejected_at",
            )
        )

        valid_stream = (
            parsed_stream
            .filter(col("validation_error").isNull())
            .withWatermark("event_timestamp", "1 day")
            .dropDuplicatesWithinWatermark(["event_id"])
            .join(routes, on="route_id", how="left")
            .join(stops, on="stop_id", how="left")
            .withColumn("event_date", to_date(col("event_timestamp")))
            .select(
                "event_id",
                "event_type",
                "source",
                "schema_version",
                "trip_id",
                "route_id",
                "line_name",
                "stop_id",
                "stop_name",
                "scheduled_timestamp",
                "estimated_timestamp",
                "delay_seconds",
                "delay_minutes",
                "event_timestamp",
                "ingested_at",
                "event_date",
                "kafka_topic",
                "kafka_partition",
                "kafka_offset",
                "kafka_timestamp",
            )
        )

        rejected_query = (
            rejected_stream.writeStream
            .format("delta")
            .outputMode("append")
            .option(
                "checkpointLocation",
                str(SILVER_REJECTED_EVENTS_CHECKPOINT),
            )
            .trigger(availableNow=True)
            .start(str(SILVER_REJECTED_EVENTS_PATH))
        )

        valid_query = (
            valid_stream.writeStream
            .format("delta")
            .outputMode("append")
            .option(
                "checkpointLocation",
                str(SILVER_TRIP_DELAYS_CHECKPOINT),
            )
            .partitionBy("event_date")
            .trigger(availableNow=True)
            .start(str(SILVER_TRIP_DELAYS_PATH))
        )

        rejected_query.awaitTermination()
        valid_query.awaitTermination()

        print(f"Silver valid path: {SILVER_TRIP_DELAYS_PATH}")
        print(f"Silver rejected path: {SILVER_REJECTED_EVENTS_PATH}")
        print("Silver Bronze-to-Delta streaming job completed successfully.")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()