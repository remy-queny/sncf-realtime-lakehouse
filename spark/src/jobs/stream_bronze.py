import os
from pathlib import Path

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit, to_date


PROJECT_ROOT = Path(__file__).resolve().parents[3]

KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "localhost:9092",
)
KAFKA_TOPIC = os.getenv(
    "KAFKA_TRIP_UPDATES_TOPIC",
    "sncf.trip_updates.raw",
)

BRONZE_PATH = PROJECT_ROOT / "data" / "lakehouse" / "bronze" / "trip_updates"
CHECKPOINT_PATH = PROJECT_ROOT / "data" / "checkpoints" / "bronze_trip_updates"

KAFKA_CONNECTOR_PACKAGE = (
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3"
)


def create_spark_session() -> SparkSession:
    builder = (
        SparkSession.builder
        .appName("sncf-bronze-trip-updates")
        .master("local[*]")
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )

    return configure_spark_with_delta_pip(
        builder,
        extra_packages=[KAFKA_CONNECTOR_PACKAGE],
    ).getOrCreate()


def main() -> None:
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    try:
        kafka_dataframe = (
            spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
            .option("subscribe", KAFKA_TOPIC)
            .option("startingOffsets", "earliest")
            .option("failOnDataLoss", "false")
            .load()
        )

        bronze_dataframe = kafka_dataframe.select(
            col("key").cast("string").alias("kafka_key"),
            col("value").cast("string").alias("payload_json"),
            col("topic").alias("kafka_topic"),
            col("partition").alias("kafka_partition"),
            col("offset").alias("kafka_offset"),
            col("timestamp").alias("kafka_timestamp"),
            current_timestamp().alias("ingested_at"),
            to_date(current_timestamp()).alias("ingestion_date"),
            lit("not_parsed").alias("parse_status"),
            lit(None).cast("string").alias("parse_error"),
        )

        query = (
            bronze_dataframe.writeStream
            .format("delta")
            .outputMode("append")
            .option("checkpointLocation", str(CHECKPOINT_PATH))
            .trigger(availableNow=True)
            .start(str(BRONZE_PATH))
        )

        query.awaitTermination()

        print(f"Bronze Delta path: {BRONZE_PATH}")
        print(f"Checkpoint path: {CHECKPOINT_PATH}")
        print("Bronze Kafka-to-Delta streaming job completed successfully.")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()