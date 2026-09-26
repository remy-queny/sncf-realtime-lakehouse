from pathlib import Path

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count


PROJECT_ROOT = Path(__file__).resolve().parents[3]
SILVER_ROOT = PROJECT_ROOT / "data" / "lakehouse" / "silver"


def main() -> None:
    builder = (
        SparkSession.builder
        .appName("sncf-inspect-silver")
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
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    try:
        valid = spark.read.format("delta").load(
            str(SILVER_ROOT / "trip_delays")
        )
        rejected = spark.read.format("delta").load(
            str(SILVER_ROOT / "rejected_events")
        )

        print(f"Silver valid row count: {valid.count()}")
        print(f"Silver rejected row count: {rejected.count()}")

        print("Valid events:")
        valid.select(
            "event_id",
            "trip_id",
            "route_id",
            "line_name",
            "stop_id",
            "stop_name",
            "delay_seconds",
            "delay_minutes",
            "event_timestamp",
        ).show(10, truncate=False)

        print("Silver columns:", valid.columns)

        print("Rejection reasons:")
        rejected.groupBy("validation_error").agg(
            count("*").alias("row_count")
        ).show(truncate=False)

        print("Duplicate event IDs in Silver:")
        valid.groupBy("event_id").agg(
            count("*").alias("row_count")
        ).filter(col("row_count") > 1).show(truncate=False)

        print("Route and stop IDs to enrich:")
        valid.select("route_id", "stop_id").distinct().show(
            truncate=False
        )
    finally:
        spark.stop()

if __name__ == "__main__":
    main()