from pathlib import Path

from delta import configure_spark_with_delta_pip
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    abs as spark_abs,
    col,
    count,
    isnan,
    length,
    trim,
)


PROJECT_ROOT = Path(__file__).resolve().parents[3]
SILVER_ROOT = PROJECT_ROOT / "data" / "lakehouse" / "silver"

def check_duplicate_event_ids(valid: DataFrame) -> None:
    duplicates = (
        valid.groupBy("event_id")
        .agg(count("*").alias("row_count"))
        .filter(col("row_count") > 1)
    )

    print("Duplicate event IDs in Silver:")
    duplicates.show(truncate=False)

    if duplicates.take(1):
        raise RuntimeError(
            "Échec du contrôle qualité Silver : "
            "des event_id sont présents plusieurs fois."
        )

    print("Contrôle des doublons : OK")

def check_required_fields(valid: DataFrame) -> None:
    invalid_rows = valid.filter(
        col("event_id").isNull()
        | (length(trim(col("event_id"))) == 0)
        | col("trip_id").isNull()
        | (length(trim(col("trip_id"))) == 0)
        | col("event_timestamp").isNull()
        | col("delay_seconds").isNull()
    )

    if invalid_rows.take(1):
        invalid_rows.select(
            "event_id",
            "trip_id",
            "event_timestamp",
            "delay_seconds",
        ).show(10, truncate=False)

        raise RuntimeError(
            "Échec du contrôle qualité Silver : "
            "des champs obligatoires sont absents ou vides."
        )

    print("Contrôle des champs obligatoires : OK")

def check_delay_consistency(valid: DataFrame) -> None:
    expected_minutes = col("delay_seconds") / 60.0

    invalid_rows = valid.filter(
        col("delay_seconds").isNull()
        | col("delay_minutes").isNull()
        | isnan(col("delay_minutes"))
        | (
            spark_abs(col("delay_minutes") - expected_minutes)
            > 1e-9
        )
    )

    if invalid_rows.take(1):
        invalid_rows.select(
            "event_id",
            "delay_seconds",
            "delay_minutes",
        ).show(10, truncate=False)

        raise RuntimeError(
            "Échec du contrôle qualité Silver : "
            "conversion secondes/minutes incohérente."
        )

    print("Contrôle de cohérence des retards : OK")

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

        check_duplicate_event_ids(valid)

        check_required_fields(valid)

        check_delay_consistency(valid)

        print("Route and stop IDs to enrich:")
        valid.select("route_id", "stop_id").distinct().show(
            truncate=False
        )
    finally:
        spark.stop()

if __name__ == "__main__":
    main()