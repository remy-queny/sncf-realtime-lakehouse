from pathlib import Path

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    count,
    countDistinct,
    max as spark_max,
    round as spark_round,
    when,
    window,
)
from transforms.gold import aggregate_delay_by_line


PROJECT_ROOT = Path(__file__).resolve().parents[3]
SILVER_PATH = (
    PROJECT_ROOT / "data" / "lakehouse" / "silver" / "trip_delays"
)
GOLD_PATH = (
    PROJECT_ROOT
    / "data"
    / "lakehouse"
    / "gold"
    / "delay_by_line_15min"
)


def main() -> None:
    builder = (
        SparkSession.builder
        .appName("sncf-gold-delay-by-line")
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
        silver = spark.read.format("delta").load(str(SILVER_PATH))

        gold = aggregate_delay_by_line(silver)

        print("Gold preview:")
        gold.orderBy("window_start", "route_id").show(
            truncate=False
        )

        gold.write.format("delta").mode("overwrite").save(
            str(GOLD_PATH)
        )

        print(f"Gold Delta path: {GOLD_PATH}")
        print("Gold aggregation completed successfully.")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()