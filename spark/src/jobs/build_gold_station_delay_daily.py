import os
from pathlib import Path

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    count,
    max as spark_max,
    round as spark_round,
    sum as spark_sum,
    when,
)
from transforms.gold import aggregate_station_delay_daily


PROJECT_ROOT = Path(__file__).resolve().parents[3]

LAKEHOUSE_ROOT = Path(
    os.getenv(
        "LAKEHOUSE_ROOT",
        str(PROJECT_ROOT / "data" / "lakehouse"),
    )
)

SILVER_PATH = LAKEHOUSE_ROOT / "silver" / "trip_delays"

GOLD_PATH = LAKEHOUSE_ROOT / "gold" / "station_delay_daily"


def main() -> None:
    builder = (
        SparkSession.builder
        .appName("sncf-gold-station-delay-daily")
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

        gold = aggregate_station_delay_daily(silver)

        print("Gold station preview:")
        gold.orderBy("event_date", "stop_id").show(
            truncate=False
        )

        gold.write.format("delta").mode("overwrite").save(
            str(GOLD_PATH)
        )

        print(f"Gold Delta path: {GOLD_PATH}")
        print("Gold station aggregation completed successfully.")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()